//! The storage stage and restart recovery (ARCHITECTURE §9; gaps P5, S1–S3, R4).
//!
//! The core stage hands every committed entry (with the time the core used
//! for it) and every publication to the storage stage over ring S. The
//! storage stage appends them to `krusty-log`, fsyncs once per batch (group
//! commit), writes snapshots the core sends, and enforces retention.
//!
//! Durability modes (PRD FR-8, single node; for a cluster see `cluster`):
//! - **Async (a)**: egress does not wait; storage runs behind, off the
//!   latency path. A crash can lose the last unsynced batch.
//! - **Sync (b)**: the core routes egress records (frames, feedback)
//!   through ring S too, and the storage stage forwards a batch to egress
//!   only after its fsync. Nothing is published or acknowledged before it is
//!   durable.
//!
//! Restart: load the latest snapshot, replay the log suffix through the core
//! (no output), which also rewrites the egress index and refills the
//! in-memory history; then carry on from the last log index.

use crate::metrics::Metrics;
use crate::stages::{HIST_HEADER, put_history, write_ring};
use crate::{NodeConfig, StorageConfig};
use krusty_core::Core;
use krusty_log::{Log, Pos};
use krusty_ring::byte_ring::{Consumer, Producer};
use krusty_ring::history::History;
use krusty_wire::ingress::Submit;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};

/// Ring S record kinds.
/// `log_index u64 | term u64 | timestamp u64 | payload`
pub(crate) const S_ENTRY: u32 = 1;
/// `seqno u64 | log_index u64`: stream message `seqno` came from that entry.
pub(crate) const S_PUBLISHED: u32 = 2;
/// `log_index u64`: a snapshot taken after that entry follows on the channel.
pub(crate) const S_SNAPSHOT: u32 = 3;
/// `log_index u64`: remove entries from there on (Raft, uncommitted only).
pub(crate) const S_TRUNCATE: u32 = 4;
pub(crate) const S_ENTRY_HEADER: usize = 24;

/// Fills an S_ENTRY record.
pub(crate) fn put_entry(b: &mut [u8], log_index: u64, term: u64, ts: u64, payload: &[u8]) {
    b[..8].copy_from_slice(&log_index.to_le_bytes());
    b[8..16].copy_from_slice(&term.to_le_bytes());
    b[16..24].copy_from_slice(&ts.to_le_bytes());
    b[S_ENTRY_HEADER..].copy_from_slice(payload);
}
/// Egress records in mode (b): ring B kind + this.
pub(crate) const S_EGRESS: u32 = 100;

/// How durable a published message is (PRD FR-8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// (a) Publish at once; fsync in the background.
    Async,
    /// (b) Publish only after the fsync of its batch (group commit).
    Sync,
}

pub(crate) struct Restored {
    pub core: Core,
    pub log_index: u64,
    pub last_ts: u64,
    pub log: Log,
    pub history: History,
    pub snapshot_index: u64,
    pub replayed: u64,
}

/// Opens the log and rebuilds the core: snapshot, then the log suffix.
pub(crate) fn restore(cfg: &NodeConfig, st: &StorageConfig) -> io::Result<Restored> {
    let opened = Log::open(st.log.clone())?;
    let mut log = opened.log;
    let (mut core, snapshot_index) = match opened.snapshot {
        Some((i, bytes)) => {
            let core = Core::restore(cfg.core, &bytes).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("snapshot {i}: {e:?}"))
            })?;
            if core.applied() != i {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot index mismatch",
                ));
            }
            (core, i)
        }
        None => (Core::new(cfg.core), 0),
    };
    let history = History::starting_at(
        cfg.history,
        HIST_HEADER + cfg.max_payload,
        core.next_seqno(),
    );
    log.reset_index_to(core.next_seqno());
    let mut scratch = vec![0u8; HIST_HEADER + cfg.max_payload];
    let (mut last_ts, mut replayed) = (0u64, 0u64);
    let mut failure: Option<String> = None;
    let mut published: Vec<(u64, Pos)> = Vec::new();
    log.replay(snapshot_index + 1, |pos, ts, payload| {
        if failure.is_some() {
            return;
        }
        let s = match Submit::decode(payload) {
            Ok(s) => s,
            Err(e) => {
                failure = Some(format!("entry {}: {e:?}", pos.log_index));
                return;
            }
        };
        match core.apply(pos.log_index, ts, &s) {
            Ok(e) => {
                if let Some(p) = e.publish {
                    published.push((p.seqno, pos));
                    put_history(&history, p.seqno, &p.meta, s.payload, &mut scratch);
                }
            }
            Err(e) => failure = Some(format!("entry {}: {e:?}", pos.log_index)),
        }
        last_ts = ts;
        replayed += 1;
    })?;
    if let Some(f) = failure {
        return Err(io::Error::new(io::ErrorKind::InvalidData, f));
    }
    for (seqno, pos) in published {
        log.index_published(seqno, pos);
    }
    log.flush()?;
    let log_index = core.applied();
    Ok(Restored {
        core,
        log_index,
        last_ts,
        log,
        history,
        snapshot_index,
        replayed,
    })
}

/// Bytes egress records in mode (b) are staged in before forwarding.
const PENDING_BYTES: usize = 4 << 20;

fn fatal(what: &str, e: io::Error) -> ! {
    // Decisions that cannot be made durable must not go out (G3): stop hard;
    // a restart recovers from the log.
    eprintln!("krusty-node: storage failure ({what}): {e}; aborting");
    std::process::abort();
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn store_stage(
    cfg: NodeConfig,
    mut log: Log,
    mut recent: std::collections::VecDeque<Pos>,
    mut input: Consumer,
    mut egress: Producer,
    snapshots: Receiver<Vec<u8>>,
    returned: SyncSender<Vec<u8>>,
    core_done: Arc<AtomicBool>,
    store_done: Arc<AtomicBool>,
    stats: Arc<Metrics>,
    synced: Option<Arc<AtomicU64>>,
) {
    crate::stages::pin(cfg.storage.as_ref().and_then(|s| s.core));
    // Entries (S_ENTRY records) received; after each fsync, `synced` (when
    // given: replication in mode b) says how many of them are durable.
    let mut entries = 0u64;
    // Staged egress records: kind u32 | len u32 | bytes, back to back.
    let mut pending: Vec<u8> = Vec::with_capacity(PENDING_BYTES);
    let mut empty = 0;
    // Positions of entries appended but not yet published (in order).
    recent.reserve(1 << 16);
    let u64_at = |b: &[u8], i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    loop {
        let finished = core_done.load(Ordering::Acquire);
        let mut appended = 0u64;
        let mut snapshot_at: Option<u64> = None;
        pending.clear();
        let n = input.read(cfg.batch * 4, |kind, rec| match kind {
            S_ENTRY => {
                let (li, term, ts) = (u64_at(rec, 0), u64_at(rec, 8), u64_at(rec, 16));
                if li != log.next_index() {
                    fatal(
                        "append",
                        io::Error::other(format!("entry {li}, expected {}", log.next_index())),
                    );
                }
                let pos = log
                    .append_entry(term, ts, &rec[S_ENTRY_HEADER..])
                    .unwrap_or_else(|e| fatal("append", e));
                recent.push_back(pos);
                appended += 1;
                entries += 1;
            }
            S_PUBLISHED => {
                let (seqno, li) = (u64_at(rec, 0), u64_at(rec, 8));
                while recent.front().is_some_and(|p| p.log_index < li) {
                    recent.pop_front();
                }
                let pos = match recent.front() {
                    Some(p) if p.log_index == li => *p,
                    // Appended before a restart.
                    _ => log.pos_of(li).unwrap_or_else(|e| fatal("index", e)),
                };
                log.index_published(seqno, pos);
            }
            S_TRUNCATE => {
                let from = u64_at(rec, 0);
                while recent.back().is_some_and(|p| p.log_index >= from) {
                    recent.pop_back();
                }
                log.truncate_from(from)
                    .unwrap_or_else(|e| fatal("truncate", e));
            }
            S_SNAPSHOT => snapshot_at = Some(u64_at(rec, 0)),
            k => {
                pending.extend_from_slice(&(k - S_EGRESS).to_le_bytes());
                pending.extend_from_slice(&(rec.len() as u32).to_le_bytes());
                pending.extend_from_slice(rec);
            }
        });
        if n == 0 {
            if finished && input.is_empty() {
                log.sync().unwrap_or_else(|e| fatal("final sync", e));
                store_done.store(true, Ordering::Release);
                return;
            }
            crate::stages::idle(cfg.idle, &mut empty);
            continue;
        }
        empty = 0;
        if appended > 0 || !pending.is_empty() {
            let t0 = crate::metrics::mono_ns();
            log.sync_entries().unwrap_or_else(|e| fatal("sync", e));
            stats.store_sync.record(crate::metrics::mono_ns() - t0);
            stats.stored.fetch_add(appended, Ordering::Relaxed);
            if let Some(s) = &synced {
                s.store(entries, Ordering::Release);
            }
        }
        // Mode (b): what was waiting for this fsync may go out now.
        let mut at = 0;
        while at < pending.len() {
            let kind = u32::from_le_bytes(pending[at..at + 4].try_into().unwrap());
            let len = u32::from_le_bytes(pending[at + 4..at + 8].try_into().unwrap()) as usize;
            let body = &pending[at + 8..at + 8 + len];
            write_ring(&mut egress, kind, len, |b| b.copy_from_slice(body));
            at += 8 + len;
        }
        egress.publish();
        if let Some(i) = snapshot_at
            && let Ok(bytes) = snapshots.recv()
        {
            log.write_snapshot(i, &bytes)
                .unwrap_or_else(|e| fatal("snapshot", e));
            let _ = returned.try_send(bytes);
            stats.snapshots.fetch_add(1, Ordering::Relaxed);
            log.enforce_retention()
                .unwrap_or_else(|e| fatal("retention", e));
        }
    }
}
