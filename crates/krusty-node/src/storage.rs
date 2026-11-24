//! The storage stage and restart recovery (ARCHITECTURE §9; gaps P5, S1–S3, R4).
//!
//! The core stage hands every committed entry (with the time the core used
//! for it) and every publication to the storage stage over ring S. The
//! storage stage appends them to `krusty-log`, fsyncs once per batch (group
//! commit), writes snapshots the core sends, and enforces retention.
//!
//! Durability modes (PRD FR-8, single node; for a cluster see `cluster`):
//! - **Async (a)**: egress does not wait; storage runs behind, off the
//!   latency path. A crash can lose the last unsynced batch. In a cluster
//!   (`written`), the storage stage writes each batch out (to the page
//!   cache, which outlives a process crash) and reports how many entries
//!   it has written; Raft counts and acknowledges only those, so killing a
//!   node loses nothing it acknowledged. Fsyncs run on a thread of their
//!   own (`krusty-fsync`), so writes never wait behind one (2026-11-23,
//!   spec/RAFT-VERIFICATION.md: before, entries still in ring S counted).
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
        log.index_published(seqno, pos)?;
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
/// At most this many ring S records between two fsyncs. A group takes
/// whatever has queued up, so the slower the fsync, the larger the next
/// group: throughput does not depend on how many fsyncs a second the
/// device manages (it was 128 entries per fsync, about 160 k msg/s on a
/// consumer NVMe; BASELINE E3).
const MAX_GROUP: usize = 1 << 16;

fn fatal(what: &str, e: io::Error) -> ! {
    // Decisions that cannot be made durable must not go out (G3): stop hard;
    // a restart recovers from the log.
    eprintln!("krusty-node: storage failure ({what}): {e}; aborting");
    std::process::abort();
}

/// Mode (a) in a cluster: the most often the storage stage starts an fsync.
/// Waking the fsync thread costs the storage stage a few µs, and in mode
/// (a) an fsync only bounds what a power loss can take (the cautious
/// restart recovers the rest).
const SYNC_EVERY_NS: u64 = 1_000_000;

enum Job {
    Sync(krusty_log::SyncJob),
    Snapshot(krusty_log::SnapshotJob),
}

/// Mode (a) in a cluster: fsyncs and snapshots on a thread of their own
/// (`krusty-fsync`), one job at a time, so the storage stage's writes (which
/// acknowledgements wait for) never wait behind a device flush.
struct Background {
    tx: std::sync::mpsc::SyncSender<Job>,
    thread: std::thread::JoinHandle<()>,
    busy: Arc<AtomicBool>,
    /// Snapshots stored by the thread; retention runs when it moves.
    snaps_done: Arc<AtomicU64>,
    snaps_seen: u64,
    synced_at: u64,
    /// Written since the last fsync started.
    written: bool,
    /// A snapshot waiting for the thread.
    snapshot: Option<(u64, Vec<u8>)>,
}

impl Background {
    fn start(stats: Arc<Metrics>, returned: SyncSender<Vec<u8>>) -> Background {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Job>(1);
        let busy = Arc::new(AtomicBool::new(false));
        let snaps_done = Arc::new(AtomicU64::new(0));
        let (b, d) = (busy.clone(), snaps_done.clone());
        let thread = std::thread::Builder::new()
            .name("krusty-fsync".into())
            .spawn(move || {
                let debug = std::env::var_os("KRUSTY_STORE_DEBUG").is_some();
                for job in rx {
                    let t0 = crate::metrics::mono_ns();
                    let kind = match &job {
                        Job::Sync(_) => "sync",
                        Job::Snapshot(_) => "snapshot",
                    };
                    match job {
                        Job::Sync(j) => j.run().unwrap_or_else(|e| fatal("sync", e)),
                        Job::Snapshot(j) => {
                            let bytes = j.run().unwrap_or_else(|e| fatal("snapshot", e));
                            let _ = returned.try_send(bytes);
                            stats.snapshots.fetch_add(1, Ordering::Relaxed);
                            d.fetch_add(1, Ordering::Release);
                        }
                    }
                    let t1 = crate::metrics::mono_ns();
                    stats.store_sync.record(t1 - t0);
                    if debug && t1 - t0 > 2_000_000 {
                        eprintln!("store-debug job {kind} {t0} {t1} {}us", (t1 - t0) / 1000);
                    }
                    b.store(false, Ordering::Release);
                }
            })
            .expect("fsync thread");
        Background {
            tx,
            thread,
            busy,
            snaps_done,
            snaps_seen: 0,
            synced_at: 0,
            written: false,
            snapshot: None,
        }
    }

    /// Hands the thread a pending snapshot, or an fsync of what was written
    /// (at most every SYNC_EVERY_NS; the idle loop polls too, so the last
    /// batch gets one), if it is free; runs retention after a snapshot it
    /// stored.
    fn poll(&mut self, log: &mut Log) {
        let done = self.snaps_done.load(Ordering::Acquire);
        if done != self.snaps_seen {
            self.snaps_seen = done;
            log.enforce_retention()
                .unwrap_or_else(|e| fatal("retention", e));
        }
        if self.busy.load(Ordering::Acquire) {
            return;
        }
        let now = crate::metrics::mono_ns();
        let job = if let Some((i, bytes)) = self.snapshot.take() {
            Job::Snapshot(
                log.snapshot_job(i, bytes)
                    .unwrap_or_else(|e| fatal("snapshot", e)),
            )
        } else if self.written && now >= self.synced_at + SYNC_EVERY_NS {
            Job::Sync(log.take_sync().unwrap_or_else(|e| fatal("sync", e)))
        } else {
            return;
        };
        self.written = false;
        self.synced_at = now;
        self.busy.store(true, Ordering::Release);
        self.tx.send(job).expect("fsync thread");
    }

    /// Stores a waiting snapshot and ends the thread.
    fn finish(mut self, log: &mut Log) {
        if let Some((i, bytes)) = self.snapshot.take() {
            let job = log
                .snapshot_job(i, bytes)
                .unwrap_or_else(|e| fatal("snapshot", e));
            self.tx.send(Job::Snapshot(job)).expect("fsync thread");
        }
        drop(self.tx);
        let _ = self.thread.join();
    }
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
    // Entries (S_ENTRY records) received; `synced` (when given: a cluster)
    // says how many of them count as persisted: written out in mode (a),
    // fsynced in mode (b).
    let mut entries = 0u64;
    let background = cfg
        .storage
        .as_ref()
        .is_some_and(|s| s.durability == Durability::Async)
        && synced.is_some();
    // Mode (a) in a cluster: fsyncs and snapshots on their own thread.
    let mut bg = background.then(|| Background::start(stats.clone(), returned.clone()));
    // Pinned only now: the fsync thread keeps the process's CPUs (the
    // housekeeping ones), not this stage's.
    crate::stages::pin(cfg.storage.as_ref().and_then(|s| s.core));
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
        let mut n = 0;
        let mut group_start = 0u64;
        // Group commit: drain what is queued (bounded) before one fsync.
        // Stop at a snapshot (one per group: its bytes come in order on the
        // channel) and before the staged egress records (mode b) could
        // outgrow their buffer: one chunk adds at most chunk * max record.
        let chunk = cfg.batch * 4;
        let chunk_bytes = chunk * (cfg.mtu + 64);
        loop {
            if group_start == 0 && !input.is_empty() {
                group_start = crate::metrics::mono_ns();
            }
            let k = input.read(chunk, |kind, rec| match kind {
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
                    log.index_published(seqno, pos)
                        .unwrap_or_else(|e| fatal("index", e));
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
            n += k;
            if k < chunk
                || n >= MAX_GROUP
                || snapshot_at.is_some()
                || pending.len() + chunk_bytes > PENDING_BYTES
            {
                break;
            }
        }
        if n == 0 {
            if finished && input.is_empty() {
                if let Some(bg) = bg.take() {
                    bg.finish(&mut log);
                }
                log.sync().unwrap_or_else(|e| fatal("final sync", e));
                store_done.store(true, Ordering::Release);
                return;
            }
            // Idle: the last batch written gets its fsync too.
            if let Some(bg) = &mut bg {
                bg.poll(&mut log);
            }
            crate::stages::idle(cfg.idle, &mut empty);
            continue;
        }
        empty = 0;
        if background && appended > 0 {
            // Written out, so it survives a process crash: Raft may count it.
            let t0 = crate::metrics::mono_ns();
            log.flush().unwrap_or_else(|e| fatal("write", e));
            let t1 = crate::metrics::mono_ns();
            stats.store_write.record(t1 - t0);
            if t1 - t0 > 1_000_000 && std::env::var_os("KRUSTY_STORE_DEBUG").is_some() {
                eprintln!("store-debug write {t0} {t1} {}us", (t1 - t0) / 1000);
            }
            stats
                .store_group
                .record(t1 - if group_start == 0 { t0 } else { group_start });
            stats.stored.fetch_add(appended, Ordering::Relaxed);
            if let Some(s) = &synced {
                s.store(entries, Ordering::Release);
            }
            if let Some(bg) = &mut bg {
                bg.written = true;
                bg.poll(&mut log);
            }
        } else if appended > 0 || !pending.is_empty() {
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
        if let Some(bg) = &mut bg {
            if let Some(i) = snapshot_at
                && let Ok(bytes) = snapshots.recv()
            {
                bg.snapshot = Some((i, bytes));
                bg.poll(&mut log);
            }
        } else if let Some(i) = snapshot_at
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
