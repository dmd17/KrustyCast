//! Replication (M3; ARCHITECTURE §6, §8.2): the core stage runs Raft
//! (`krusty-raft`) next to the sequencer core, and a replication stage
//! moves Raft messages between nodes.
//!
//! ```text
//!  peers ─UDP─▶ [repl] ─ring R─▶ [core: Raft + sequencer] ─ring O─▶ [repl] ─UDP─▶ peers
//!  clients ─▶ [ingress] ─ring A─▶   │  ├─ring S─▶ [store]  (entries, truncations, publications)
//!                                  │  └─ring B─▶ [egress] (feed and replies: leader only)
//! ```
//!
//! - Every node stores each Raft entry when it is appended (the leader when
//!   it proposes, followers when they receive it) and applies entries to
//!   its own sequencer core once committed, so every node has the same
//!   history and can serve recovery and take over.
//! - A Raft entry is `timestamp u64 | raw SUBMIT` (the leader's clock, made
//!   monotonic across leaders); an empty entry is the leader's no-op. The
//!   log record stores the timestamp and the SUBMIT separately.
//! - Only the leader proposes, publishes DATA frames and heartbeats, and
//!   replies to clients (followers answer NOT_LEADER with the leader's id).
//!   A node that proposed an entry remembers the client's address and
//!   replies when the entry is applied, if it is still the same entry.
//! - Durability mode (a) only: an entry handed to the storage stage counts
//!   as persisted for Raft, so followers acknowledge on receipt and a
//!   committed entry is in the memory of a majority. The hard state (term,
//!   vote) is written before any message that depends on it is sent.
//! - Restart: the latest snapshot (taken at an applied, hence committed,
//!   index) restores the core; entries after it are loaded into the Raft
//!   log, uncommitted as far as this node knows, and applied once a leader
//!   confirms the commit index.
//!
//! Not yet: InstallSnapshot (a follower that lags past the log's retention
//! cannot catch up; `repl_unservable` counts it), mode (b) with
//! replication, membership changes.

use crate::metrics::{Metrics, mono_ns};
use crate::stages::{
    HIST_HEADER, Out, REC_HDR, REC_TO, emit_frames, feed_record, get_addr, get_ts, idle, now_ns,
    pin, put_addr, put_history, put_ts, write_ring,
};
use crate::storage::{S_ENTRY, S_ENTRY_HEADER, S_SNAPSHOT, S_TRUNCATE, put_entry};
use crate::{ClusterConfig, NodeConfig, StorageConfig};
use krusty_core::Core;
use krusty_log::{HardStateFile, Log, LogReader};
use krusty_net::{RecvBatch, SendBatch};
use krusty_raft::{Config as RaftConfig, HardState, Message, Outbox, Raft, RaftLog, Role, wire};
use krusty_ring::byte_ring::{Consumer, Producer};
use krusty_ring::history::History;
use krusty_wire::feedback::{Feedback, Outcome};
use krusty_wire::ingress::Submit;
use krusty_wire::stream;
use std::collections::VecDeque;
use std::io;
use std::net::{SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Largest Raft datagram.
const MAX_DATAGRAM: usize = 65_507;
/// How often the core stage advances Raft's clock.
const TICK_NS: u64 = 100_000;

fn fatal(what: &str, e: impl std::fmt::Display) -> ! {
    eprintln!("krusty-node: replication failure ({what}): {e}; aborting");
    std::process::abort();
}

pub(crate) fn raft_config(cl: &ClusterConfig) -> RaftConfig {
    RaftConfig {
        election_min_ns: cl.election_min_us * 1000,
        election_max_ns: cl.election_max_us * 1000,
        heartbeat_ns: cl.heartbeat_us * 1000,
        max_append_entries: 1024,
        max_append_bytes: cl.max_append_bytes - wire::HEADER_LEN,
        max_inflight: cl.max_inflight,
        retain_entries: cl.retain_entries,
        seed: now_ns() ^ (cl.id as u64).rotate_left(32),
        ..RaftConfig::new(cl.id, cl.members.iter().map(|m| m.0).collect())
    }
}

/// What the cluster core stage starts from.
pub(crate) struct ClusterStart {
    pub core: Core,
    /// Raft index applied to `core` (the snapshot's).
    pub applied: u64,
    pub raft: Raft,
    pub hs_file: HardStateFile,
    pub last_ts: u64,
    pub disk: LogReader,
    pub snapshots: Option<crate::stages::SnapshotLink>,
}

pub(crate) struct Restored {
    pub start: ClusterStart,
    pub log: Log,
    /// Where the entries after the snapshot are (the storage stage indexes
    /// them when they are applied again).
    pub positions: VecDeque<krusty_log::Pos>,
    pub history: History,
    pub snapshot_index: u64,
    pub loaded: u64,
}

/// Opens the log: core from the latest snapshot, Raft log from the entries
/// after it, hard state from `raft.state`.
pub(crate) fn restore(
    cfg: &NodeConfig,
    st: &StorageConfig,
    cl: &ClusterConfig,
) -> io::Result<Restored> {
    let bad = |m: String| io::Error::new(io::ErrorKind::InvalidData, m);
    let opened = Log::open(st.log.clone())?;
    let mut log = opened.log;
    // Cluster snapshots are `term of the entry u64 | core snapshot`.
    let (core, snap, snap_term) = match opened.snapshot {
        Some((i, bytes)) if bytes.len() >= 8 => {
            let term = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            let core = Core::restore(cfg.core, &bytes[8..])
                .map_err(|e| bad(format!("snapshot {i}: {e:?}")))?;
            (core, i, term)
        }
        Some((i, _)) => return Err(bad(format!("snapshot {i}: too short"))),
        None => (Core::new(cfg.core), 0, 0),
    };
    let history = History::starting_at(
        cfg.history,
        HIST_HEADER + cfg.max_payload,
        core.next_seqno(),
    );
    log.reset_index_to(core.next_seqno());
    let mut rl = RaftLog::new(snap, snap_term);
    let mut last_ts = 0;
    let mut data = Vec::new();
    let mut positions = VecDeque::new();
    log.replay_entries(snap + 1, |pos, h, payload| {
        debug_assert_eq!(pos.log_index, rl.last_index() + 1);
        positions.push_back(pos);
        last_ts = last_ts.max(h.timestamp_ns);
        data.clear();
        if !payload.is_empty() {
            data.extend_from_slice(&h.timestamp_ns.to_le_bytes());
            data.extend_from_slice(payload);
        }
        rl.append(h.term, &data);
    })?;
    let loaded = rl.last_index() - snap;
    if rl.last_index() != opened.last_index {
        return Err(bad("log does not continue from the snapshot".into()));
    }
    let (hs_file, (term, vote)) = HardStateFile::open(&st.log.dir)?;
    let hs = HardState {
        term,
        voted_for: (vote != 0).then_some(vote),
    };
    let raft = Raft::new(raft_config(cl), hs, rl, snap, mono_ns());
    log.flush()?;
    let disk = log.reader();
    Ok(Restored {
        start: ClusterStart {
            core,
            applied: snap,
            raft,
            hs_file,
            last_ts,
            disk,
            snapshots: None,
        },
        log,
        positions,
        history,
        snapshot_index: snap,
        loaded,
    })
}

/// The replication I/O stage: datagrams from peers into ring R, ring O
/// (record kind = destination node id) out to peers.
pub(crate) fn repl_stage(
    cfg: NodeConfig,
    cl: ClusterConfig,
    sock: UdpSocket,
    mut to_core: Producer,
    mut from_core: Consumer,
    stop: Arc<AtomicBool>,
    stats: Arc<Metrics>,
) {
    pin(cl.core);
    let mut rb = RecvBatch::new(32, MAX_DATAGRAM);
    let mut sb = SendBatch::new(32, MAX_DATAGRAM);
    let mut empty = 0;
    while !stop.load(Ordering::Acquire) {
        let n = rb.recv(&sock).unwrap_or(0);
        for i in 0..n {
            let (b, _) = rb.get(i);
            stats.repl_in.fetch_add(1, Ordering::Relaxed);
            // A full ring drops the message: Raft tolerates loss.
            if !to_core.try_write(1, b.len(), |d| d.copy_from_slice(b)) {
                stats.repl_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        if n > 0 {
            to_core.publish();
        }
        let m = from_core.read(32, |to, rec| {
            let Some(a) = cl.addr_of(to) else { return };
            if sb.is_full() {
                let _ = sb.flush(&sock);
            }
            sb.push(a, |b| {
                b[..rec.len()].copy_from_slice(rec);
                rec.len()
            });
            stats.repl_out.fetch_add(1, Ordering::Relaxed);
        });
        if !sb.is_empty() {
            let _ = sb.flush(&sock);
        }
        if n + m == 0 {
            idle(cfg.idle, &mut empty);
        } else {
            empty = 0;
        }
    }
}

/// The core stage's state in a cluster.
struct Shell {
    cfg: NodeConfig,
    id: u32,
    max_bytes: usize,
    raft: Raft,
    ob: Outbox,
    core: Core,
    /// The core's own index (SUBMITs only; no-ops are skipped).
    core_index: u64,
    /// Raft index applied.
    applied: u64,
    out: Out,
    peers: Producer,
    hs_file: HardStateFile,
    disk: LogReader,
    history: Arc<History>,
    stats: Arc<Metrics>,
    /// Entries this node proposed: (index, term, client).
    pending: VecDeque<(u64, u64, SocketAddrV4)>,
    last_ts: u64,
    proposals: u64,
    msg: Vec<u8>,
    offsets: Vec<u32>,
    data: Vec<u8>,
    raw: Vec<u8>,
    scratch: Vec<u8>,
    frame: Vec<u8>,
    entry: Vec<u8>,
    fb: [u8; krusty_wire::feedback::LEN],
}

fn send_feedback(out: &mut Out, fb: &mut [u8], to: SocketAddrV4, f: &Feedback) {
    let t = mono_ns();
    out.egress(REC_TO, REC_HDR + krusty_wire::feedback::LEN, |b| {
        put_addr(b, to);
        put_ts(b, t);
        f.encode(fb).unwrap();
        b[REC_HDR..].copy_from_slice(fb);
    });
}

impl Shell {
    /// Carries out the outbox: hard state, log changes, then messages.
    fn carry_out(&mut self) {
        if let Some(hs) = self.ob.hard_state.take() {
            self.hs_file
                .store(hs.term, hs.voted_for.unwrap_or(0), false)
                .unwrap_or_else(|e| fatal("hard state", e));
        }
        if let Some(t) = self.ob.truncate_from.take() {
            self.out
                .store(S_TRUNCATE, 8, |b| b.copy_from_slice(&t.to_le_bytes()));
            while self.pending.back().is_some_and(|p| p.0 >= t) {
                self.pending.pop_back();
            }
        }
        if let Some(from) = self.ob.store_from.take() {
            let last = self.raft.last_index();
            for i in from..=last {
                let (term, d) = self.raft.entry(i).expect("new entries are in memory");
                let (ts, payload) = if d.is_empty() {
                    (0, &[][..])
                } else {
                    (u64::from_le_bytes(d[..8].try_into().unwrap()), &d[8..])
                };
                self.last_ts = self.last_ts.max(ts);
                self.out
                    .store(S_ENTRY, S_ENTRY_HEADER + payload.len(), |b| {
                        put_entry(b, i, term, ts, payload)
                    });
            }
            // Mode (a): handed to the storage stage counts as persisted.
            self.raft.persisted(last, &mut self.ob);
        }
        let mut sends = std::mem::take(&mut self.ob.sends);
        for (to, m) in sends.drain(..) {
            self.send(to, m);
        }
        self.ob.sends = sends;
    }

    /// An entry's (term, data) into `self.data`, from memory or disk.
    fn load(&mut self, i: u64) -> Option<u64> {
        self.data.clear();
        if let Some((t, d)) = self.raft.entry(i) {
            self.data.extend_from_slice(d);
            return Some(t);
        }
        let h = self.disk.read_entry(i, &mut self.raw).ok()??;
        if !self.raw.is_empty() {
            self.data.extend_from_slice(&h.timestamp_ns.to_le_bytes());
            self.data.extend_from_slice(&self.raw);
        }
        Some(h.term)
    }

    fn send(&mut self, to: u32, m: Message) {
        let n = match m {
            Message::Append {
                term,
                prev_index,
                mut prev_term,
                count,
                commit,
            } => {
                if prev_index > 0 && prev_term == 0 {
                    match self.raft.term_at(prev_index) {
                        Some(t) => prev_term = t,
                        None => match self.disk.read_entry(prev_index, &mut self.raw) {
                            Ok(Some(h)) => prev_term = h.term,
                            _ => {
                                self.stats.repl_unservable.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        },
                    }
                }
                let mut at = wire::HEADER_LEN;
                let mut k = 0;
                while k < count {
                    let Some(t) = self.load(prev_index + 1 + k) else {
                        break;
                    };
                    if k > 0 && at + wire::entry_len(self.data.len()) > self.max_bytes {
                        break; // the follower's reply says how far it got
                    }
                    at = wire::push_entry(&mut self.msg, at, t, &self.data);
                    k += 1;
                }
                if count > 0 && k == 0 {
                    self.stats.repl_unservable.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                let m = Message::Append {
                    term,
                    prev_index,
                    prev_term,
                    count: k,
                    commit,
                };
                wire::encode(&mut self.msg, self.id, &m);
                at
            }
            m => wire::encode(&mut self.msg, self.id, &m),
        };
        let msg = &self.msg[..n];
        write_ring(&mut self.peers, to, n, |b| b.copy_from_slice(msg));
    }

    fn on_peer(&mut self, rec: &[u8]) {
        let offsets = std::mem::take(&mut self.offsets);
        let Some((from, m, entries)) = wire::decode(rec, offsets) else {
            self.stats.repl_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.raft.on_message(from, m, &entries, &mut self.ob);
        self.offsets = entries.into_offsets();
        self.carry_out();
    }

    fn on_submit(&mut self, rec: &[u8], batch_start: u64) {
        let src = get_addr(rec);
        self.stats
            .ingress_to_core
            .record(batch_start.saturating_sub(get_ts(rec)));
        let raw = &rec[REC_HDR..];
        let Ok(s) = Submit::decode(raw) else { return };
        if self.raft.role() == Role::Leader {
            let ts = now_ns().max(self.last_ts);
            self.last_ts = ts;
            self.data.clear();
            self.data.extend_from_slice(&ts.to_le_bytes());
            self.data.extend_from_slice(raw);
            let i = self
                .raft
                .propose(&self.data, &mut self.ob)
                .expect("the leader proposes");
            self.pending.push_back((i, self.raft.term(), src));
            self.proposals += 1;
        } else {
            self.stats.not_leader.fetch_add(1, Ordering::Relaxed);
            let f = Feedback {
                client_id: s.client_id,
                incarnation: s.incarnation,
                client_seqno: s.client_seqno,
                outcome: Outcome::NotLeader {
                    leader: self.raft.leader().unwrap_or(0) as u64,
                },
            };
            send_feedback(&mut self.out, &mut self.fb, src, &f);
        }
    }

    /// Applies committed entries; the leader publishes them. Returns how
    /// many messages were published.
    fn apply_committed(&mut self) -> u64 {
        let commit = self.raft.commit_index();
        if commit <= self.applied {
            return 0;
        }
        let leader = self.raft.role() == Role::Leader;
        let first = self.core.next_seqno();
        let mut replies = 0;
        while self.applied < commit {
            let i = self.applied + 1;
            let Some(term) = self.load(i) else {
                fatal("apply", format!("committed entry {i} is missing"));
            };
            while self.pending.front().is_some_and(|p| p.0 < i) {
                self.pending.pop_front();
            }
            let mine = match self.pending.front() {
                Some(&(pi, pt, addr)) if pi == i => {
                    self.pending.pop_front();
                    (pt == term).then_some(addr)
                }
                _ => None,
            };
            if !self.data.is_empty() {
                let ts = u64::from_le_bytes(self.data[..8].try_into().unwrap());
                let s = Submit::decode(&self.data[8..])
                    .unwrap_or_else(|e| fatal("apply", format!("entry {i}: {e:?}")));
                self.core_index += 1;
                let e = self
                    .core
                    .apply(self.core_index, ts, &s)
                    .unwrap_or_else(|e| fatal("apply", format!("{e:?}")));
                if let Some(p) = e.publish {
                    put_history(
                        &self.history,
                        p.seqno,
                        &p.meta,
                        s.payload,
                        &mut self.scratch,
                    );
                    self.out.store_published(p.seqno, i);
                }
                if let (Some(f), Some(addr)) = (e.reply, mine) {
                    send_feedback(&mut self.out, &mut self.fb, addr, &f);
                    replies += 1;
                }
            }
            self.applied = i;
        }
        self.raft.applied(self.applied);
        self.stats.feedback.fetch_add(replies, Ordering::Relaxed);
        let last = self.core.next_seqno();
        if leader && last > first {
            let (cfg, history, out) = (&self.cfg, &self.history, &mut self.out);
            let frames = emit_frames(
                cfg,
                history,
                first,
                last,
                false,
                &mut self.frame,
                &mut self.entry,
                |f| feed_record(out, f),
            );
            self.stats
                .published
                .fetch_add(last - first, Ordering::Relaxed);
            self.stats.frames.fetch_add(frames, Ordering::Relaxed);
        }
        last - first
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn cluster_core_stage(
    cfg: NodeConfig,
    mut input: Consumer,
    mut peers_in: Consumer,
    peers_out: Producer,
    out: Out,
    history: Arc<History>,
    start: ClusterStart,
    leader_flag: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    stats: Arc<Metrics>,
) {
    pin(cfg.cores[1]);
    let cl = cfg.cluster.clone().expect("cluster config");
    let ClusterStart {
        core,
        applied,
        raft,
        hs_file,
        last_ts,
        disk,
        snapshots,
    } = start;
    let mut sh = Shell {
        id: cl.id,
        max_bytes: cl.max_append_bytes,
        core_index: core.applied(),
        core,
        applied,
        raft,
        ob: Outbox::default(),
        out,
        peers: peers_out,
        hs_file,
        disk,
        history,
        stats: stats.clone(),
        pending: VecDeque::with_capacity(1 << 16),
        last_ts,
        proposals: 0,
        msg: vec![0u8; MAX_DATAGRAM],
        offsets: Vec::with_capacity(1024),
        data: Vec::with_capacity(HIST_HEADER + cfg.max_payload + 64),
        raw: Vec::with_capacity(cfg.max_payload + 64),
        scratch: vec![0u8; HIST_HEADER + cfg.max_payload],
        frame: vec![0u8; cfg.mtu],
        entry: vec![0u8; HIST_HEADER + cfg.max_payload],
        fb: [0u8; krusty_wire::feedback::LEN],
        cfg: cfg.clone(),
    };
    let mut last_snapshot = sh.applied;
    let mut snap_buf: Option<Vec<u8>> = Some(Vec::new());
    let heartbeat = Duration::from_micros(cfg.heartbeat_us);
    let mut last_sent = Instant::now();
    let mut next_tick = 0u64;
    let mut was_leader = false;
    let mut empty = 0;
    loop {
        let batch_start = mono_ns();
        let mut work = peers_in.read(cfg.batch, |_, rec| sh.on_peer(rec));
        work += input.read(cfg.batch, |_, rec| sh.on_submit(rec, batch_start));
        if sh.proposals > 0 {
            sh.proposals = 0;
            sh.raft.replicate(&mut sh.ob, false);
        }
        sh.carry_out();
        let now = mono_ns();
        if now >= next_tick {
            sh.raft.tick(now, &mut sh.ob);
            sh.carry_out();
            next_tick = now + TICK_NS;
        }
        let before = sh.applied;
        if sh.apply_committed() > 0 {
            last_sent = Instant::now();
        }
        stats
            .applied
            .fetch_add(sh.applied - before, Ordering::Relaxed);
        let leader = sh.raft.role() == Role::Leader;
        if leader != was_leader {
            was_leader = leader;
            leader_flag.store(leader, Ordering::Release);
            if leader {
                stats.leader_terms.fetch_add(1, Ordering::Relaxed);
                last_sent = Instant::now() - heartbeat; // announce at once
            }
        }
        stats.raft_role.store(
            match sh.raft.role() {
                Role::Follower => 0,
                Role::PreCandidate => 1,
                Role::Candidate => 2,
                Role::Leader => 3,
            },
            Ordering::Relaxed,
        );
        stats.raft_term.store(sh.raft.term(), Ordering::Relaxed);
        stats
            .stream_next
            .store(sh.core.next_seqno(), Ordering::Relaxed);
        stats
            .raft_commit
            .store(sh.raft.commit_index(), Ordering::Relaxed);
        if let Some((tx, back, every)) = &snapshots
            && sh.applied >= last_snapshot + every
        {
            if snap_buf.is_none() {
                snap_buf = back.try_recv().ok();
            }
            if let Some(mut buf) = snap_buf.take() {
                buf.clear();
                let term = sh
                    .raft
                    .term_at(sh.applied)
                    .expect("applied entries are in memory");
                buf.extend_from_slice(&term.to_le_bytes());
                sh.core.snapshot(&mut buf);
                let at = sh.applied;
                sh.out
                    .store(S_SNAPSHOT, 8, |b| b.copy_from_slice(&at.to_le_bytes()));
                let _ = tx.send(buf);
                last_snapshot = sh.applied;
            }
        }
        if leader && last_sent.elapsed() >= heartbeat {
            let k = stream::encode_heartbeat(&mut sh.frame, cfg.stream_id, sh.core.next_seqno())
                .unwrap();
            let f = &sh.frame[..k];
            feed_record(&mut sh.out, f);
            last_sent = Instant::now();
        }
        sh.out.publish();
        sh.peers.publish();
        if stop.load(Ordering::Acquire) {
            // A node leaving is a failure as far as the cluster is concerned:
            // no END_OF_SESSION (the stream goes on).
            done.store(true, Ordering::Release);
            return;
        }
        if work == 0 {
            idle(cfg.idle, &mut empty);
        } else {
            empty = 0;
        }
    }
}
