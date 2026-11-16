//! Deterministic cluster simulation (M3): Raft-replicated nodes, each
//! applying the committed log to its own `krusty-core`; clients that follow
//! the leader; consumers that recover from any node. Faults: loss,
//! duplication, delay, partitions, crashes and restarts (from each node's
//! store).
//!
//! Durability mode (b) (`durable_acks`): a node's store syncs after a
//! random delay; only synced entries count toward the leader's quorum, and
//! followers hold successful AppendReplies until what they acknowledge is
//! synced, as `krusty-node` does. Every crash is then a power loss (the
//! unsynced tail of the store is gone), sometimes of all nodes at once:
//! nothing committed may be lost.
//!
//! The log holds raw inputs (gap R2): an entry is `[timestamp u64][SUBMIT]`
//! proposed by the leader; an empty entry is the leader's no-op. Every node
//! derives the same decisions; only the leader answers clients and publishes
//! frames. A new leader does not know what the old one published: consumers
//! learn the next seqno from its heartbeats and recover gaps from any node.
//!
//! Checks: every consumer's stream agrees with every node's (G1), in order
//! with only explicit loss (G2), at most once (G4), conditional appends
//! (G5), session FIFO (G6), exactly-once outcomes for the clients across
//! failovers, and liveness after the network heals.

use crate::{
    Consumer, Report, Rng, STREAM_ID, consumer_receive, parse_payload, payload, take_deliveries,
};
use krusty_core::{Config as CoreConfig, Core};
use krusty_raft::wire;
use krusty_raft::{Config as RaftConfig, HardState, Message, NodeId, Outbox, Raft, RaftLog, Role};
use krusty_session::{Config as SessionConfig, Event, Session};
use krusty_stream::{Config as TrackerConfig, Tracker};
use krusty_wire::feedback::{Feedback, Outcome};
use krusty_wire::ingress::{Append, Submit};
use krusty_wire::stream::{self, DataFrameWriter, Frame, MessageMeta};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

#[derive(Clone, Debug)]
pub struct ClusterParams {
    pub nodes: u32,
    pub clients: u32,
    pub msgs_per_client: u64,
    pub consumers: usize,
    /// Per-mille of datagrams lost / duplicated while faulty.
    pub loss: u64,
    pub dup: u64,
    pub max_delay_ns: u64,
    /// Mean ms between repartitions and between crashes while faulty.
    pub repartition_ms: u64,
    pub crash_ms: u64,
    /// Faults stop at this time (ns); the run must then finish.
    pub heal_at_ns: u64,
    pub limit_ns: u64,
    pub conditional: u64,
    pub core: CoreConfig,
    /// Mode (b) with power losses (see the module docs).
    pub durable_acks: bool,
    /// Bug injection for mode (b): followers acknowledge unsynced entries.
    pub ack_unsynced: bool,
}

impl Default for ClusterParams {
    fn default() -> ClusterParams {
        ClusterParams {
            nodes: 3,
            clients: 3,
            msgs_per_client: 150,
            consumers: 2,
            loss: 50,
            dup: 30,
            max_delay_ns: 3_000_000,
            repartition_ms: 300,
            crash_ms: 400,
            heal_at_ns: 3_000_000_000,
            limit_ns: 60_000_000_000,
            conditional: 300,
            core: CoreConfig {
                outcome_ring: 8,
                ..CoreConfig::DEFAULT
            },
            durable_acks: false,
            ack_unsynced: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum To {
    Node(NodeId),
    Client(u32),
    Consumer(usize),
}

/// What survives a crash.
#[derive(Default, Clone)]
struct Store {
    hs: HardState,
    log: Vec<(u64, Vec<u8>)>,
}

struct Node {
    raft: Option<Raft>,
    store: Store,
    core: Core,
    /// Last Raft index applied, and the core's own index (submits only).
    applied: u64,
    core_index: u64,
    /// The node's egress history (seqno - 1 → message).
    history: Vec<(MessageMeta, Vec<u8>)>,
    /// Leader timestamp clock (monotonic).
    last_ts: u64,
    last_heartbeat: u64,
    /// Mode (b): entries of `store.log` that are synced.
    synced: u64,
    /// Mode (b): successful AppendReplies waiting for a sync.
    held: Vec<(NodeId, Message)>,
}

struct ClientState {
    session: Session,
    submitted: u64,
    target: NodeId,
    believed: HashMap<u64, u64>,
    appends: HashMap<u64, Append>,
    last_heard: u64,
}

const MS: u64 = 1_000_000;
/// In-flight datagrams beyond which a run is stopped as a livelock.
const MAX_QUEUE: usize = 20_000;

fn raft_config(id: NodeId, n: u32, seed: u64) -> RaftConfig {
    RaftConfig {
        seed,
        election_min_ns: 50 * MS,
        election_max_ns: 100 * MS,
        heartbeat_ns: 10 * MS,
        retain_entries: 64,
        ..RaftConfig::new(id, (1..=n).collect())
    }
}

fn boot(id: NodeId, n: u32, seed: u64, store: &Store, now: u64) -> Raft {
    let mut log = RaftLog::new(0, 0);
    for (t, d) in &store.log {
        log.append(*t, d);
    }
    Raft::new(raft_config(id, n, seed), store.hs, log, 0, now)
}

/// A datagram in flight: (arrival, tie-breaker, destination, bytes).
type InFlight = (u64, u64, To, Vec<u8>);

struct Sim<'p> {
    p: &'p ClusterParams,
    rng: Rng,
    now: u64,
    queue: BinaryHeap<Reverse<InFlight>>,
    counter: u64,
    faulty: bool,
    group: Vec<u32>,
    nodes: Vec<Node>,
    report: Report,
}

impl Sim<'_> {
    fn up(&self, a: To, b: To) -> bool {
        match (a, b) {
            (To::Node(x), To::Node(y)) => self.group[x as usize] == self.group[y as usize],
            _ => true, // clients and consumers reach every node
        }
    }

    fn send(&mut self, from: To, to: To, bytes: Vec<u8>) {
        self.report.datagrams += 1;
        if !self.up(from, to) {
            return;
        }
        let copies = if self.faulty {
            if self.rng.per_mille(self.p.loss) {
                0
            } else if self.rng.per_mille(self.p.dup) {
                2
            } else {
                1
            }
        } else {
            1
        };
        for _ in 0..copies {
            let delay = 100_000 + self.rng.below(self.p.max_delay_ns);
            self.counter += 1;
            self.queue
                .push(Reverse((self.now + delay, self.counter, to, bytes.clone())));
        }
    }

    /// Persists, then sends (the outbox contract), then applies commits.
    fn after_step(&mut self, k: usize, mut out: Outbox) {
        let id = k as NodeId + 1;
        let b = self.p.durable_acks;
        {
            let node = &mut self.nodes[k];
            let raft = node.raft.as_mut().unwrap();
            if let Some(hs) = out.hard_state {
                node.store.hs = hs; // synced at once in both modes
            }
            if let Some(t) = out.truncate_from {
                node.store.log.truncate(t as usize - 1);
                node.synced = node.synced.min(t - 1);
                node.held.retain(
                    |(_, m)| !matches!(m, Message::AppendReply { index, .. } if *index >= t),
                );
            }
            if let Some(from) = out.store_from {
                node.store.log.truncate(from as usize - 1);
                node.synced = node.synced.min(from - 1);
                for i in from..=raft.last_index() {
                    let (t, d) = raft.entry(i).expect("new entries are in memory");
                    node.store.log.push((t, d.to_vec()));
                }
            }
            if !b {
                // Mode (a): stored is persisted.
                let last = raft.last_index();
                let mut more = Outbox::default();
                raft.persisted(last, &mut more);
                out.sends.append(&mut more.sends);
            } else if !self.p.ack_unsynced {
                // Mode (b): acknowledge only what is synced.
                let synced = node.synced;
                let held = &mut node.held;
                out.sends.retain(|&(to, m)| match m {
                    Message::AppendReply {
                        success: true,
                        index,
                        ..
                    } if index > synced => {
                        held.push((to, m));
                        false
                    }
                    _ => true,
                });
            }
        }
        let sends = std::mem::take(&mut out.sends);
        for (to, m) in sends {
            let node = &self.nodes[k];
            let raft = node.raft.as_ref().unwrap();
            let mut m = m;
            if let Message::Append {
                prev_index,
                ref mut prev_term,
                ..
            } = m
                && prev_index > 0
                && *prev_term == 0
            {
                *prev_term = node.store.log[prev_index as usize - 1].0;
            }
            let mut buf = vec![0u8; 1 << 16];
            let mut at = wire::encode(&mut buf, id, &m);
            if let Message::Append {
                prev_index, count, ..
            } = m
            {
                for i in prev_index + 1..=prev_index + count {
                    let (t, d) = match raft.entry(i) {
                        Some((t, d)) => (t, d.to_vec()),
                        None => node.store.log[i as usize - 1].clone(),
                    };
                    at = wire::push_entry(&mut buf, at, t, &d);
                }
            }
            buf.truncate(at);
            buf.shrink_to_fit(); // queued copies would otherwise keep 64 KiB each
            self.send(To::Node(id), To::Node(to), buf);
        }
        self.apply_committed(k);
    }

    /// Mode (b): node `k`'s store finishes a sync of everything stored.
    fn sync(&mut self, k: usize) {
        let node = &mut self.nodes[k];
        let Some(raft) = node.raft.as_mut() else {
            return;
        };
        node.synced = node.store.log.len() as u64;
        let mut out = Outbox::default();
        raft.persisted(node.synced, &mut out);
        let (term, synced) = (raft.term(), node.synced);
        for (to, m) in std::mem::take(&mut node.held) {
            match m {
                Message::AppendReply { term: t, index, .. } if index <= synced => {
                    if t == term {
                        out.sends.push((to, m));
                    }
                }
                _ => node.held.push((to, m)),
            }
        }
        self.after_step(k, out);
    }

    fn apply_committed(&mut self, k: usize) {
        let id = k as NodeId + 1;
        loop {
            let node = &mut self.nodes[k];
            let raft = node.raft.as_mut().unwrap();
            if node.applied >= raft.commit_index() {
                return;
            }
            node.applied += 1;
            raft.applied(node.applied);
            let leader = raft.role() == Role::Leader;
            let data = node.store.log[node.applied as usize - 1].1.clone();
            if data.is_empty() {
                continue; // the leader's no-op
            }
            let ts = u64::from_le_bytes(data[..8].try_into().unwrap());
            let Ok(s) = Submit::decode(&data[8..]) else {
                self.report
                    .violations
                    .push(format!("node {id}: undecodable committed entry"));
                continue;
            };
            node.core_index += 1;
            let e = node.core.apply(node.core_index, ts, &s).expect("in order");
            let mut frame = None;
            if let Some(pb) = e.publish {
                if pb.seqno != node.history.len() as u64 + 1 {
                    self.report.violations.push(format!(
                        "node {id}: seqno {} after {}",
                        pb.seqno,
                        node.history.len()
                    ));
                }
                node.history.push((pb.meta, s.payload.to_vec()));
                if leader {
                    let mut buf = vec![0u8; 2048];
                    let mut w = DataFrameWriter::new(&mut buf, STREAM_ID, pb.seqno, false).unwrap();
                    w.push(&pb.meta, s.payload).unwrap();
                    let n = w.finish().unwrap();
                    buf.truncate(n);
                    frame = Some(buf);
                }
            }
            let reply = if leader { e.reply } else { None };
            if let Some(f) = frame {
                for c in 0..self.p.consumers {
                    self.send(To::Node(id), To::Consumer(c), f.clone());
                }
            }
            if let Some(f) = reply {
                let mut buf = vec![0u8; 64];
                let n = f.encode(&mut buf).unwrap();
                buf.truncate(n);
                self.send(To::Node(id), To::Client(f.client_id), buf);
            }
        }
    }

    /// A datagram for node `k`: Raft, a client SUBMIT, or a RECOVER.
    fn node_receive(&mut self, k: usize, bytes: &[u8]) {
        let id = k as NodeId + 1;
        if self.nodes[k].raft.is_none() {
            return; // crashed
        }
        if let Some((from, m, entries)) = wire::decode(bytes, Vec::new()) {
            let mut out = Outbox::default();
            self.nodes[k]
                .raft
                .as_mut()
                .unwrap()
                .on_message(from, m, &entries, &mut out);
            self.after_step(k, out);
            return;
        }
        if let Ok(s) = Submit::decode(bytes) {
            let raft = self.nodes[k].raft.as_mut().unwrap();
            if raft.role() != Role::Leader {
                let hint = raft.leader().unwrap_or(0) as u64;
                let f = Feedback {
                    client_id: s.client_id,
                    incarnation: s.incarnation,
                    client_seqno: s.client_seqno,
                    outcome: Outcome::NotLeader { leader: hint },
                };
                let mut buf = vec![0u8; 64];
                let n = f.encode(&mut buf).unwrap();
                buf.truncate(n);
                self.send(To::Node(id), To::Client(s.client_id), buf);
                return;
            }
            let node = &mut self.nodes[k];
            node.last_ts = node.last_ts.max(self.now);
            let mut entry = node.last_ts.to_le_bytes().to_vec();
            entry.extend_from_slice(bytes);
            let mut out = Outbox::default();
            let raft = node.raft.as_mut().unwrap();
            raft.propose(&entry, &mut out);
            raft.replicate(&mut out, false);
            self.after_step(k, out);
            return;
        }
        if let Ok(Frame::Recover { from, count, .. }) = Frame::decode(bytes) {
            // Any node serves recovery from its own history; answer all
            // consumers (stand-in for a retransmission on the feed).
            let node = &self.nodes[k];
            let len = node.history.len() as u64;
            let end = (from + count).min(len + 1);
            let mut frames = Vec::new();
            let mut s = from;
            while s < end {
                let mut buf = vec![0u8; 4096];
                let mut w = DataFrameWriter::new(&mut buf, STREAM_ID, s, true).unwrap();
                while s < end && w.count() < 16 {
                    let (meta, pl) = &node.history[s as usize - 1];
                    w.push(meta, pl).unwrap();
                    s += 1;
                }
                let n = w.finish().unwrap();
                buf.truncate(n);
                frames.push(buf);
            }
            for f in frames {
                for c in 0..self.p.consumers {
                    self.send(To::Node(id), To::Consumer(c), f.clone());
                }
            }
        }
    }
}

pub fn run_cluster(seed: u64, p: &ClusterParams) -> Report {
    let n = p.nodes;
    let mut sim = Sim {
        p,
        rng: Rng(seed),
        now: 0,
        queue: BinaryHeap::new(),
        counter: 0,
        faulty: true,
        group: vec![0; n as usize + 1],
        nodes: (1..=n)
            .map(|id| Node {
                raft: Some(boot(id, n, seed, &Store::default(), 0)),
                store: Store::default(),
                core: Core::new(p.core),
                applied: 0,
                core_index: 0,
                history: Vec::new(),
                last_ts: 0,
                last_heartbeat: 0,
                synced: 0,
                held: Vec::new(),
            })
            .collect(),
        report: Report::default(),
    };
    let mut clients: Vec<ClientState> = (0..p.clients)
        .map(|c| ClientState {
            session: Session::new(
                SessionConfig {
                    client_id: c,
                    window: p.core.outcome_ring,
                    retry_interval_ns: 30 * MS,
                    max_attempts: 100_000,
                },
                1_000 + c as u64,
            ),
            submitted: 0,
            target: 1 + c % n,
            believed: HashMap::new(),
            appends: HashMap::new(),
            last_heard: 0,
        })
        .collect();
    let mut consumers: Vec<Consumer> = (0..p.consumers)
        .map(|_| Consumer {
            tracker: Tracker::new(TrackerConfig {
                pinned: Some(STREAM_ID),
                start_seqno: Some(1),
                recover: true,
                retry_interval_ns: 20 * MS,
                gap_policy: krusty_stream::GapPolicy::Wait,
            }),
            stash: HashMap::new(),
            delivered: Vec::new(),
        })
        .collect();
    let mut events: HashMap<(u32, u64), Event> = HashMap::new();
    let mut leaders: HashMap<u64, NodeId> = HashMap::new();
    let total = p.clients as u64 * p.msgs_per_client;
    let tick = MS / 2;

    while sim.now < p.limit_ns {
        // A message storm (fan-out above one per round trip) would exhaust
        // memory long before the time limit; healthy runs stay in the
        // low thousands.
        if sim.queue.len() > MAX_QUEUE {
            sim.report.violations.push(format!(
                "livelock: over {MAX_QUEUE} datagrams in flight at {} ms",
                sim.now / MS
            ));
            break;
        }
        let done = events.len() as u64 == total;
        let longest = sim.nodes.iter().map(|x| x.history.len()).max().unwrap_or(0) as u64;
        if done
            && consumers
                .iter()
                .all(|c| c.tracker.next_seqno() == Some(longest + 1))
            && !sim.faulty
        {
            break;
        }
        sim.now += tick;
        sim.faulty = sim.now < p.heal_at_ns;
        // Faults.
        if sim.faulty && sim.rng.below(p.repartition_ms * 2) == 0 {
            for g in sim.group.iter_mut().skip(1) {
                *g = sim.rng.below(2) as u32;
            }
        }
        if !sim.faulty {
            sim.group.iter_mut().for_each(|g| *g = 0);
        }
        if sim.faulty && sim.rng.below(p.crash_ms * 2) == 0 {
            let k = sim.rng.below(n as u64) as usize;
            // Mode (b): power losses, sometimes of every node at once.
            let all = p.durable_acks && sim.rng.below(3) == 0;
            for (j, node) in sim.nodes.iter_mut().enumerate() {
                if j == k || all {
                    node.raft = None; // memory is gone; the store survives
                    if p.durable_acks {
                        node.store.log.truncate(node.synced as usize);
                        node.held.clear();
                    }
                }
            }
        }
        for k in 0..n as usize {
            if sim.nodes[k].raft.is_none() && (!sim.faulty || sim.rng.below(200) == 0) {
                let node = &mut sim.nodes[k];
                node.raft = Some(boot(k as u32 + 1, n, seed ^ sim.now, &node.store, sim.now));
                node.synced = node.store.log.len() as u64;
                // Rebuild the core by re-applying the committed log as the
                // commit index becomes known again.
                node.core = Core::new(p.core);
                node.applied = 0;
                node.core_index = 0;
                node.history.clear();
            }
        }
        // Deliver.
        while let Some(Reverse((t, _, _, _))) = sim.queue.peek() {
            if *t > sim.now {
                break;
            }
            let Reverse((_, _, to, bytes)) = sim.queue.pop().unwrap();
            match to {
                To::Node(id) => sim.node_receive(id as usize - 1, &bytes),
                To::Client(c) => {
                    if let Ok(f) = Feedback::decode(&bytes) {
                        let cl = &mut clients[c as usize];
                        cl.last_heard = sim.now;
                        if let Outcome::NotLeader { leader } = f.outcome {
                            cl.target = if leader != 0 {
                                leader as NodeId
                            } else {
                                cl.target % n + 1
                            };
                        }
                        let mut ev = Vec::new();
                        cl.session.on_feedback(&f, sim.now, &mut ev);
                        record(c, ev, cl, &mut events, &mut sim.report);
                    }
                }
                To::Consumer(k) => consumer_receive(&bytes, &mut consumers[k], k, &mut sim.report),
            }
        }
        // Mode (b): stores finish syncs (about 2 ms after a write).
        if p.durable_acks {
            for k in 0..n as usize {
                let node = &sim.nodes[k];
                if node.raft.is_some()
                    && node.synced < node.store.log.len() as u64
                    && sim.rng.below(4) == 0
                {
                    sim.sync(k);
                }
            }
        }
        // Raft ticks and leader heartbeats on the stream.
        for k in 0..n as usize {
            let Some(raft) = sim.nodes[k].raft.as_mut() else {
                continue;
            };
            let mut out = Outbox::default();
            raft.tick(sim.now, &mut out);
            let leader = raft.role() == Role::Leader;
            let term = raft.term();
            sim.after_step(k, out);
            if leader {
                if let Some(prev) = leaders.insert(term, k as NodeId + 1)
                    && prev != k as NodeId + 1
                {
                    sim.report
                        .violations
                        .push(format!("two leaders in term {term}"));
                }
                if sim.now - sim.nodes[k].last_heartbeat >= 5 * MS {
                    sim.nodes[k].last_heartbeat = sim.now;
                    let next = sim.nodes[k].history.len() as u64 + 1;
                    let mut buf = vec![0u8; 64];
                    let len = stream::encode_heartbeat(&mut buf, STREAM_ID, next).unwrap();
                    buf.truncate(len);
                    for c in 0..p.consumers {
                        sim.send(To::Node(k as NodeId + 1), To::Consumer(c), buf.clone());
                    }
                }
            }
        }
        // Clients: submit, (re)transmit; rotate if nothing heard for a while.
        for c in 0..p.clients {
            let cl = &mut clients[c as usize];
            while cl.submitted < p.msgs_per_client {
                let handle = cl.submitted;
                let append = if sim.rng.per_mille(p.conditional) {
                    let key = sim.rng.below(2);
                    Append::Conditional {
                        key,
                        expected_version: *cl.believed.get(&key).unwrap_or(&0),
                    }
                } else {
                    Append::Unconditional
                };
                match cl.session.submit(append, handle) {
                    Ok(_) => {
                        cl.appends.insert(handle, append);
                        cl.submitted += 1;
                    }
                    Err(_) => break,
                }
            }
            if sim.now - cl.last_heard > 100 * MS && cl.session.in_flight() > 0 {
                cl.target = cl.target % n + 1;
                cl.last_heard = sim.now;
            }
            let (mut tx, mut ev) = (Vec::new(), Vec::new());
            cl.session.poll(sim.now, &mut tx, &mut ev);
            record(c, ev, cl, &mut events, &mut sim.report);
            let target = cl.target;
            for t in tx {
                let pl = payload(c, t.handle);
                let s = Submit {
                    client_id: c,
                    incarnation: t.incarnation,
                    client_seqno: t.client_seqno,
                    append: t.append,
                    payload: &pl,
                };
                let mut buf = vec![0u8; 256];
                let len = s.encode(&mut buf).unwrap();
                buf.truncate(len);
                sim.send(To::Client(c), To::Node(target), buf);
            }
        }
        // Consumers recover from a random node.
        for (k, cons) in consumers.iter_mut().enumerate() {
            let mut out = Vec::new();
            if let Some(r) = cons.tracker.poll_recover(sim.now, &mut out) {
                let mut buf = vec![0u8; 64];
                let len = stream::encode_recover(&mut buf, r.stream_id, r.from, r.count).unwrap();
                buf.truncate(len);
                let node = 1 + sim.rng.below(n as u64) as NodeId;
                sim.send(To::Consumer(k), To::Node(node), buf);
            }
            take_deliveries(cons, out, k, &mut sim.report);
        }
    }

    let total_events = events.len() as u64;
    if total_events < total {
        sim.report.violations.push(format!(
            "liveness: {total_events} of {total} outcomes by the limit"
        ));
    }
    sim.report.published = sim.nodes.iter().map(|x| x.history.len()).max().unwrap_or(0) as u64;
    sim.report.delivered = consumers.iter().map(|c| c.delivered.len() as u64).collect();
    sim.report.lost = consumers.iter().map(|c| c.tracker.lost()).collect();
    let v = check(&sim, &events, &consumers);
    sim.report.violations.extend(v);
    sim.report.end_ns = sim.now;
    sim.report
}

fn record(
    c: u32,
    ev: Vec<Event>,
    cl: &mut ClientState,
    events: &mut HashMap<(u32, u64), Event>,
    r: &mut Report,
) {
    for e in ev {
        let handle = match e {
            Event::Accepted { handle, .. } => {
                r.accepted += 1;
                handle
            }
            Event::Rejected {
                handle,
                key,
                current_version,
                ..
            } => {
                r.rejected += 1;
                cl.believed.insert(key, current_version);
                handle
            }
            Event::Failed { handle, .. } => {
                r.failed += 1;
                handle
            }
        };
        if let (
            Some(Append::Conditional {
                key,
                expected_version,
            }),
            Event::Accepted { .. },
        ) = (cl.appends.get(&handle).copied(), e)
        {
            cl.believed.insert(key, expected_version + 1);
        }
        if events.insert((c, handle), e).is_some() {
            r.violations
                .push(format!("client {c} handle {handle}: two outcomes"));
        }
    }
}

fn check(sim: &Sim, events: &HashMap<(u32, u64), Event>, consumers: &[Consumer]) -> Vec<String> {
    let mut v = Vec::new();
    // All nodes' histories are prefixes of the longest one (G1 across nodes).
    let longest = sim.nodes.iter().max_by_key(|x| x.history.len()).unwrap();
    for (k, node) in sim.nodes.iter().enumerate() {
        if node.history[..] != longest.history[..node.history.len()] {
            v.push(format!("G1: node {} history diverges", k + 1));
        }
    }
    let stream = &longest.history;
    let mut seen = HashMap::new();
    let mut versions: HashMap<u64, u64> = HashMap::new();
    let mut last_handle: HashMap<u32, u64> = HashMap::new();
    for (i, (meta, pl)) in stream.iter().enumerate() {
        let g = i as u64 + 1;
        let (c, h) = parse_payload(pl);
        if let Some(prev) = seen.insert((c, h), g) {
            v.push(format!("G4: client {c} handle {h} at {prev} and {g}"));
        }
        if let Some(prev) = last_handle.insert(c, h)
            && h <= prev
        {
            v.push(format!("G6: client {c} handle {h} after {prev}"));
        }
        if let Some((key, ver)) = meta.conditional {
            let e = versions.entry(key).or_insert(0);
            if ver != *e + 1 {
                v.push(format!("G5: key {key} reached {ver} after {e}"));
            }
            *e = ver;
        }
    }
    for ((c, h), e) in events {
        match e {
            Event::Accepted { global_seqno, .. } => {
                if seen.get(&(*c, *h)) != Some(global_seqno) {
                    v.push(format!(
                        "client {c} handle {h}: ACCEPTED at {global_seqno}, stream has {:?}",
                        seen.get(&(*c, *h))
                    ));
                }
            }
            Event::Rejected { .. } => {
                if seen.contains_key(&(*c, *h)) {
                    v.push(format!("client {c} handle {h}: REJECTED but published"));
                }
            }
            Event::Failed { .. } => v.push(format!("client {c} handle {h}: failed")),
        }
    }
    for (c, h) in seen.keys() {
        if !matches!(events.get(&(*c, *h)), Some(Event::Accepted { .. })) {
            v.push(format!("client {c} handle {h}: published without ACCEPTED"));
        }
    }
    for (k, cons) in consumers.iter().enumerate() {
        let mut prev = 0;
        for (seqno, meta, pl) in &cons.delivered {
            if *seqno <= prev {
                v.push(format!("G2: consumer {k} delivered {seqno} after {prev}"));
            }
            prev = *seqno;
            match stream.get(*seqno as usize - 1) {
                Some((m, b)) if m == meta && b == pl => {}
                _ => v.push(format!(
                    "G1: consumer {k} seqno {seqno} differs from the nodes"
                )),
            }
        }
        if cons.delivered.len() as u64 + cons.tracker.lost() != stream.len() as u64 {
            v.push(format!(
                "consumer {k}: delivered {} + lost {} != {}",
                cons.delivered.len(),
                cons.tracker.lost(),
                stream.len()
            ));
        }
    }
    v
}

/// Named cluster scenarios for the nightly runner.
pub fn cluster_scenario(name: &str) -> Option<ClusterParams> {
    let d = ClusterParams::default();
    Some(match name {
        "cluster3" => d,
        "cluster5" => ClusterParams { nodes: 5, ..d },
        "cluster3-harsh" => ClusterParams {
            loss: 200,
            dup: 100,
            max_delay_ns: 20_000_000,
            repartition_ms: 100,
            crash_ms: 150,
            ..d
        },
        // Mode (b): power losses, including of the whole cluster.
        "cluster3-b" => ClusterParams {
            durable_acks: true,
            crash_ms: 50,
            ..d
        },
        _ => return None,
    })
}

pub const CLUSTER_SCENARIOS: [&str; 4] = ["cluster3", "cluster5", "cluster3-harsh", "cluster3-b"];

/// Runs a cluster simulation; deterministic in `seed` and `p`.
pub fn simulate(seed: u64, p: &ClusterParams) -> Report {
    run_cluster(seed, p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_node_clusters_under_faults_are_clean() {
        for seed in 0..10 {
            let r = simulate(seed, &ClusterParams::default());
            assert!(
                r.violations.is_empty(),
                "seed {seed}: {:?}",
                &r.violations[..r.violations.len().min(8)]
            );
            assert_eq!(r.accepted + r.rejected, 3 * 150);
        }
    }

    #[test]
    fn five_node_clusters_under_faults_are_clean() {
        let p = ClusterParams {
            nodes: 5,
            ..ClusterParams::default()
        };
        for seed in 100..104 {
            let r = simulate(seed, &p);
            assert!(
                r.violations.is_empty(),
                "seed {seed}: {:?}",
                &r.violations[..r.violations.len().min(8)]
            );
        }
    }

    #[test]
    fn harsh_cluster_scenario_is_clean() {
        let p = cluster_scenario("cluster3-harsh").unwrap();
        for seed in 0..4 {
            let r = simulate(seed, &p);
            assert!(
                r.violations.is_empty(),
                "seed {seed}: {:?}",
                &r.violations[..r.violations.len().min(8)]
            );
        }
    }

    #[test]
    fn mode_b_survives_power_losses() {
        let p = cluster_scenario("cluster3-b").unwrap();
        for seed in 0..10 {
            let r = simulate(seed, &p);
            assert!(
                r.violations.is_empty(),
                "seed {seed}: {:?}",
                &r.violations[..r.violations.len().min(8)]
            );
            assert_eq!(r.accepted + r.rejected, 3 * 150);
        }
    }

    /// The check has teeth: followers acknowledging unsynced entries lose
    /// committed messages to power losses.
    #[test]
    fn acknowledging_unsynced_entries_is_caught() {
        let p = ClusterParams {
            ack_unsynced: true,
            ..cluster_scenario("cluster3-b").unwrap()
        };
        // Caught by a safety check, not only by the livelock guard or its
        // liveness fallout (the leader's stale match index can storm).
        let caught = (0..20).any(|seed| {
            simulate(seed, &p)
                .violations
                .iter()
                .any(|v| !v.starts_with("livelock") && !v.starts_with("liveness"))
        });
        assert!(caught);
    }

    #[test]
    fn cluster_runs_are_deterministic() {
        let p = ClusterParams {
            msgs_per_client: 40,
            ..ClusterParams::default()
        };
        assert_eq!(simulate(3, &p), simulate(3, &p));
    }
}
