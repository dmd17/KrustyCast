#![allow(clippy::needless_range_loop)]
//! Randomized clusters: lossy, duplicating, reordering network, changing
//! partitions, crashes and restarts from storage. Checks Raft's safety
//! properties throughout and liveness once the network heals.
use krusty_raft::wire::{self, EntriesView};
use krusty_raft::{Config, HardState, Message, NodeId, Outbox, Raft, RaftLog, Role};
use std::collections::{BTreeMap, HashMap};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// What a node's disk holds.
#[derive(Default, Clone)]
struct Store {
    hs: HardState,
    log: Vec<(u64, Vec<u8>)>, // index i at log[i - 1]
}

struct Node {
    raft: Option<Raft>,
    store: Store,
    applied: u64,
}

const MS: u64 = 1_000_000;

fn config(id: NodeId, n: u32, seed: u64) -> Config {
    Config {
        retain_entries: 8,      // small: lagging followers are served from storage
        max_append_entries: 12, // larger than the window: batches outrun it
        max_append_bytes: 200,  // some batches are cut short by size
        max_inflight: 4,
        seed,
        ..Config::new(id, (1..=n).collect())
    }
}

fn boot(id: NodeId, n: u32, seed: u64, store: &Store, now: u64) -> Raft {
    let mut log = RaftLog::new(0, 0);
    for (t, d) in &store.log {
        log.append(*t, d);
    }
    Raft::new(config(id, n, seed), store.hs, log, 0, now)
}

/// Carries out an outbox in the required order: persist, then send.
/// Fault intensity.
#[derive(Clone, Copy)]
struct Faults {
    /// Per-mille of messages lost.
    loss: u64,
    max_delay_ms: u64,
    /// Mean steps (0.5 ms) between repartitions / crashes.
    repartition_every: u64,
    crash_every: u64,
}

const GENTLE: Faults = Faults {
    loss: 100,
    max_delay_ms: 5,
    repartition_every: 400,
    crash_every: 600,
};
const HARSH: Faults = Faults {
    loss: 300,
    max_delay_ms: 40,
    repartition_every: 100,
    crash_every: 150,
};

#[allow(clippy::too_many_arguments)]
fn apply_outbox(
    f: Faults,
    id: NodeId,
    node: &mut Node,
    out: &mut Outbox,
    net: &mut Vec<(u64, NodeId, Vec<u8>)>,
    rng: &mut Rng,
    now: u64,
    up: &dyn Fn(NodeId, NodeId) -> bool,
) {
    let raft = node.raft.as_mut().unwrap();
    if let Some(hs) = out.hard_state {
        node.store.hs = hs;
    }
    if let Some(t) = out.truncate_from {
        node.store.log.truncate(t as usize - 1);
    }
    if let Some(from) = out.store_from {
        node.store.log.truncate(from as usize - 1);
        for i in from..=raft.last_index() {
            let (t, d) = raft.entry(i).expect("new entries are in memory");
            node.store.log.push((t, d.to_vec()));
        }
    }
    let last = raft.last_index();
    let mut more = Outbox::default();
    raft.persisted(last, &mut more);
    out.sends.append(&mut more.sends);
    for (to, m) in out.sends.drain(..) {
        if !up(id, to) || rng.below(1000) < f.loss {
            continue; // partitioned, or lost
        }
        let mut buf = vec![0u8; 1 << 16];
        let mut m = m;
        // Fill prev_term and entries from memory, or from storage if compacted.
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
        let copies = if rng.below(20) == 0 { 2 } else { 1 };
        for _ in 0..copies {
            net.push((now + rng.below(f.max_delay_ms * MS + 1), to, buf.clone()));
        }
    }
}

fn run(seed: u64, n: u32, f: Faults) {
    let mut rng = Rng(seed);
    let mut nodes: Vec<Node> = (1..=n)
        .map(|id| Node {
            raft: Some(boot(id, n, seed, &Store::default(), 0)),
            store: Store::default(),
            applied: 0,
        })
        .collect();
    let mut net: Vec<(u64, NodeId, Vec<u8>)> = Vec::new();
    let mut leaders: HashMap<u64, NodeId> = HashMap::new(); // term -> leader
    let mut committed: Vec<(u64, Vec<u8>)> = Vec::new(); // the one true committed log
    let mut proposed = 0u64;
    // Partition: group[id] (nodes talk only within their group).
    let mut group: Vec<u32> = vec![0; n as usize + 1];
    let end = 6_000 * MS;
    let heal_at = 4_000 * MS;
    let mut now = 0u64;
    let mut offsets = Vec::new();
    while now < end {
        now += MS / 2;
        let faulty = now < heal_at;
        // Faults: repartition, crash, restart.
        if faulty && rng.below(f.repartition_every) == 0 {
            for g in group.iter_mut().skip(1) {
                *g = rng.below(2) as u32;
            }
        }
        if !faulty {
            group.iter_mut().for_each(|g| *g = 0);
        }
        if faulty && rng.below(f.crash_every) == 0 {
            let k = rng.below(n as u64) as usize;
            nodes[k].raft = None; // crash: memory gone, the store survives
        }
        for (k, node) in nodes.iter_mut().enumerate() {
            if node.raft.is_none() && (!faulty || rng.below(300) == 0) {
                node.raft = Some(boot(k as u32 + 1, n, seed ^ now, &node.store, now));
                node.applied = 0;
            }
        }
        let g = group.clone();
        let up = move |a: NodeId, b: NodeId| g[a as usize] == g[b as usize];
        // Deliver due messages (any order).
        let mut due: Vec<(u64, NodeId, Vec<u8>)> = Vec::new();
        net.retain(|m| {
            if m.0 <= now {
                due.push(m.clone());
                false
            } else {
                true
            }
        });
        for i in (1..due.len()).rev() {
            due.swap(i, rng.below(i as u64 + 1) as usize);
        }
        for (_, to, bytes) in due {
            let node = &mut nodes[to as usize - 1];
            let Some(raft) = node.raft.as_mut() else {
                continue;
            };
            let (from, m, entries) =
                wire::decode(&bytes, std::mem::take(&mut offsets)).expect("valid message");
            let mut out = Outbox::default();
            raft.on_message(from, m, &entries, &mut out);
            offsets = entries.into_offsets();
            apply_outbox(f, to, node, &mut out, &mut net, &mut rng, now, &up);
        }
        // Ticks and proposals.
        for k in 0..n as usize {
            let id = k as u32 + 1;
            let node = &mut nodes[k];
            let Some(raft) = node.raft.as_mut() else {
                continue;
            };
            let mut out = Outbox::default();
            raft.tick(now, &mut out);
            if raft.role() == Role::Leader && rng.below(4) == 0 {
                proposed += 1;
                raft.propose(&proposed.to_le_bytes(), &mut out);
                raft.replicate(&mut out, false);
            }
            apply_outbox(f, id, node, &mut out, &mut net, &mut rng, now, &up);
        }
        // Invariants.
        for (k, node) in nodes.iter_mut().enumerate() {
            let Some(raft) = node.raft.as_mut() else {
                continue;
            };
            if raft.role() == Role::Leader {
                // Election safety: one leader per term.
                let prev = leaders.insert(raft.term(), k as u32 + 1);
                assert!(
                    prev.is_none() || prev == Some(k as u32 + 1),
                    "seed {seed}: two leaders in term {}",
                    raft.term()
                );
                // Leader completeness: holds everything committed so far.
                for (i, e) in committed.iter().enumerate() {
                    assert_eq!(
                        &node.store.log[i],
                        e,
                        "seed {seed}: leader {} lacks committed entry {}",
                        k + 1,
                        i + 1
                    );
                }
            }
            // State machine safety: committed entries agree everywhere and never change.
            let c = raft.commit_index();
            assert!(
                c <= node.store.log.len() as u64,
                "seed {seed}: committed beyond the stored log"
            );
            for i in node.applied + 1..=c {
                let e = &node.store.log[i as usize - 1];
                match committed.get(i as usize - 1) {
                    Some(x) => assert_eq!(
                        x,
                        e,
                        "seed {seed}: node {} committed a different entry {i}",
                        k + 1
                    ),
                    None => {
                        assert_eq!(committed.len() as u64, i - 1);
                        committed.push(e.clone());
                    }
                }
            }
            node.applied = node.applied.max(c);
            raft.applied(node.applied);
        }
    }
    // Liveness: after healing, a leader exists and work got committed.
    let leaders_now = nodes
        .iter()
        .filter(|x| x.raft.as_ref().map(|r| r.role()) == Some(Role::Leader))
        .count();
    assert_eq!(
        leaders_now, 1,
        "seed {seed}: {leaders_now} leaders after healing"
    );
    let committed_ids: BTreeMap<u64, ()> = committed
        .iter()
        .filter(|(_, d)| d.len() == 8)
        .map(|(_, d)| (u64::from_le_bytes(d[..8].try_into().unwrap()), ()))
        .collect();
    assert_eq!(
        committed_ids.len(),
        committed.iter().filter(|(_, d)| d.len() == 8).count(),
        "seed {seed}: a proposal committed twice"
    );
    assert!(
        committed.len() > 50,
        "seed {seed}: only {} entries committed",
        committed.len()
    );
}

#[test]
fn three_nodes_under_faults() {
    for seed in 0..40 {
        run(seed, 3, GENTLE);
    }
}

#[test]
fn five_nodes_under_faults() {
    for seed in 100..120 {
        run(seed, 5, GENTLE);
    }
}

/// A long sweep: `KRUSTY_RAFT_SEEDS=N cargo test --release -p krusty-raft -- --ignored`.
#[test]
#[ignore]
fn harsh_sweep() {
    let var = |k: &str, d: u64| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let (n, base) = (
        var("KRUSTY_RAFT_SEEDS", 2000),
        var("KRUSTY_RAFT_BASE", 10_000),
    );
    for seed in base..base + n {
        run(seed, 3, HARSH);
    }
}

/// Long delays relative to election timeouts, heavy loss, constant
/// repartitions and crashes: elections collide and logs diverge.
#[test]
fn harsh_faults() {
    for seed in 200..260 {
        run(seed, 3, HARSH);
    }
    for seed in 300..320 {
        run(seed, 5, HARSH);
    }
}

#[test]
fn wire_round_trip() {
    let mut buf = vec![0u8; 1024];
    let m = Message::Append {
        term: 7,
        prev_index: 10,
        prev_term: 6,
        count: 2,
        commit: 9,
    };
    let mut at = wire::encode(&mut buf, 3, &m);
    at = wire::push_entry(&mut buf, at, 7, b"hello");
    at = wire::push_entry(&mut buf, at, 7, b"");
    let (from, d, e) = wire::decode(&buf[..at], Vec::new()).unwrap();
    assert_eq!((from, d), (3, m));
    use krusty_raft::Entries;
    assert_eq!(
        (e.len(), e.term(0), e.data(0), e.data(1)),
        (2, 7, &b"hello"[..], &b""[..])
    );
    assert!(wire::decode(&buf[..at - 1], Vec::new()).is_none());
    let _ = EntriesView::empty();
}
