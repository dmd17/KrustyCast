#![allow(clippy::needless_range_loop)]
//! Randomized clusters: lossy, duplicating, reordering network, changing
//! partitions, crashes and restarts from storage, and (with `power_every`)
//! power losses in mode (a): a node's log reverts to what was last fsynced,
//! and it restarts recovering (spec/RAFT-VERIFICATION.md, "Decision").
//! Checks Raft's safety properties throughout and liveness once the network
//! heals.
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
    /// What survives a process crash (the page cache).
    store: Store,
    /// The log as last fsynced: what survives a power loss. The hard state
    /// is fsynced before use, so it always survives.
    disk: Vec<(u64, Vec<u8>)>,
    /// The `recovering` marker: set by a power loss, kept until Raft has
    /// recovered (so a crash while recovering does not end it).
    marker: bool,
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

fn boot(id: NodeId, n: u32, seed: u64, store: &Store, now: u64, recover: bool) -> Raft {
    let mut log = RaftLog::new(0, 0);
    for (t, d) in &store.log {
        log.append(*t, d);
    }
    let mut r = Raft::new(config(id, n, seed), store.hs, log, 0, now);
    if recover {
        r.recover_after_power_loss();
    }
    r
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
    /// Mean steps between power losses (0: none). At most one node is
    /// down from one, or recovering, at a time (f = 1 of 3).
    power_every: u64,
    /// Whether a node back from a power loss recovers (the fix); false is
    /// the code before it, for the check that the harness sees the bug.
    recover: bool,
}

const GENTLE: Faults = Faults {
    loss: 100,
    max_delay_ms: 5,
    repartition_every: 400,
    crash_every: 600,
    power_every: 0,
    recover: true,
};
const HARSH: Faults = Faults {
    loss: 300,
    max_delay_ms: 40,
    repartition_every: 100,
    crash_every: 150,
    power_every: 0,
    recover: true,
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

/// Runs one simulation; returns how many leadership transfers completed
/// (the next leader was the one the transfer named).
fn run(seed: u64, n: u32, f: Faults) -> u64 {
    let mut rng = Rng(seed);
    let mut nodes: Vec<Node> = (1..=n)
        .map(|id| Node {
            raft: Some(boot(id, n, seed, &Store::default(), 0, false)),
            store: Store::default(),
            disk: Vec::new(),
            marker: false,
            applied: 0,
        })
        .collect();
    let mut net: Vec<(u64, NodeId, Vec<u8>)> = Vec::new();
    let mut leaders: HashMap<u64, NodeId> = HashMap::new(); // term -> leader
    let mut committed: Vec<(u64, Vec<u8>)> = Vec::new(); // the one true committed log
    // The term of the node that first saw each entry committed: at or above
    // the term it was committed in.
    let mut committed_in: Vec<u64> = Vec::new();
    let mut proposed = 0u64;
    // A transfer under way: (target, term it started in).
    let mut transfer: Option<(NodeId, u64)> = None;
    let mut handovers = 0u64;
    let mut power_down: Option<usize> = None;
    let mut power_losses = 0u64;
    // Partition: group[id] (nodes talk only within their group).
    let mut group: Vec<u32> = vec![0; n as usize + 1];
    let end = std::env::var("KRUSTY_RAFT_END_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        // With power losses a node may still be recovering when the network
        // heals (it votes only once a leader exists): the other two must then
        // agree, which under heavy loss takes longer, as with a node down.
        .unwrap_or(if f.power_every > 0 { 10_000 } else { 6_000 })
        * MS;
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
        // The OS writes logs back now and then (mode a: not before acking).
        for node in nodes.iter_mut() {
            if rng.below(40) == 0 {
                node.disk = node.store.log.clone();
            }
        }
        if faulty
            && f.power_every > 0
            && rng.below(f.power_every) == 0
            && power_down.is_none()
            && !nodes.iter().any(|x| x.marker)
        {
            // Power loss: what was not fsynced is gone.
            let k = rng.below(n as u64) as usize;
            let node = &mut nodes[k];
            node.raft = None;
            node.store.log = node.disk.clone();
            node.marker = f.recover;
            power_down = Some(k);
            power_losses += 1;
        }
        for (k, node) in nodes.iter_mut().enumerate() {
            if node.raft.is_none() && (!faulty || rng.below(300) == 0) {
                node.raft = Some(boot(
                    k as u32 + 1,
                    n,
                    seed ^ now,
                    &node.store,
                    now,
                    node.marker,
                ));
                node.applied = 0;
                if power_down == Some(k) {
                    power_down = None;
                }
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
            // Now and then the leader hands over to a random peer.
            // Only while faults run: a handover is an election, and under the
            // harsh model (delays near the election timeout) elections can
            // cascade; the liveness check after healing is about recovery,
            // not about transfers (see leadership_transfers_hand_over).
            if faulty
                && raft.role() == Role::Leader
                && raft.transferring().is_none()
                && rng.below(400) == 0
            {
                let to = 1 + (id + rng.below(n as u64 - 1) as u32) % n;
                assert!(raft.transfer_leadership(to, &mut out));
                transfer = Some((to, raft.term()));
            }
            if raft.role() == Role::Leader && rng.below(4) == 0 {
                proposed += 1;
                raft.propose(&proposed.to_le_bytes(), &mut out);
                raft.replicate(&mut out, false);
            }
            apply_outbox(f, id, node, &mut out, &mut net, &mut rng, now, &up);
        }
        if std::env::var_os("KRUSTY_RAFT_DEBUG").is_some() && now.is_multiple_of(100 * MS) {
            let st: Vec<String> = nodes
                .iter()
                .map(|x| match &x.raft {
                    Some(r) => format!(
                        "{:?}/t{}/log{}/c{}{}",
                        r.role(),
                        r.term(),
                        r.last_index(),
                        r.commit_index(),
                        if r.recovering() { "/REC" } else { "" }
                    ),
                    None => "down".into(),
                })
                .collect();
            let stores: Vec<String> = nodes
                .iter()
                .map(|x| {
                    format!(
                        "{}/{}{}",
                        x.store.log.len(),
                        x.disk.len(),
                        if x.marker { "M" } else { "" }
                    )
                })
                .collect();
            eprintln!(
                "seed {seed} t={}ms net={} {} stores {}",
                now / MS,
                net.len(),
                st.join(" "),
                stores.join(" ")
            );
        }
        // Invariants.
        for (k, node) in nodes.iter_mut().enumerate() {
            let Some(raft) = node.raft.as_mut() else {
                continue;
            };
            if raft.role() == Role::Leader {
                // Election safety: one leader per term.
                if let Some((to, t)) = transfer
                    && raft.term() > t
                {
                    if to == k as u32 + 1 {
                        handovers += 1;
                    }
                    transfer = None;
                }
                let prev = leaders.insert(raft.term(), k as u32 + 1);
                assert!(
                    prev.is_none() || prev == Some(k as u32 + 1),
                    "seed {seed}: two leaders in term {}",
                    raft.term()
                );
                // Leader completeness: holds everything committed in its
                // term or earlier. A stale leader of an older term need not
                // hold what a newer one committed (it cannot commit).
                let term = raft.term();
                for (i, e) in committed.iter().enumerate() {
                    if committed_in[i] > term {
                        continue;
                    }
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
                        committed_in.push(raft.term());
                    }
                }
            }
            node.applied = node.applied.max(c);
            raft.applied(node.applied);
            if node.marker && !raft.recovering() {
                node.marker = false; // recovered: the marker goes
            }
        }
    }
    assert!(
        f.power_every == 0 || power_losses > 0,
        "seed {seed}: no power loss happened"
    );
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
    handovers
}

/// On a quiet network a transfer hands leadership to the named peer.
#[test]
fn leadership_transfers_hand_over() {
    const QUIET: Faults = Faults {
        loss: 0,
        max_delay_ms: 1,
        repartition_every: u64::MAX,
        crash_every: u64::MAX,
        power_every: 0,
        recover: true,
    };
    let mut handovers = 0;
    for seed in 0..10 {
        handovers += run(seed, 3, QUIET);
    }
    // About one transfer per 200 ms of leadership, over 10 runs of 6 s.
    assert!(handovers >= 100, "{handovers} handovers");
}

#[test]
fn three_nodes_under_faults() {
    for seed in 0..40 {
        run(seed, 3, GENTLE);
    }
}

/// Power losses in mode (a): one node at a time loses what it had not
/// fsynced and restarts recovering.
#[test]
fn three_nodes_with_power_losses() {
    for seed in 300..360 {
        run(
            seed,
            3,
            Faults {
                power_every: 300,
                ..GENTLE
            },
        );
    }
}

#[test]
fn harsh_faults_with_power_losses() {
    for seed in 400..430 {
        run(
            seed,
            3,
            Faults {
                power_every: 120,
                ..HARSH
            },
        );
    }
}

/// Without recovery (the code before 2026-11-23), the same power losses
/// break Raft's safety in some runs: the harness can see the bug.
#[test]
fn power_losses_without_recovery_break_safety() {
    let quiet = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let mut broken = Vec::new();
    for seed in 500..800 {
        let r = std::panic::catch_unwind(|| {
            run(
                seed,
                3,
                Faults {
                    power_every: 120,
                    recover: false,
                    ..HARSH
                },
            )
        });
        if let Err(e) = r {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            if msg.contains("two leaders")
                || msg.contains("lacks committed")
                || msg.contains("committed a different")
            {
                broken.push(seed);
            }
        }
    }
    std::panic::set_hook(quiet);
    assert!(!broken.is_empty(), "no safety violation without recovery");
    eprintln!(
        "safety violated without recovery in {} of 300 runs",
        broken.len()
    );
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
        // Every other seed with power losses in mode (a) (recovering nodes).
        let f = if seed % 2 == 0 {
            HARSH
        } else {
            Faults {
                power_every: 120,
                ..HARSH
            }
        };
        run(seed, 3, f);
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
        last: 12,
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
    let t = Message::TimeoutNow { term: 12 };
    let k = wire::encode(&mut buf, 2, &t);
    let (from, d, _) = wire::decode(&buf[..k], Vec::new()).unwrap();
    assert_eq!((from, d), (2, t));
    let _ = EntriesView::empty();
}
