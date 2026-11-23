#![allow(clippy::needless_range_loop)]
//! Trace export for validation against the TLA+ spec (gap Q3,
//! `spec/tla/TraceRaftKC.tla`).
//!
//! A short randomized run (loss, delay, reordering, partitions, crashes and
//! restarts; no duplicates, see the TLA+ module) logs one record per call
//! into the Raft core: the node, the message it received and the messages it
//! sent in RaftKC's form, its state afterwards, and the RaftKC actions that
//! explain the call. Calls that change nothing RaftKC sees (pre-vote, quiet
//! ticks) are left out: they are stuttering steps.
//!
//! `KRUSTY_RAFT_TRACE_DIR=dir cargo test -p krusty-raft --test trace -- --ignored`
//! writes `dir/<seed>/TraceData.tla` for `KRUSTY_RAFT_TRACE_SEEDS` seeds
//! (default 3); `spec/tla/check-traces.sh dir` checks them with TLC.
use krusty_raft::wire::{self, EntriesView};
use krusty_raft::{Config, Entries, HardState, Message, NodeId, Outbox, Raft, RaftLog, Role};
use std::fmt::Write as _;

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

#[derive(Default, Clone)]
struct Store {
    hs: HardState,
    log: Vec<(u64, Vec<u8>)>,
}

struct Node {
    raft: Option<Raft>,
    /// What survives a process crash (the page cache).
    store: Store,
    /// The log as last fsynced: what survives a power loss (mode a).
    disk: Vec<(u64, Vec<u8>)>,
    /// The `recovering` marker: set by a power loss, kept until recovered.
    marker: bool,
}

const MS: u64 = 1_000_000;

fn config(id: NodeId, n: u32, seed: u64) -> Config {
    Config {
        retain_entries: 4,
        max_append_entries: 3,
        max_inflight: 2,
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

fn value(d: &[u8]) -> String {
    if d.is_empty() {
        "\"noop\"".into()
    } else {
        format!("\"v{}\"", u64::from_le_bytes(d[..8].try_into().unwrap()))
    }
}

fn entry(t: u64, d: &[u8]) -> String {
    format!("[term |-> {t}, value |-> {}]", value(d))
}

fn node(id: NodeId) -> String {
    format!("\"n{id}\"")
}

/// RaftKC's form of a message (None for pre-vote and leadership transfer's
/// TimeoutNow, which RaftKC lacks: the election it starts is a `timeout`).
fn spec_msg(from: NodeId, to: NodeId, m: &Message, entries: &[(u64, Vec<u8>)]) -> Option<String> {
    let (f, t) = (node(from), node(to));
    Some(match *m {
        Message::PreVote { .. } | Message::PreVoteReply { .. } | Message::TimeoutNow { .. } => {
            return None;
        }
        Message::Vote {
            term,
            last_index,
            last_term,
        } => format!(
            "[type |-> \"vote\", term |-> {term}, from |-> {f}, to |-> {t}, lastTerm |-> {last_term}, lastIndex |-> {last_index}]"
        ),
        Message::VoteReply { term, granted } => format!(
            "[type |-> \"voteReply\", term |-> {term}, from |-> {f}, to |-> {t}, granted |-> {}]",
            if granted { "TRUE" } else { "FALSE" }
        ),
        Message::Append {
            term,
            prev_index,
            prev_term,
            count,
            commit,
            last,
        } => {
            let es: Vec<String> = entries.iter().map(|(t, d)| entry(*t, d)).collect();
            format!(
                "[type |-> \"append\", term |-> {term}, from |-> {f}, to |-> {t}, prev |-> {prev_index}, prevTerm |-> {prev_term}, entries |-> <<{}>>, commit |-> {}, last |-> {last}]",
                es.join(", "),
                commit.min(prev_index + count)
            )
        }
        Message::AppendReply {
            term,
            success,
            index,
        } => format!(
            "[type |-> \"appendReply\", term |-> {term}, from |-> {f}, to |-> {t}, success |-> {}, match |-> {}]",
            if success { "TRUE" } else { "FALSE" },
            if success { index } else { 0 }
        ),
    })
}

fn role(r: Role) -> &'static str {
    match r {
        Role::Follower => "follower",
        Role::PreCandidate => "precandidate",
        Role::Candidate => "candidate",
        Role::Leader => "leader",
    }
}

/// What a call into the core was.
enum Call {
    Tick,
    Receive(String, bool), // RaftKC form of the message (if any); can it start an election
    Propose(String),
    Restart,
    /// A boot with the recovering marker: after a power loss (the log is
    /// what the disk kept), or a crash while recovering.
    PowerLoss,
}

struct Before {
    role: Role,
    term: u64,
}

struct Tracer {
    records: Vec<String>,
    /// Calls that removed log entries (a follower dropping a conflict).
    truncations: u64,
    elections: u64,
    transfers: u64,
}

impl Tracer {
    /// Records a call: `out` is what the core produced (before the shell
    /// persisted), `c1`/`c2` the commit index after the call and after
    /// `persisted`.
    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        id: NodeId,
        call: Call,
        before: Before,
        c0: u64,
        c1: u64,
        raft: &Raft,
        store: &Store,
        sends: &[(NodeId, Message, Option<String>)],
    ) {
        let mut plan: Vec<String> = Vec::new();
        let mut msg = "[type |-> \"none\"]".to_string();
        let leader_now = raft.role() == Role::Leader;
        match &call {
            Call::Tick => {
                if before.role == Role::Leader && raft.role() == Role::Follower {
                    plan.push("[a |-> \"stepDown\"]".into());
                }
            }
            Call::Receive(m, pre_vote_reply) => {
                if !m.is_empty() {
                    msg = m.clone();
                    plan.push("[a |-> \"inject\"]".into());
                    plan.push("[a |-> \"receive\"]".into());
                }
                if *pre_vote_reply && raft.term() > before.term {
                    if raft.role() == Role::Candidate && raft.term() == before.term + 1 {
                        plan.push("[a |-> \"timeout\"]".into());
                    } else {
                        // Rejected by a node at a later term: caught up to it.
                        plan.push(format!("[a |-> \"learnTerm\", term |-> {}]", raft.term()));
                    }
                }
                if before.role != Role::Leader && leader_now {
                    plan.push("[a |-> \"becomeLeader\"]".into());
                }
                if leader_now && before.role == Role::Leader && c1 > c0 {
                    plan.push(format!("[a |-> \"advanceCommit\", n |-> {c1}]"));
                }
            }
            Call::Propose(v) => plan.push(format!("[a |-> \"clientRequest\", v |-> {v}]")),
            Call::Restart => plan.push("[a |-> \"restart\"]".into()),
            Call::PowerLoss => plan.push("[a |-> \"powerLoss\"]".into()),
        }
        // Requests the core sent (replies come with `receive`).
        for (to, m, _) in sends {
            match *m {
                Message::Vote { .. } => {
                    plan.push(format!("[a |-> \"requestVote\", t |-> {}]", node(*to)))
                }
                Message::Append {
                    prev_index, count, ..
                } => plan.push(format!(
                    "[a |-> \"append\", t |-> {}, prev |-> {prev_index}, last |-> {}]",
                    node(*to),
                    prev_index + count
                )),
                _ => {}
            }
        }
        let c2 = raft.commit_index();
        if leader_now && c2 > c1 {
            plan.push(format!("[a |-> \"advanceCommit\", n |-> {c2}]"));
        }
        let spec_sends: Vec<&String> = sends.iter().filter_map(|s| s.2.as_ref()).collect();
        let changed = !plan.is_empty() || !spec_sends.is_empty();
        if !changed {
            return; // a stuttering step for RaftKC
        }
        let log: Vec<String> = store.log.iter().map(|(t, d)| entry(*t, d)).collect();
        let voted = match raft.hard_state().voted_for {
            Some(v) => node(v),
            None => "\"nil\"".into(),
        };
        let mut r = String::new();
        let _ = write!(
            r,
            "[node |-> {}, msg |-> {msg}, plan |-> <<{}>>, term |-> {}, role |-> \"{}\", votedFor |-> {voted}, log |-> <<{}>>, commit |-> {}, recovering |-> {}, sends |-> {{{}}}]",
            node(id),
            plan.join(", "),
            raft.term(),
            role(raft.role()),
            log.join(", "),
            c2,
            if raft.recovering() { "TRUE" } else { "FALSE" },
            spec_sends
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        self.records.push(r);
    }
}

type Net = Vec<(u64, NodeId, Vec<u8>, NodeId, Option<String>)>;

/// Carries out an outbox (persist, then send) and records the call.
#[allow(clippy::too_many_arguments)]
fn carry_out(
    id: NodeId,
    node: &mut Node,
    mut out: Outbox,
    call: Call,
    before: Before,
    c0: u64,
    net: &mut Net,
    rng: &mut Rng,
    now: u64,
    up: &dyn Fn(NodeId, NodeId) -> bool,
    tracer: &mut Tracer,
) {
    let raft = node.raft.as_mut().unwrap();
    let c1 = raft.commit_index();
    if out.truncate_from.is_some() {
        tracer.truncations += 1;
    }
    if before.role != Role::Leader && raft.role() == Role::Leader {
        tracer.elections += 1;
    }
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
    // Fill each message (prev_term and entries from memory or storage).
    let mut sends: Vec<(NodeId, Message, Option<String>)> = Vec::new();
    let mut wires: Vec<(NodeId, Vec<u8>)> = Vec::new();
    for (to, mut m) in out.sends.drain(..) {
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
        let mut entries = Vec::new();
        if let Message::Append {
            prev_index, count, ..
        } = m
        {
            for i in prev_index + 1..=prev_index + count {
                entries.push(match raft.entry(i) {
                    Some((t, d)) => (t, d.to_vec()),
                    None => node.store.log[i as usize - 1].clone(),
                });
            }
        }
        let mut buf = vec![0u8; 1 << 16];
        let mut at = wire::encode(&mut buf, id, &m);
        for (t, d) in &entries {
            at = wire::push_entry(&mut buf, at, *t, d);
        }
        buf.truncate(at);
        sends.push((to, m, spec_msg(id, to, &m, &entries)));
        wires.push((to, buf));
    }
    tracer.record(id, call, before, c0, c1, raft, &node.store, &sends);
    if node.marker && !raft.recovering() {
        node.marker = false; // recovered: the marker goes
    }
    for ((to, buf), (_, _, spec)) in wires.into_iter().zip(sends) {
        if !up(id, to) || rng.below(1000) < 100 {
            continue;
        }
        net.push((now + rng.below(15 * MS + 1), to, buf, id, spec));
    }
}

fn run(seed: u64, n: u32) -> Tracer {
    let mut rng = Rng(seed);
    let mut tracer = Tracer {
        records: Vec::new(),
        truncations: 0,
        elections: 0,
        transfers: 0,
    };
    let mut nodes: Vec<Node> = (1..=n)
        .map(|id| Node {
            raft: Some(boot(id, n, seed, &Store::default(), 0, false)),
            store: Store::default(),
            disk: Vec::new(),
            marker: false,
        })
        .collect();
    let mut net: Net = Vec::new();
    let mut group: Vec<u32> = vec![0; n as usize + 1];
    let mut proposed = 0u64;
    let mut now = 0u64;
    let mut offsets = Vec::new();
    while now < 1500 * MS {
        now += MS / 2;
        if rng.below(120) == 0 {
            for g in group.iter_mut().skip(1) {
                *g = rng.below(2) as u32;
            }
        }
        if now > 1300 * MS {
            group.iter_mut().for_each(|g| *g = 0);
        }
        if now < 1200 * MS && rng.below(250) == 0 {
            let k = rng.below(n as u64) as usize;
            nodes[k].raft = None;
        }
        // Mode (a): the OS writes logs back now and then; a power loss
        // keeps only that (one node at a time, f = 1).
        for node in nodes.iter_mut() {
            if rng.below(40) == 0 {
                node.disk = node.store.log.clone();
            }
        }
        if now < 1200 * MS && rng.below(400) == 0 && !nodes.iter().any(|x| x.marker) {
            let k = rng.below(n as u64) as usize;
            let node = &mut nodes[k];
            node.raft = None;
            node.store.log = node.disk.clone();
            node.marker = true;
        }
        let g = group.clone();
        let up = move |a: NodeId, b: NodeId| g[a as usize] == g[b as usize];
        for k in 0..n as usize {
            if nodes[k].raft.is_none() && rng.below(100) == 0 {
                let id = k as u32 + 1;
                let node = &mut nodes[k];
                let before = Before {
                    role: Role::Follower,
                    term: node.store.hs.term,
                };
                let recover = node.marker;
                node.raft = Some(boot(id, n, seed ^ now, &node.store, now, recover));
                carry_out(
                    id,
                    node,
                    Outbox::default(),
                    if recover {
                        Call::PowerLoss
                    } else {
                        Call::Restart
                    },
                    before,
                    0,
                    &mut net,
                    &mut rng,
                    now,
                    &up,
                    &mut tracer,
                );
            }
        }
        let mut due: Net = Vec::new();
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
        for (_, to, bytes, _, spec) in due {
            let node = &mut nodes[to as usize - 1];
            let Some(raft) = node.raft.as_mut() else {
                continue;
            };
            let (from, m, entries) =
                wire::decode(&bytes, std::mem::take(&mut offsets)).expect("valid message");
            let before = Before {
                role: raft.role(),
                term: raft.term(),
            };
            let c0 = raft.commit_index();
            // A won pre-vote, or a leader's TimeoutNow, starts an election.
            let pre_vote_reply =
                matches!(m, Message::PreVoteReply { .. } | Message::TimeoutNow { .. });
            let mut out = Outbox::default();
            raft.on_message(from, m, &entries, &mut out);
            let _ = entries.len();
            offsets = entries.into_offsets();
            carry_out(
                to,
                node,
                out,
                Call::Receive(spec.unwrap_or_default(), pre_vote_reply),
                before,
                c0,
                &mut net,
                &mut rng,
                now,
                &up,
                &mut tracer,
            );
        }
        for k in 0..n as usize {
            let id = k as u32 + 1;
            let node = &mut nodes[k];
            let Some(raft) = node.raft.as_mut() else {
                continue;
            };
            let before = Before {
                role: raft.role(),
                term: raft.term(),
            };
            let c0 = raft.commit_index();
            let mut out = Outbox::default();
            raft.tick(now, &mut out);
            carry_out(
                id,
                node,
                out,
                Call::Tick,
                before,
                c0,
                &mut net,
                &mut rng,
                now,
                &up,
                &mut tracer,
            );
            let raft = node.raft.as_mut().unwrap();
            // Now and then the leader hands over to a random peer (a stutter
            // step for RaftKC; the election it leads to is the target's).
            if raft.role() == Role::Leader && raft.transferring().is_none() && rng.below(200) == 0 {
                let to = 1 + (id + rng.below(n as u64 - 1) as u32) % n;
                let before = Before {
                    role: raft.role(),
                    term: raft.term(),
                };
                let c0 = raft.commit_index();
                let mut out = Outbox::default();
                assert!(raft.transfer_leadership(to, &mut out));
                tracer.transfers += 1;
                carry_out(
                    id,
                    node,
                    out,
                    Call::Tick,
                    before,
                    c0,
                    &mut net,
                    &mut rng,
                    now,
                    &up,
                    &mut tracer,
                );
            }
            let raft = node.raft.as_mut().unwrap();
            if raft.role() == Role::Leader && raft.transferring().is_none() && rng.below(10) == 0 {
                proposed += 1;
                let before = Before {
                    role: raft.role(),
                    term: raft.term(),
                };
                let c0 = raft.commit_index();
                let mut out = Outbox::default();
                let d = proposed.to_le_bytes();
                raft.propose(&d, &mut out);
                raft.replicate(&mut out, false);
                carry_out(
                    id,
                    node,
                    out,
                    Call::Propose(value(&d)),
                    before,
                    c0,
                    &mut net,
                    &mut rng,
                    now,
                    &up,
                    &mut tracer,
                );
            }
            let raft = node.raft.as_mut().unwrap();
            let c = raft.commit_index();
            raft.applied(c);
        }
    }
    let _ = EntriesView::empty();
    tracer
}

fn write_trace(dir: &std::path::Path, seed: u64, n: u32, t: &Tracer) {
    let d = dir.join(seed.to_string());
    std::fs::create_dir_all(&d).unwrap();
    let servers: Vec<String> = (1..=n).map(node).collect();
    let body = format!(
        "---- MODULE TraceData ----\n\
         \\* Generated by crates/krusty-raft/tests/trace.rs, seed {seed}.\n\
         TraceServers == {{{}}}\n\
         Trace == <<\n{}\n>>\n\
         ====\n",
        servers.join(", "),
        t.records.join(",\n")
    );
    std::fs::write(d.join("TraceData.tla"), body).unwrap();
}

#[test]
#[ignore]
fn export_traces() {
    let dir = std::env::var("KRUSTY_RAFT_TRACE_DIR").expect("KRUSTY_RAFT_TRACE_DIR");
    let seeds: u64 = std::env::var("KRUSTY_RAFT_TRACE_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let base: u64 = std::env::var("KRUSTY_RAFT_TRACE_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    for seed in base..base + seeds {
        let t = run(seed, 3);
        write_trace(std::path::Path::new(&dir), seed, 3, &t);
        eprintln!(
            "seed {seed}: {} records, {} elections, {} truncations, {} transfers",
            t.records.len(),
            t.elections,
            t.truncations,
            t.transfers
        );
    }
}

/// The generator's runs exercise what trace validation is for: several
/// elections, commits and followers dropping conflicting entries.
#[test]
fn traces_cover_elections_commits_and_conflicts() {
    let (mut elections, mut truncations, mut commits, mut transfers) = (0, 0, 0, 0);
    let mut power_losses = 0;
    // (Ten seeds: with power losses in the runs, five no longer always
    // included a follower dropping conflicting entries.)
    for seed in 0..10 {
        let t = run(seed, 3);
        elections += t.elections;
        truncations += t.truncations;
        transfers += t.transfers;
        commits += t
            .records
            .iter()
            .filter(|r| r.contains("\"advanceCommit\""))
            .count();
        power_losses += t
            .records
            .iter()
            .filter(|r| r.contains("\"powerLoss\""))
            .count();
    }
    eprintln!(
        "{elections} elections, {truncations} truncations, {commits} commits, {transfers} transfers, {power_losses} power losses"
    );
    assert!(power_losses >= 5, "{power_losses} power losses");
    assert!(elections >= 10, "{elections} elections");
    assert!(truncations >= 1, "no conflicts");
    assert!(commits >= 50, "{commits} commits");
    assert!(transfers >= 3, "{transfers} leadership transfers");
}
