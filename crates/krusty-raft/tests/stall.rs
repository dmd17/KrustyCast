//! A node that was held up (its thread blocked behind a full ring or a slow
//! segment roll, BASELINE E3) must not mistake its own stall for its
//! peers' silence: messages that waited for it count as heard when it
//! handles them, provided the caller advances the clock first
//! (`Raft::advance_clock`), as krusty-node's cluster stage does.
use krusty_raft::wire::{self, EntriesView};
use krusty_raft::{Config, HardState, Message, NodeId, Outbox, Raft, RaftLog, Role};

const MS: u64 = 1_000_000;

struct Net {
    nodes: Vec<Raft>,
    /// Messages in flight: (to, encoded).
    queue: Vec<(NodeId, Vec<u8>)>,
}

impl Net {
    /// Three nodes; node `fast` has election timeouts [min, max] ms and the
    /// others the defaults (50-100 ms).
    fn new(fast: NodeId, min: u64, max: u64) -> Net {
        let nodes = (1..=3)
            .map(|id| {
                let mut c = Config::new(id, vec![1, 2, 3]);
                c.seed = id as u64;
                if id == fast {
                    c.election_min_ns = min * MS;
                    c.election_max_ns = max * MS;
                }
                Raft::new(c, HardState::default(), RaftLog::new(0, 0), 0, 0)
            })
            .collect();
        Net {
            nodes,
            queue: Vec::new(),
        }
    }

    fn raft(&mut self, id: NodeId) -> &mut Raft {
        &mut self.nodes[id as usize - 1]
    }

    /// Persists at once, then queues the sends (entries from memory).
    fn outbox(&mut self, id: NodeId, mut out: Outbox) {
        let raft = &mut self.nodes[id as usize - 1];
        let last = raft.last_index();
        let mut more = Outbox::default();
        raft.persisted(last, &mut more);
        out.sends.append(&mut more.sends);
        for (to, m) in out.sends.drain(..) {
            let mut buf = vec![0u8; 1 << 16];
            let mut m = m;
            if let Message::Append {
                prev_index,
                ref mut prev_term,
                ..
            } = m
                && prev_index > 0
                && *prev_term == 0
            {
                *prev_term = raft.entry(prev_index).expect("in memory").0;
            }
            let mut at = wire::encode(&mut buf, id, &m);
            if let Message::Append {
                prev_index, count, ..
            } = m
            {
                for i in prev_index + 1..=prev_index + count {
                    let (t, d) = raft.entry(i).expect("in memory");
                    at = wire::push_entry(&mut buf, at, t, d);
                }
            }
            buf.truncate(at);
            self.queue.push((to, buf));
        }
    }

    /// One millisecond step at `now` for the nodes in `running`: each
    /// handles what was sent to it (the others' messages wait), then ticks.
    /// `advance`: whether a node advances its clock before handling them.
    fn step(&mut self, now: u64, running: &[NodeId], advance: bool) {
        let (mine, rest): (Vec<_>, Vec<_>) = self
            .queue
            .drain(..)
            .partition(|(to, _)| running.contains(to));
        self.queue = rest;
        for (to, bytes) in mine {
            if advance {
                self.raft(to).advance_clock(now);
            }
            let (from, m, entries): (NodeId, Message, EntriesView) =
                wire::decode(&bytes, Vec::new()).expect("valid");
            let mut out = Outbox::default();
            self.raft(to).on_message(from, m, &entries, &mut out);
            self.outbox(to, out);
        }
        for &id in running {
            let mut out = Outbox::default();
            self.raft(id).tick(now, &mut out);
            self.outbox(id, out);
        }
    }

    /// Runs everyone until `id` leads; returns the time.
    fn elect(&mut self, id: NodeId) -> u64 {
        let mut now = 0;
        while self.raft(id).role() != Role::Leader {
            now += MS;
            self.step(now, &[1, 2, 3], true);
            assert!(now < 2_000 * MS, "node {id} never led");
        }
        // Settle: everyone has heard the new leader.
        for _ in 0..30 {
            now += MS;
            self.step(now, &[1, 2, 3], true);
        }
        now
    }

    fn roles(&mut self) -> Vec<(Role, u64)> {
        (1..=3)
            .map(|id| (self.raft(id).role(), self.raft(id).term()))
            .collect()
    }
}

/// A follower that stops for 150 ms (longer than any election timeout)
/// while its leader keeps sending must, once it catches up on what the
/// leader sent, carry on as a follower: no pre-vote, no new term.
fn follower_stall(advance: bool) -> (Role, u64, u64) {
    let mut net = Net::new(1, 20, 30); // node 1 leads; node 2 has the defaults
    let mut now = net.elect(1);
    let term = net.raft(1).term();
    for _ in 0..150 {
        now += MS;
        net.step(now, &[1, 3], true); // node 2 is held up
    }
    now += MS;
    net.step(now, &[2], advance); // it catches up, then ticks
    (net.raft(2).role(), net.raft(2).term(), term)
}

#[test]
fn a_held_up_follower_does_not_start_an_election() {
    let (role, term, before) = follower_stall(true);
    assert_eq!((role, term), (Role::Follower, before));
}

/// The test above would fail without advancing the clock: the leader's
/// waiting messages reset the election timer from the stale time.
#[test]
fn without_advancing_the_clock_it_would() {
    let (role, _, _) = follower_stall(false);
    assert_eq!(role, Role::PreCandidate);
}

/// A leader with a 20 ms check-quorum window (krusty-load's leader setting)
/// held up for 40 ms, while its followers (50-100 ms timeouts) answer what
/// it sent before: once it handles their replies it must still lead.
fn leader_stall(advance: bool) -> Vec<(Role, u64)> {
    let mut net = Net::new(1, 20, 30);
    let mut now = net.elect(1);
    for _ in 0..40 {
        now += MS;
        net.step(now, &[2, 3], true); // node 1 is held up
    }
    now += MS;
    net.step(now, &[1], advance);
    net.roles()
}

#[test]
fn a_held_up_leader_does_not_step_down() {
    let roles = leader_stall(true);
    assert_eq!(roles[0].0, Role::Leader, "{roles:?}");
    assert!(roles.iter().all(|r| r.1 == roles[0].1), "{roles:?}");
}

#[test]
fn without_advancing_the_clock_it_would_step_down() {
    let roles = leader_stall(false);
    assert_eq!(roles[0].0, Role::Follower, "{roles:?}");
}
