//! Raft as a sans-I/O state machine (ADR-3; ARCHITECTURE §5, §8.2).
//!
//! The node's shell feeds [`Raft`] with time ([`Raft::tick`]), peer messages
//! ([`Raft::on_message`]), proposals ([`Raft::propose`]) and storage
//! progress ([`Raft::persisted`]), and carries out what it puts in the
//! [`Outbox`]:
//!
//! 1. persist the hard state (term, vote) and the log changes (truncate,
//!    then store the new entries);
//! 2. only then send the messages (a vote or an append acknowledgement
//!    must never go out before what it promises is stored);
//! 3. apply committed entries, in order, up to `committed`.
//!
//! Features: pre-vote and check-quorum (no disruptive elections), pipelined
//! batched replication with a per-follower window, a no-op entry at the
//! start of each term so the new leader can commit, and fast conflict
//! backtracking. Election timeouts come from a seeded PRNG kept in the
//! state, so a run is deterministic given its inputs.
//!
//! Entries are opaque bytes; an empty entry is the leader's no-op. The core
//! keeps a window of recent entries in memory (buffers are reused, so the
//! steady state does not allocate); older ones are served by the shell from
//! storage.

mod log;
pub mod wire;

pub use log::RaftLog;

pub type NodeId = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    /// All voting members, including `id`.
    pub members: Vec<NodeId>,
    pub election_min_ns: u64,
    pub election_max_ns: u64,
    pub heartbeat_ns: u64,
    /// Most entries per Append.
    pub max_append_entries: u64,
    /// Most entry bytes per Append (the encoded size, see
    /// [`wire::entry_len`]); at least one entry is always sent. Entries
    /// served from storage are not counted: the shell sends fewer if they
    /// do not fit, which the follower's reply reports.
    pub max_append_bytes: usize,
    /// Most unacknowledged Appends per follower. Small (2): the leader
    /// sends an Append on each pass that has new entries, and more in
    /// flight only queued them behind the replication thread's
    /// per-datagram cost (docs/BASELINE.md E3: p50 at 300k msg/s 44 µs
    /// with 2, 146 µs with 16), while entries left behind ride in the
    /// next, larger Append.
    pub max_inflight: u32,
    /// Committed entries kept in memory for followers that lag.
    pub retain_entries: u64,
    pub seed: u64,
}

impl Config {
    pub fn new(id: NodeId, members: Vec<NodeId>) -> Config {
        Config {
            id,
            members,
            election_min_ns: 50_000_000,
            election_max_ns: 100_000_000,
            heartbeat_ns: 10_000_000,
            max_append_entries: 256,
            max_append_bytes: usize::MAX,
            max_inflight: 2,
            retain_entries: 1 << 16,
            seed: 0,
        }
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }
}

/// What must survive a restart besides the log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: u64,
    pub voted_for: Option<NodeId>,
}

/// A message this node sends. `Append` names a range of the leader's log;
/// the shell encodes those entries (see [`wire`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Message {
    PreVote {
        term: u64,
        last_index: u64,
        last_term: u64,
    },
    PreVoteReply {
        term: u64,
        granted: bool,
    },
    Vote {
        term: u64,
        last_index: u64,
        last_term: u64,
    },
    VoteReply {
        term: u64,
        granted: bool,
    },
    /// Entries `prev_index + 1 ..= prev_index + count`. When the leader no
    /// longer has `prev_index` in memory, `prev_term` is 0 and the shell
    /// fills it, and the entries, from storage.
    Append {
        term: u64,
        prev_index: u64,
        prev_term: u64,
        count: u64,
        commit: u64,
    },
    /// `success`: everything up to `index` matches. Otherwise `index` is a
    /// hint: retry with `prev_index = index`.
    AppendReply {
        term: u64,
        success: bool,
        index: u64,
    },
    /// Leadership transfer: the leader, having caught the receiver up,
    /// asks it to start an election now (Raft dissertation §3.10).
    TimeoutNow {
        term: u64,
    },
}

/// A received Append's entries, in order: `(term, data)`.
pub trait Entries {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn term(&self, k: u64) -> u64;
    fn data(&self, k: u64) -> &[u8];
}

#[derive(Debug, Default)]
pub struct Outbox {
    /// Persist before sending anything.
    pub hard_state: Option<HardState>,
    /// Remove log entries from this index on (before storing new ones).
    pub truncate_from: Option<u64>,
    /// Store entries `from..=last_index` (read them with `Raft::entry`).
    pub store_from: Option<u64>,
    pub sends: Vec<(NodeId, Message)>,
    /// Commit index after this step (apply entries up to here).
    pub committed: u64,
}

impl Outbox {
    pub fn clear(&mut self) {
        self.hard_state = None;
        self.truncate_from = None;
        self.store_from = None;
        self.sends.clear();
    }

    fn store(&mut self, from: u64) {
        self.store_from = Some(match self.store_from {
            Some(f) => f.min(from),
            None => from,
        });
    }

    fn truncate(&mut self, from: u64) {
        self.truncate_from = Some(match self.truncate_from {
            Some(f) => f.min(from),
            None => from,
        });
        if let Some(s) = self.store_from
            && s >= from
        {
            self.store_from = None;
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Progress {
    id: NodeId,
    /// Highest index known to match on the follower.
    matched: u64,
    /// Next index to send.
    next: u64,
    inflight: u32,
    /// Time of the last message from this follower (check-quorum).
    heard_at: u64,
    sent_at: u64,
    granted: bool,
}

pub struct Raft {
    cfg: Config,
    role: Role,
    hs: HardState,
    leader: Option<NodeId>,
    log: RaftLog,
    commit: u64,
    /// Highest local index durable (counts toward the leader's quorum).
    durable: u64,
    /// Highest index the shell has applied (entries above stay in memory).
    applied: u64,
    peers: Vec<Progress>,
    now: u64,
    election_deadline: u64,
    /// When this node last heard from a valid leader (pre-vote stickiness).
    leader_heard_at: Option<u64>,
    /// Leader only: handing leadership to this peer, until the deadline.
    transfer: Option<Transfer>,
    rng: u64,
    scratch: Vec<u64>,
    events: Events,
}

/// Counts of this node's elections and step-downs since it started, for
/// metrics: an election under steady load means some node went quiet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Events {
    /// Election timeouts: pre-votes this node started.
    pub pre_votes: u64,
    /// Elections (pre-vote won, or a leadership transfer's TimeoutNow).
    pub elections: u64,
    /// Times this node, as leader, stepped down because a majority of its
    /// peers had gone quiet for an election timeout (check-quorum).
    pub quorum_lost: u64,
}

#[derive(Clone, Copy, Debug)]
struct Transfer {
    to: NodeId,
    deadline: u64,
    sent: bool,
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Raft {
    /// A node starting from `hs` and `log` (what storage holds) at time `now`.
    pub fn new(cfg: Config, hs: HardState, log: RaftLog, commit: u64, now: u64) -> Raft {
        assert!(cfg.members.contains(&cfg.id), "a node must be a member");
        assert!(
            cfg.election_min_ns > cfg.heartbeat_ns && cfg.election_max_ns > cfg.election_min_ns
        );
        let peers = cfg
            .members
            .iter()
            .filter(|&&m| m != cfg.id)
            .map(|&id| Progress {
                id,
                matched: 0,
                next: 1,
                inflight: 0,
                heard_at: now,
                sent_at: 0,
                granted: false,
            })
            .collect();
        let durable = log.last_index();
        let commit = commit.min(durable);
        let rng = cfg.seed ^ (cfg.id as u64).wrapping_mul(0x9E37_79B9);
        let mut r = Raft {
            commit,
            cfg,
            role: Role::Follower,
            hs,
            leader: None,
            log,
            durable,
            applied: commit,
            peers,
            now,
            election_deadline: 0,
            leader_heard_at: None,
            transfer: None,
            rng,
            scratch: Vec::with_capacity(16),
            events: Events::default(),
        };
        r.reset_election_timer();
        r
    }

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn events(&self) -> Events {
        self.events
    }

    pub fn term(&self) -> u64 {
        self.hs.term
    }

    pub fn hard_state(&self) -> HardState {
        self.hs
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn commit_index(&self) -> u64 {
        self.commit
    }

    pub fn last_index(&self) -> u64 {
        self.log.last_index()
    }

    pub fn log(&self) -> &RaftLog {
        &self.log
    }

    /// Entry `i` if it is still in memory: (term, data).
    pub fn entry(&self, i: u64) -> Option<(u64, &[u8])> {
        self.log.get(i)
    }

    /// Term of entry `i`, if known (0 for index 0).
    pub fn term_at(&self, i: u64) -> Option<u64> {
        self.log.term_at(i)
    }

    fn reset_election_timer(&mut self) {
        let span = self.cfg.election_max_ns - self.cfg.election_min_ns;
        self.election_deadline =
            self.now + self.cfg.election_min_ns + splitmix(&mut self.rng) % span;
    }

    fn set_term(&mut self, term: u64, out: &mut Outbox) {
        if term > self.hs.term {
            self.hs = HardState {
                term,
                voted_for: None,
            };
            out.hard_state = Some(self.hs);
        }
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>, out: &mut Outbox) {
        self.set_term(term, out);
        self.role = Role::Follower;
        self.transfer = None;
        self.leader = leader;
        for p in &mut self.peers {
            p.granted = false;
            p.inflight = 0;
        }
    }

    fn up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        let my_term = self.log.last_term();
        last_term > my_term || (last_term == my_term && last_index >= self.log.last_index())
    }

    /// Moves the clock to `now` without acting on it (no elections, no
    /// heartbeats). Call it before handing over messages that may have
    /// waited, so they count as heard now: messages stamped with the time
    /// of the last tick make a peer look silent for as long as this node
    /// was not running, and the next tick then starts an election (or a
    /// leader steps down) although the peers were answering all along.
    pub fn advance_clock(&mut self, now: u64) {
        self.now = now.max(self.now);
    }

    /// Advances time: elections, heartbeats, check-quorum.
    pub fn tick(&mut self, now: u64, out: &mut Outbox) {
        self.now = now.max(self.now);
        match self.role {
            Role::Leader => {
                if self.transfer.is_some_and(|t| self.now >= t.deadline) {
                    // The target did not take over in time: lead on.
                    self.transfer = None;
                }
                // Check-quorum: step down if a majority has gone quiet.
                let heard = 1 + self
                    .peers
                    .iter()
                    .filter(|p| self.now - p.heard_at < self.cfg.election_min_ns)
                    .count();
                if heard < self.cfg.quorum() {
                    self.events.quorum_lost += 1;
                    let term = self.hs.term;
                    self.become_follower(term, None, out);
                    self.reset_election_timer();
                    return;
                }
                self.replicate(out, true);
            }
            _ => {
                if self.now >= self.election_deadline {
                    self.start_pre_vote(out);
                }
            }
        }
        out.committed = self.commit;
    }

    fn start_pre_vote(&mut self, out: &mut Outbox) {
        self.events.pre_votes += 1;
        self.role = Role::PreCandidate;
        self.leader = None;
        for p in &mut self.peers {
            p.granted = false;
        }
        self.reset_election_timer();
        if self.peers.is_empty() {
            self.start_election(out);
            return;
        }
        let m = Message::PreVote {
            term: self.hs.term + 1,
            last_index: self.log.last_index(),
            last_term: self.log.last_term(),
        };
        for p in &self.peers {
            out.sends.push((p.id, m));
        }
    }

    fn start_election(&mut self, out: &mut Outbox) {
        self.events.elections += 1;
        self.role = Role::Candidate;
        self.hs = HardState {
            term: self.hs.term + 1,
            voted_for: Some(self.cfg.id),
        };
        out.hard_state = Some(self.hs);
        for p in &mut self.peers {
            p.granted = false;
        }
        self.reset_election_timer();
        if self.peers.is_empty() {
            self.become_leader(out);
            return;
        }
        let m = Message::Vote {
            term: self.hs.term,
            last_index: self.log.last_index(),
            last_term: self.log.last_term(),
        };
        for p in &self.peers {
            out.sends.push((p.id, m));
        }
    }

    fn granted(&self) -> usize {
        1 + self.peers.iter().filter(|p| p.granted).count()
    }

    fn become_leader(&mut self, out: &mut Outbox) {
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        let next = self.log.last_index() + 1;
        for p in &mut self.peers {
            p.matched = 0;
            p.next = next;
            p.inflight = 0;
            p.heard_at = self.now;
            p.sent_at = 0;
        }
        // A no-op in the new term lets the leader commit earlier entries.
        let i = self.log.append(self.hs.term, &[]);
        out.store(i);
        self.replicate(out, true);
    }

    /// Starts handing leadership to `to` (leader only; false otherwise or
    /// if `to` is not a peer). Until it completes, or an election timeout
    /// passes, the leader accepts no proposals; once `to` has every entry,
    /// it is told to start an election, which it wins with an up-to-date log.
    pub fn transfer_leadership(&mut self, to: NodeId, out: &mut Outbox) -> bool {
        if self.role != Role::Leader || self.peers.iter().all(|p| p.id != to) {
            return false;
        }
        self.transfer = Some(Transfer {
            to,
            deadline: self.now + self.cfg.election_max_ns,
            sent: false,
        });
        self.replicate(out, false);
        self.maybe_timeout_now(out);
        true
    }

    /// The peer leadership is being handed to, if a transfer is under way.
    pub fn transferring(&self) -> Option<NodeId> {
        self.transfer.map(|t| t.to)
    }

    fn maybe_timeout_now(&mut self, out: &mut Outbox) {
        let last = self.log.last_index();
        let now = self.now;
        let hold = self.cfg.election_max_ns;
        if let Some(t) = &mut self.transfer
            && !t.sent
            && self.peers.iter().any(|p| p.id == t.to && p.matched == last)
        {
            t.sent = true;
            // Keep refusing proposals for a full election from now: a
            // TimeoutNow that arrived after we had appended more would start
            // an election the target cannot win.
            t.deadline = now + hold;
            out.sends
                .push((t.to, Message::TimeoutNow { term: self.hs.term }));
        }
    }

    /// Appends `data` as a new entry (leader only, and not while handing
    /// leadership over). Returns its index.
    pub fn propose(&mut self, data: &[u8], out: &mut Outbox) -> Option<u64> {
        if self.role != Role::Leader || self.transfer.is_some() {
            return None;
        }
        let i = self.log.append(self.hs.term, data);
        out.store(i);
        Some(i)
    }

    /// Sends what each follower is due: new entries (up to the window) or,
    /// if `heartbeat`, an empty Append to whoever has not heard from us.
    pub fn replicate(&mut self, out: &mut Outbox, heartbeat: bool) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.log.last_index();
        for k in 0..self.peers.len() {
            let p = self.peers[k];
            let mut next = p.next;
            let mut inflight = p.inflight;
            let mut sent = false;
            while next <= last && inflight < self.cfg.max_inflight {
                let count = self.batch_len(next, last);
                let prev = next - 1;
                let prev_term = self.term_at_or_storage(prev);
                out.sends.push((
                    p.id,
                    Message::Append {
                        term: self.hs.term,
                        prev_index: prev,
                        prev_term,
                        count,
                        commit: self.commit,
                    },
                ));
                next += count;
                inflight += 1;
                sent = true;
            }
            if !sent && heartbeat && self.now.saturating_sub(p.sent_at) >= self.cfg.heartbeat_ns {
                let prev = next - 1;
                let prev_term = self.term_at_or_storage(prev);
                out.sends.push((
                    p.id,
                    Message::Append {
                        term: self.hs.term,
                        prev_index: prev,
                        prev_term,
                        count: 0,
                        commit: self.commit,
                    },
                ));
                sent = true;
            }
            let q = &mut self.peers[k];
            q.next = next;
            q.inflight = inflight;
            if sent {
                q.sent_at = self.now;
            }
        }
    }

    /// How many entries from `next` go in one Append: up to the count and
    /// byte limits (entries in memory only; at least one).
    fn batch_len(&self, next: u64, last: u64) -> u64 {
        let most = (last + 1 - next).min(self.cfg.max_append_entries);
        if self.cfg.max_append_bytes == usize::MAX {
            return most;
        }
        let mut bytes = 0usize;
        let mut n = 0;
        while n < most {
            let Some((_, d)) = self.log.get(next + n) else {
                return most; // from storage: the shell fits them
            };
            bytes += wire::entry_len(d.len());
            if n > 0 && bytes > self.cfg.max_append_bytes {
                break;
            }
            n += 1;
        }
        n
    }

    /// The shell applied entries up to `upto` (they may leave memory).
    pub fn applied(&mut self, upto: u64) {
        self.applied = self.applied.max(upto.min(self.commit));
        self.log
            .compact(self.compact_point(), self.cfg.retain_entries);
    }

    /// The term of `i` from memory, or 0 if it was compacted away: the
    /// shell then fills `prev_term` (and the entries) from storage.
    fn term_at_or_storage(&self, i: u64) -> u64 {
        self.log.term_at(i).unwrap_or(0)
    }

    /// The local log is durable up to `upto` (the shell finished a write).
    pub fn persisted(&mut self, upto: u64, out: &mut Outbox) {
        self.durable = self.durable.max(upto.min(self.log.last_index()));
        if self.role == Role::Leader {
            self.advance_commit();
        }
        out.committed = self.commit;
    }

    fn advance_commit(&mut self) {
        self.scratch.clear();
        self.scratch.push(self.durable);
        self.scratch.extend(self.peers.iter().map(|p| p.matched));
        self.scratch.sort_unstable_by(|a, b| b.cmp(a));
        let n = self.scratch[self.cfg.quorum() - 1];
        if n > self.commit && self.log.term_at(n) == Some(self.hs.term) {
            self.commit = n;
            self.log
                .compact(self.compact_point(), self.cfg.retain_entries);
        }
    }

    /// Entries below this may leave memory: committed, already stored by
    /// the shell (which reads new entries from memory to persist them) and
    /// applied. Followers lagging further are served from storage.
    fn compact_point(&self) -> u64 {
        self.commit.min(self.durable).min(self.applied)
    }

    fn peer(&mut self, id: NodeId) -> Option<&mut Progress> {
        self.peers.iter_mut().find(|p| p.id == id)
    }

    /// Handles a message from `from`. For `Append`, `entries` holds the
    /// entries it carried.
    pub fn on_message(
        &mut self,
        from: NodeId,
        m: Message,
        entries: &dyn Entries,
        out: &mut Outbox,
    ) {
        if !self.cfg.members.contains(&from) || from == self.cfg.id {
            return;
        }
        match m {
            Message::PreVote {
                term,
                last_index,
                last_term,
            } => {
                // Grant only if we have no live leader and the log is up to date;
                // our own term does not change.
                let leader_alive = match self.leader_heard_at {
                    Some(t) => self.now - t < self.cfg.election_min_ns,
                    None => false,
                } || self.role == Role::Leader;
                let granted =
                    term > self.hs.term && !leader_alive && self.up_to_date(last_index, last_term);
                out.sends
                    .push((from, Message::PreVoteReply { term, granted }));
            }
            Message::PreVoteReply { term, granted } => {
                if self.role == Role::PreCandidate && term == self.hs.term + 1 && granted {
                    if let Some(p) = self.peer(from) {
                        p.granted = true;
                    }
                    if self.granted() >= self.cfg.quorum() {
                        self.start_election(out);
                    }
                } else if !granted && term > self.hs.term + 1 {
                    // Someone is ahead of us: catch up to their view of the term.
                }
            }
            Message::Vote {
                term,
                last_index,
                last_term,
            } => {
                if term > self.hs.term {
                    self.become_follower(term, None, out);
                }
                let can_vote = term == self.hs.term
                    && (self.hs.voted_for.is_none() || self.hs.voted_for == Some(from))
                    && self.up_to_date(last_index, last_term);
                if can_vote {
                    self.hs.voted_for = Some(from);
                    out.hard_state = Some(self.hs);
                    self.reset_election_timer();
                }
                out.sends.push((
                    from,
                    Message::VoteReply {
                        term: self.hs.term,
                        granted: can_vote,
                    },
                ));
            }
            Message::VoteReply { term, granted } => {
                if term > self.hs.term {
                    self.become_follower(term, None, out);
                    self.reset_election_timer();
                } else if self.role == Role::Candidate && term == self.hs.term && granted {
                    if let Some(p) = self.peer(from) {
                        p.granted = true;
                    }
                    if self.granted() >= self.cfg.quorum() {
                        self.become_leader(out);
                    }
                }
            }
            Message::Append {
                term,
                prev_index,
                prev_term,
                count,
                commit,
            } => {
                if term < self.hs.term {
                    out.sends.push((
                        from,
                        Message::AppendReply {
                            term: self.hs.term,
                            success: false,
                            index: 0,
                        },
                    ));
                    return;
                }
                if term > self.hs.term || self.role != Role::Follower || self.leader != Some(from) {
                    self.become_follower(term, Some(from), out);
                }
                self.leader_heard_at = Some(self.now);
                self.reset_election_timer();
                debug_assert_eq!(entries.len(), count);
                self.handle_append(from, prev_index, prev_term, commit, entries, out);
            }
            Message::AppendReply {
                term,
                success,
                index,
            } => {
                if term > self.hs.term {
                    self.become_follower(term, None, out);
                    self.reset_election_timer();
                    return;
                }
                if self.role != Role::Leader || term != self.hs.term {
                    return;
                }
                let now = self.now;
                let last = self.log.last_index();
                let Some(p) = self.peer(from) else { return };
                p.heard_at = now;
                if success {
                    p.matched = p.matched.max(index.min(last));
                    p.next = p.next.max(p.matched + 1);
                    p.inflight = p.inflight.saturating_sub(1);
                    self.advance_commit();
                    self.maybe_timeout_now(out);
                } else {
                    // Back up and resend from the hint (probe: one batch at a time).
                    p.next = (index + 1).max(p.matched + 1).min(p.next);
                    p.inflight = 0;
                }
                self.replicate(out, false);
            }
            Message::TimeoutNow { term } => {
                // Only from our current leader, in its term: start an election
                // now, without pre-vote (the leader asked for it).
                if term == self.hs.term && self.role == Role::Follower && self.leader == Some(from)
                {
                    self.start_election(out);
                }
            }
        }
        out.committed = self.commit;
    }

    fn handle_append(
        &mut self,
        from: NodeId,
        prev_index: u64,
        prev_term: u64,
        commit: u64,
        entries: &dyn Entries,
        out: &mut Outbox,
    ) {
        let term = self.hs.term;
        if prev_index > self.log.last_index() {
            let hint = self.log.last_index();
            out.sends.push((
                from,
                Message::AppendReply {
                    term,
                    success: false,
                    index: hint,
                },
            ));
            return;
        }
        // Entries at or below our commit index are final and match by Log
        // Matching; only check later ones.
        if prev_index > self.commit {
            match self.log.term_at(prev_index) {
                Some(t) if t == prev_term => {}
                Some(t) => {
                    // Skip back over the whole conflicting term.
                    let hint = self
                        .log
                        .first_index_of_term_before(prev_index, t)
                        .max(self.commit);
                    out.sends.push((
                        from,
                        Message::AppendReply {
                            term,
                            success: false,
                            index: hint,
                        },
                    ));
                    return;
                }
                None => {
                    out.sends.push((
                        from,
                        Message::AppendReply {
                            term,
                            success: false,
                            index: self.commit,
                        },
                    ));
                    return;
                }
            }
        }
        let n = entries.len();
        let mut k = 0;
        // Skip entries we already have (with the same term).
        while k < n {
            let i = prev_index + 1 + k;
            if i <= self.commit {
                k += 1;
                continue;
            }
            match self.log.term_at(i) {
                Some(t) if t == entries.term(k) => k += 1,
                Some(_) => {
                    assert!(i > self.commit, "a committed entry would be overwritten");
                    self.log.truncate_from(i);
                    self.durable = self.durable.min(i - 1);
                    out.truncate(i);
                    break;
                }
                None => break,
            }
        }
        if k < n {
            let first_new = self.log.last_index() + 1;
            debug_assert_eq!(first_new, prev_index + 1 + k);
            while k < n {
                self.log.append(entries.term(k), entries.data(k));
                k += 1;
            }
            out.store(first_new);
        }
        let matched = prev_index + n;
        if commit > self.commit {
            self.commit = commit.min(matched).max(self.commit);
            self.log
                .compact(self.compact_point(), self.cfg.retain_entries);
        }
        out.sends.push((
            from,
            Message::AppendReply {
                term,
                success: true,
                index: matched,
            },
        ));
    }
}

/// No entries (for messages other than Append, and heartbeats).
pub struct NoEntries;

impl Entries for NoEntries {
    fn len(&self) -> u64 {
        0
    }
    fn term(&self, _: u64) -> u64 {
        unreachable!()
    }
    fn data(&self, _: u64) -> &[u8] {
        unreachable!()
    }
}
