//! The sequencer state machine (ARCHITECTURE §5; ADR-4, ADR-12).
//!
//! [`Core::apply`] consumes one committed log entry and returns what it
//! causes: at most one message published to the stream and at most one
//! outcome for the submitting client. It is deterministic: the same state
//! and entry give the same new state and effects on every node. It does no
//! I/O, reads no clock (time comes from the leader's timestamp on each
//! entry), and allocates only in [`Core::new`].
//!
//! Sessions (ADR-12):
//! - One session per `client_id`: the latest incarnation. A client's
//!   incarnations must increase (the SDK uses its start time in ns), so a
//!   newer incarnation retires the older one.
//! - The client table is bounded. When it is full, the least recently
//!   active client that has been idle for at least
//!   [`Config::evict_idle_ns`] is evicted, and the global incarnation floor
//!   rises to its incarnation. Anything at or below the floor from a client
//!   without a session is answered `SESSION_EXPIRED`, so evicting state
//!   never lets a resend be sequenced twice.
//! - Each session remembers the outcomes of its last [`Config::outcome_ring`]
//!   messages. Clients keep at most that many messages unanswered.
#![forbid(unsafe_code)]
// Written in the subset Aeneas translates (ARCHITECTURE §11.3): explicit
// `match` and `while` instead of closures and iterator adaptors.
#![allow(
    clippy::manual_map,
    clippy::manual_unwrap_or,
    clippy::needless_range_loop,
    clippy::while_let_loop
)]

mod restore;
mod snapshot;
mod table;

pub use restore::{Corruption, SnapshotError};

use krusty_wire::feedback::{Feedback, Outcome};
use krusty_wire::ingress::{Append, Submit};
use krusty_wire::stream::MessageMeta;
use table::IdMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Most clients with dedup state.
    pub max_clients: usize,
    /// Outcomes remembered per session (the client's window). ≥ 1.
    pub outcome_ring: usize,
    /// Most keys with a version.
    pub max_keys: usize,
    /// A full client table evicts only clients idle at least this long
    /// (leader time).
    pub evict_idle_ns: u64,
}

impl Config {
    pub const DEFAULT: Config = Config {
        max_clients: 4096,
        outcome_ring: 64,
        max_keys: 1 << 16,
        evict_idle_ns: 60_000_000_000,
    };
}

/// A decided outcome, as remembered for resends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decided {
    Accepted { global_seqno: u64 },
    Rejected { key: u64, current_version: u64 },
}

impl Decided {
    fn outcome(self) -> Outcome {
        match self {
            Decided::Accepted { global_seqno } => Outcome::Accepted { global_seqno },
            Decided::Rejected {
                key,
                current_version,
            } => Outcome::Rejected {
                key,
                current_version,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RingSlot {
    client_seqno: u64,
    decided: Decided,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Session {
    client_id: u32,
    incarnation: u64,
    /// Next client seqno to decide.
    next: u64,
    /// Leader time of the last decided message.
    last_active_ns: u64,
}

/// The parts of a submission the core looks at (not the payload). Plain
/// values, so the verified code holds no borrows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Request {
    pub client_id: u32,
    pub incarnation: u64,
    pub client_seqno: u64,
    pub append: Append,
}

impl Request {
    pub fn of(s: &Submit<'_>) -> Request {
        Request {
            client_id: s.client_id,
            incarnation: s.incarnation,
            client_seqno: s.client_seqno,
            append: s.append,
        }
    }
}

/// What one committed entry causes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Effects {
    /// Publish the entry's payload as this stream message.
    pub publish: Option<Published>,
    /// Send this to the submitting client (leader only).
    pub reply: Option<Feedback>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Published {
    pub seqno: u64,
    pub meta: MessageMeta,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyError {
    /// Entries must be applied in log order without gaps.
    OutOfOrder { expected: u64, got: u64 },
}

pub struct Core {
    cfg: Config,
    /// Last applied log index (0 = none).
    applied: u64,
    /// Seqno of the next published message.
    next_seqno: u64,
    /// Leader time, made monotonic: max of all entry timestamps so far.
    now_ns: u64,
    /// Incarnations at or below this, from clients without a session, are expired.
    incarnation_floor: u64,

    clients: IdMap,
    sessions: Vec<Option<Session>>,
    /// `outcome_ring` slots per session, indexed by `client_seqno % outcome_ring`.
    ring: Vec<Option<RingSlot>>,
    /// Session slots `0..unused` have been handed out; later ones never
    /// have. (A slot is only ever reused by evicting its session.)
    unused: usize,

    keys: IdMap,
    versions: Vec<u64>,
    /// The key at each `versions` slot (for snapshots).
    key_of_slot: Vec<u64>,
}

impl Core {
    pub fn new(cfg: Config) -> Core {
        assert!(cfg.max_clients >= 1 && cfg.max_clients <= u32::MAX as usize);
        assert!(cfg.outcome_ring >= 1);
        assert!(cfg.max_keys <= u32::MAX as usize);
        Core {
            cfg,
            applied: 0,
            next_seqno: 1,
            now_ns: 0,
            incarnation_floor: 0,
            clients: IdMap::new(cfg.max_clients),
            sessions: vec![None; cfg.max_clients],
            ring: vec![None; cfg.max_clients * cfg.outcome_ring],
            unused: 0,
            keys: IdMap::new(cfg.max_keys),
            versions: Vec::with_capacity(cfg.max_keys),
            key_of_slot: Vec::with_capacity(cfg.max_keys),
        }
    }

    pub fn applied(&self) -> u64 {
        self.applied
    }

    pub fn next_seqno(&self) -> u64 {
        self.next_seqno
    }

    pub fn incarnation_floor(&self) -> u64 {
        self.incarnation_floor
    }

    pub fn clients(&self) -> usize {
        self.clients.len()
    }

    /// Current version of `key` (0 if never appended to).
    pub fn version(&self, key: u64) -> u64 {
        match self.keys.get(key) {
            Some(i) => self.versions[i as usize],
            None => 0,
        }
    }

    /// Applies committed entry `log_index` (the next one), submitted by a
    /// client and timestamped by the leader.
    pub fn apply(
        &mut self,
        log_index: u64,
        timestamp_ns: u64,
        s: &Submit<'_>,
    ) -> Result<Effects, ApplyError> {
        if self.applied == u64::MAX || log_index != self.applied + 1 {
            return Err(ApplyError::OutOfOrder {
                expected: self.applied.wrapping_add(1),
                got: log_index,
            });
        }
        self.applied = log_index;
        if timestamp_ns > self.now_ns {
            self.now_ns = timestamp_ns;
        }
        Ok(self.decide(Request::of(s)))
    }

    fn reply(s: Request, client_seqno: u64, outcome: Outcome) -> Effects {
        Effects {
            publish: None,
            reply: Some(Feedback {
                client_id: s.client_id,
                incarnation: s.incarnation,
                client_seqno,
                outcome,
            }),
        }
    }

    fn busy(s: Request) -> Effects {
        Core::reply(s, s.client_seqno, Outcome::Busy { retry_after_us: 0 })
    }

    fn decide(&mut self, s: Request) -> Effects {
        let found = self.clients.get(s.client_id as u64);
        match found {
            Some(slot) => {
                let sess = self.sessions[slot as usize].expect("mapped slot holds a session");
                if s.incarnation < sess.incarnation {
                    // A previous incarnation of this client: it is retired.
                    Core::reply(
                        s,
                        s.client_seqno,
                        Outcome::SessionExpired {
                            min_incarnation: sess.incarnation.saturating_add(1),
                        },
                    )
                } else if s.incarnation == sess.incarnation {
                    self.in_session(slot, s)
                } else if s.client_seqno == 1 {
                    // A newer incarnation starts: it replaces the old session.
                    self.start_session(slot, s);
                    self.in_session(slot, s)
                } else {
                    // A newer incarnation whose first message is missing.
                    Core::reply(s, 1, Outcome::Nak)
                }
            }
            None => {
                if s.incarnation <= self.incarnation_floor {
                    let min = self.incarnation_floor.saturating_add(1);
                    Core::reply(
                        s,
                        s.client_seqno,
                        Outcome::SessionExpired {
                            min_incarnation: min,
                        },
                    )
                } else if s.client_seqno != 1 {
                    Core::reply(s, 1, Outcome::Nak)
                } else {
                    match self.allocate_session() {
                        Some(slot) => {
                            self.clients.insert(s.client_id as u64, slot);
                            self.start_session(slot, s);
                            self.in_session(slot, s)
                        }
                        None => Core::busy(s),
                    }
                }
            }
        }
    }

    /// A free session slot, evicting the least recently active idle client
    /// if the table is full.
    fn allocate_session(&mut self) -> Option<u32> {
        if self.unused < self.sessions.len() {
            self.unused += 1;
            return Some((self.unused - 1) as u32);
        }
        let mut victim: Option<(u32, Session)> = None;
        let mut i = 0;
        while i < self.sessions.len() {
            let cell = self.sessions[i];
            if let Some(sess) = cell {
                let idle =
                    self.now_ns.saturating_sub(sess.last_active_ns) >= self.cfg.evict_idle_ns;
                let older = match victim {
                    Some((_, v)) => sess.last_active_ns < v.last_active_ns,
                    None => true,
                };
                if idle && older {
                    victim = Some((i as u32, sess));
                }
            }
            i += 1;
        }
        match victim {
            Some((slot, sess)) => {
                self.clients.remove(sess.client_id as u64);
                if sess.incarnation > self.incarnation_floor {
                    self.incarnation_floor = sess.incarnation;
                }
                self.sessions[slot as usize] = None;
                Some(slot)
            }
            None => None,
        }
    }

    fn start_session(&mut self, slot: u32, s: Request) {
        self.sessions[slot as usize] = Some(Session {
            client_id: s.client_id,
            incarnation: s.incarnation,
            next: 1,
            last_active_ns: self.now_ns,
        });
        let n = self.cfg.outcome_ring;
        let base = slot as usize * n;
        let mut i = 0;
        while i < n {
            self.ring[base + i] = None;
            i += 1;
        }
    }

    fn ring_index(&self, slot: u32, client_seqno: u64) -> usize {
        slot as usize * self.cfg.outcome_ring
            + (client_seqno % self.cfg.outcome_ring as u64) as usize
    }

    fn in_session(&mut self, slot: u32, s: Request) -> Effects {
        let sess = self.sessions[slot as usize].expect("session");
        if s.client_seqno < sess.next {
            // A resend of something decided: repeat its outcome if remembered.
            // A client that respects its window never asks for anything older.
            let cell = self.ring[self.ring_index(slot, s.client_seqno)];
            return match cell {
                Some(r) => {
                    if r.client_seqno == s.client_seqno {
                        Core::reply(s, s.client_seqno, r.decided.outcome())
                    } else {
                        Effects {
                            publish: None,
                            reply: None,
                        }
                    }
                }
                None => Effects {
                    publish: None,
                    reply: None,
                },
            };
        }
        if s.client_seqno > sess.next {
            return Core::reply(s, sess.next, Outcome::Nak);
        }
        if s.client_seqno == u64::MAX {
            // The session cannot go further: the client must restart.
            let min = s.incarnation.saturating_add(1);
            return Core::reply(
                s,
                s.client_seqno,
                Outcome::SessionExpired {
                    min_incarnation: min,
                },
            );
        }
        if self.next_seqno == u64::MAX {
            return Core::busy(s); // stream exhausted; cannot happen in practice
        }

        let (decided, publish) = self.decide_append(s);
        match decided {
            None => Core::busy(s),
            Some(d) => {
                let i = self.ring_index(slot, s.client_seqno);
                self.ring[i] = Some(RingSlot {
                    client_seqno: s.client_seqno,
                    decided: d,
                });
                self.sessions[slot as usize] = Some(Session {
                    next: sess.next + 1,
                    last_active_ns: self.now_ns,
                    ..sess
                });
                let published = match publish {
                    Some(meta) => {
                        let p = Published {
                            seqno: self.next_seqno,
                            meta,
                        };
                        self.next_seqno += 1;
                        Some(p)
                    }
                    None => None,
                };
                let mut e = Core::reply(s, s.client_seqno, d.outcome());
                e.publish = published;
                e
            }
        }
    }

    /// Decides the next message of a session (updating the key version if
    /// a conditional append wins), and what to publish. `None`: undecided
    /// because the key table is full.
    fn decide_append(&mut self, s: Request) -> (Option<Decided>, Option<MessageMeta>) {
        match s.append {
            Append::Unconditional => {
                let meta = MessageMeta {
                    timestamp_ns: self.now_ns,
                    client_id: s.client_id,
                    conditional: None,
                };
                (
                    Some(Decided::Accepted {
                        global_seqno: self.next_seqno,
                    }),
                    Some(meta),
                )
            }
            Append::Conditional {
                key,
                expected_version,
            } => {
                let current = self.version(key);
                if current != expected_version || current == u64::MAX {
                    (
                        Some(Decided::Rejected {
                            key,
                            current_version: current,
                        }),
                        None,
                    )
                } else if self.set_version(key, current + 1) {
                    let meta = MessageMeta {
                        timestamp_ns: self.now_ns,
                        client_id: s.client_id,
                        conditional: Some((key, current + 1)),
                    };
                    (
                        Some(Decided::Accepted {
                            global_seqno: self.next_seqno,
                        }),
                        Some(meta),
                    )
                } else {
                    (None, None)
                }
            }
        }
    }

    /// Sets `key` to `version`; false if the key is new and the table is full.
    fn set_version(&mut self, key: u64, version: u64) -> bool {
        match self.keys.get(key) {
            Some(i) => {
                self.versions[i as usize] = version;
                true
            }
            None => {
                if self.keys.is_full() {
                    false
                } else {
                    let i = self.versions.len() as u32;
                    self.versions.push(version);
                    self.key_of_slot.push(key);
                    self.keys.insert(key, i)
                }
            }
        }
    }
}
