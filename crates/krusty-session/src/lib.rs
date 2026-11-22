//! Client session core (ARCHITECTURE §4 `krusty-session`, gap K4).
//!
//! Sans-I/O: the application submits messages, the I/O shell feeds in
//! feedback and the time, and the session says what to (re)transmit and
//! which messages are finished. Every submitted message ends in exactly one
//! [`Event`]: accepted, rejected, or failed (gap K3: failure is explicit).
//!
//! Protocol obligations it implements (ADR-12):
//! - At most `window` messages unanswered; the cluster's outcome ring must
//!   be at least that large.
//! - Go back N: on NAK, resend from the expected seqno; on timeout, resend
//!   the oldest unanswered messages; on BUSY, wait the advised delay.
//! - SESSION_EXPIRED: pending messages fail (their fate is unknown), and the
//!   shell must restart with an incarnation of at least `min_incarnation`.
//!
//! Payloads are opaque `u64` handles owned by the shell.
#![forbid(unsafe_code)]
#![allow(clippy::manual_map, clippy::needless_range_loop, clippy::single_match)]

use krusty_wire::feedback::{Feedback, Outcome};
use krusty_wire::ingress::Append;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub client_id: u32,
    /// Most unanswered messages; ≤ the cluster's `outcome_ring`.
    pub window: usize,
    pub retry_interval_ns: u64,
    /// Transmissions of one message before it fails.
    pub max_attempts: u32,
}

/// Something to send to the cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transmit {
    pub incarnation: u64,
    pub client_seqno: u64,
    pub append: Append,
    pub handle: u64,
}

/// A finished message (each submitted message gets exactly one).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Accepted {
        client_seqno: u64,
        handle: u64,
        global_seqno: u64,
    },
    Rejected {
        client_seqno: u64,
        handle: u64,
        key: u64,
        current_version: u64,
    },
    /// Unknown fate: maybe sequenced, maybe not.
    Failed {
        client_seqno: u64,
        handle: u64,
        reason: FailReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailReason {
    /// No decided outcome after `max_attempts` transmissions.
    NoAnswer,
    /// The cluster no longer has this session's dedup state.
    SessionExpired,
    /// The application restarted the session with messages pending.
    Restarted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitError {
    /// The window is full: wait for outcomes.
    WindowFull,
    /// The session must restart (after SESSION_EXPIRED) before new submissions.
    MustRestart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Pending {
    client_seqno: u64,
    append: Append,
    handle: u64,
    /// Transmissions so far (0 = not sent yet).
    attempts: u32,
    last_sent_ns: u64,
}

pub struct Session {
    cfg: Config,
    incarnation: u64,
    /// Seqno the next submission gets.
    next_seqno: u64,
    /// `pending[s % window]` holds unanswered message `s`.
    pending: Vec<Option<Pending>>,
    in_flight: usize,
    /// Do not transmit before this time (BUSY back-off).
    hold_until_ns: u64,
    /// Resend everything from this seqno at the next poll (NAK).
    resend_from: Option<u64>,
    /// SESSION_EXPIRED received: the minimum next incarnation.
    expired: Option<u64>,
    /// The oldest unanswered seqno (`next_seqno` if none), kept as entries
    /// finish: each seqno is passed once, so finding it costs O(1) per
    /// message, not a scan of the window (at a window of 1024 that scan
    /// was what limited a client, BASELINE E3 2026-11-23).
    oldest: u64,
    /// Every seqno below this has been sent at least once.
    unsent_from: u64,
    /// No retry is due before this time: until then `poll` sends only new
    /// messages instead of scanning everything in flight.
    next_due_ns: u64,
}

impl Session {
    pub fn new(cfg: Config, incarnation: u64) -> Session {
        assert!(cfg.window >= 1 && cfg.max_attempts >= 1);
        Session {
            cfg,
            incarnation,
            next_seqno: 1,
            pending: vec![None; cfg.window],
            in_flight: 0,
            hold_until_ns: 0,
            resend_from: None,
            expired: None,
            oldest: 1,
            unsent_from: 1,
            next_due_ns: u64::MAX,
        }
    }

    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Messages submitted and not yet finished.
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// After SESSION_EXPIRED: the smallest incarnation the cluster accepts.
    pub fn must_restart(&self) -> Option<u64> {
        self.expired
    }

    fn index(&self, client_seqno: u64) -> usize {
        (client_seqno % self.cfg.window as u64) as usize
    }

    /// The oldest unanswered seqno, if any.
    fn oldest(&self) -> Option<u64> {
        if self.oldest < self.next_seqno {
            Some(self.oldest)
        } else {
            None
        }
    }

    /// Moves `oldest` past finished messages.
    fn advance_oldest(&mut self) {
        while self.oldest < self.next_seqno {
            let i = self.index(self.oldest);
            match self.pending[i] {
                Some(p) if p.client_seqno == self.oldest => return,
                _ => self.oldest += 1,
            }
        }
    }

    /// Queues a message; returns its client seqno. It goes out at the next `poll`.
    pub fn submit(&mut self, append: Append, handle: u64) -> Result<u64, SubmitError> {
        if self.expired.is_some() {
            return Err(SubmitError::MustRestart);
        }
        // The window counts from the oldest unanswered message, so a resend
        // of it always falls inside the cluster's outcome ring.
        let full = match self.oldest() {
            Some(o) => self.next_seqno - o >= self.cfg.window as u64,
            None => false,
        };
        if full {
            return Err(SubmitError::WindowFull);
        }
        let s = self.next_seqno;
        let i = self.index(s);
        self.pending[i] = Some(Pending {
            client_seqno: s,
            append,
            handle,
            attempts: 0,
            last_sent_ns: 0,
        });
        self.next_seqno += 1;
        self.in_flight += 1;
        Ok(s)
    }

    /// What to transmit at `now_ns`, in seqno order; also fails messages
    /// that ran out of attempts.
    pub fn poll(&mut self, now_ns: u64, tx: &mut Vec<Transmit>, events: &mut Vec<Event>) {
        if self.expired.is_some() || now_ns < self.hold_until_ns {
            return;
        }
        let resend_from = self.resend_from;
        self.resend_from = None;
        let start = match self.oldest() {
            Some(o) => o,
            None => self.next_seqno,
        };
        // Everything in flight is looked at only when a resend may be due;
        // otherwise only what has not been sent yet.
        let full = resend_from.is_some() || now_ns >= self.next_due_ns;
        let mut s = if full || self.unsent_from < start {
            start
        } else {
            self.unsent_from
        };
        let mut earliest = u64::MAX;
        let mut failed = false;
        while s < self.next_seqno {
            let i = self.index(s);
            let cell = self.pending[i];
            match cell {
                Some(p) => {
                    let nak = match resend_from {
                        Some(r) => s >= r,
                        None => false,
                    };
                    let due = p.attempts == 0
                        || nak
                        || now_ns.saturating_sub(p.last_sent_ns) >= self.cfg.retry_interval_ns;
                    if due {
                        if p.attempts >= self.cfg.max_attempts {
                            self.pending[i] = None;
                            self.in_flight -= 1;
                            failed = true;
                            events.push(Event::Failed {
                                client_seqno: s,
                                handle: p.handle,
                                reason: FailReason::NoAnswer,
                            });
                        } else {
                            self.pending[i] = Some(Pending {
                                attempts: p.attempts + 1,
                                last_sent_ns: now_ns,
                                ..p
                            });
                            tx.push(Transmit {
                                incarnation: self.incarnation,
                                client_seqno: s,
                                append: p.append,
                                handle: p.handle,
                            });
                            earliest = earliest.min(now_ns);
                        }
                    } else {
                        earliest = earliest.min(p.last_sent_ns);
                    }
                }
                None => {}
            }
            s += 1;
        }
        self.unsent_from = self.next_seqno;
        let due = earliest.saturating_add(self.cfg.retry_interval_ns);
        // A full scan saw everything in flight; a partial one only adds
        // what it sent now.
        self.next_due_ns = if full { due } else { self.next_due_ns.min(due) };
        if failed {
            self.advance_oldest();
        }
    }

    /// Handles one feedback datagram at `now_ns`.
    pub fn on_feedback(&mut self, f: &Feedback, now_ns: u64, events: &mut Vec<Event>) {
        if f.client_id != self.cfg.client_id || f.incarnation != self.incarnation {
            return; // for another client, or an earlier incarnation
        }
        match f.outcome {
            Outcome::Accepted { global_seqno } => match self.finish(f.client_seqno) {
                Some(handle) => events.push(Event::Accepted {
                    client_seqno: f.client_seqno,
                    handle,
                    global_seqno,
                }),
                None => {}
            },
            Outcome::Rejected {
                key,
                current_version,
            } => match self.finish(f.client_seqno) {
                Some(handle) => events.push(Event::Rejected {
                    client_seqno: f.client_seqno,
                    handle,
                    key,
                    current_version,
                }),
                None => {}
            },
            Outcome::Nak => {
                self.resend_from = Some(match self.resend_from {
                    Some(r) if r < f.client_seqno => r,
                    _ => f.client_seqno,
                });
            }
            Outcome::Busy { retry_after_us } => {
                let until = now_ns.saturating_add(retry_after_us.saturating_mul(1000));
                if until > self.hold_until_ns {
                    self.hold_until_ns = until;
                }
            }
            Outcome::NotLeader { .. } => {
                // The shell switches nodes; resend everything there now.
                self.resend_from = Some(0);
            }
            Outcome::SessionExpired { min_incarnation } => {
                self.expired = Some(min_incarnation);
                self.fail_all(FailReason::SessionExpired, events);
            }
        }
    }

    /// Removes an unanswered message and returns its handle; None if it
    /// was already finished (a duplicate outcome).
    fn finish(&mut self, client_seqno: u64) -> Option<u64> {
        let i = self.index(client_seqno);
        let cell = self.pending[i];
        match cell {
            Some(p) => {
                if p.client_seqno == client_seqno {
                    self.pending[i] = None;
                    self.in_flight -= 1;
                    self.advance_oldest();
                    Some(p.handle)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    fn fail_all(&mut self, reason: FailReason, events: &mut Vec<Event>) {
        let start = match self.oldest() {
            Some(o) => o,
            None => self.next_seqno,
        };
        let mut s = start;
        while s < self.next_seqno {
            let i = self.index(s);
            let cell = self.pending[i];
            if let Some(p) = cell {
                self.pending[i] = None;
                events.push(Event::Failed {
                    client_seqno: s,
                    handle: p.handle,
                    reason,
                });
            }
            s += 1;
        }
        self.in_flight = 0;
        self.oldest = self.next_seqno;
    }

    /// Starts a new incarnation (after a SESSION_EXPIRED, or when the
    /// application chooses to). Anything still pending fails.
    pub fn restart(&mut self, incarnation: u64, events: &mut Vec<Event>) {
        self.fail_all(FailReason::Restarted, events);
        self.incarnation = incarnation;
        self.next_seqno = 1;
        self.oldest = 1;
        self.unsent_from = 1;
        self.next_due_ns = u64::MAX;
        self.hold_until_ns = 0;
        self.resend_from = None;
        self.expired = None;
    }
}

#[cfg(test)]
mod tests;
