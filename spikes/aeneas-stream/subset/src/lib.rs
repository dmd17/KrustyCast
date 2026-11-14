//! Stage B of the Aeneas spike: the consumer stream tracker from
//! `src/stream.rs`, rewritten in the restricted subset proposed by
//! ARCHITECTURE.md §11.3.
//!
//! Differences from the original, all deliberate:
//! - Fixed-capacity reorder window (`WINDOW` slots) instead of `BTreeMap`.
//!   Messages too far ahead of the window are dropped and recovered later.
//! - Time is a `u64` (nanoseconds) passed in; no `Instant`/`Duration`.
//! - Messages are opaque `u64` handles (e.g. slots in a preallocated pool),
//!   not owned byte vectors.
//! - Deliveries are appended to an output `Vec`, not passed to a closure.
//! - No `std::collections`, trait objects, closures, `async` or `unsafe`.
//!   Loops never `return` or `break` out of a nested loop.
#![forbid(unsafe_code)]

/// Reorder window size (messages buffered ahead of the next expected seqno).
pub const WINDOW: usize = 64;
/// Most messages one re-request asks for (MoldUDP64: 0xFFFF is end-of-session).
pub const MAX_REQUEST: u64 = 65534;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Session {
    pub bytes: [u8; 10],
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    /// `count` messages with seqnos `seqno..seqno+count`; message `i` has
    /// handle `first_handle + i`.
    Messages,
    Heartbeat,
    EndOfSession,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Packet {
    pub session: Session,
    /// Seqno of the first message, or the next seqno for heartbeat/end-of-session.
    pub seqno: u64,
    pub count: u64,
    pub first_handle: u64,
    pub kind: Kind,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Delivered {
    pub seqno: u64,
    pub handle: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Request {
    pub session: Session,
    pub seqno: u64,
    pub count: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    Accepted,
    NewSession,
    EndOfSession,
    ForeignSession,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Config {
    pub pinned: Option<Session>,
    pub start_seqno: Option<u64>,
    pub recover: bool,
    pub retry_interval_ns: u64,
    pub max_retries: u32,
}

/// One slot of the reorder window: the seqno it holds and that message's handle.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Slot {
    pub seqno: u64,
    pub handle: u64,
}

pub struct Tracker {
    pub cfg: Config,
    pub session: Option<Session>,
    /// Next seqno to deliver (meaningful once `session` is Some).
    pub next: u64,
    /// Every seqno below this is known to exist.
    pub high_water: u64,
    /// `window[s % WINDOW]` may hold seqno `s` for `next < s < next + WINDOW`.
    pub window: [Option<Slot>; WINDOW],
    pub ended_at: Option<u64>,
    pub lost: u64,
    /// Last request: (seqno, time sent, attempts).
    pub req_seqno: u64,
    pub req_at: u64,
    pub req_attempts: u32,
    pub has_req: bool,
}

pub fn new_tracker(cfg: Config) -> Tracker {
    Tracker {
        cfg,
        session: None,
        next: 0,
        high_water: 0,
        window: [None; WINDOW],
        ended_at: None,
        lost: 0,
        req_seqno: 0,
        req_at: 0,
        req_attempts: 0,
        has_req: false,
    }
}

fn slot_index(seqno: u64) -> usize {
    (seqno % (WINDOW as u64)) as usize
}

fn max_u64(a: u64, b: u64) -> u64 {
    if a > b { a } else { b }
}

impl Tracker {
    /// Takes the buffered message for `seqno`, if the window holds it.
    fn take(&mut self, seqno: u64) -> Option<u64> {
        let i = slot_index(seqno);
        let cell = self.window[i];
        match cell {
            Some(slot) => {
                if slot.seqno == seqno {
                    self.window[i] = None;
                    Some(slot.handle)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Delivers `next` and everything contiguous after it that is buffered.
    fn deliver_and_drain(&mut self, handle: u64, out: &mut Vec<Delivered>) {
        out.push(Delivered { seqno: self.next, handle });
        self.next += 1;
        let mut more = true;
        while more {
            match self.take(self.next) {
                Some(h) => {
                    out.push(Delivered { seqno: self.next, handle: h });
                    self.next += 1;
                }
                None => more = false,
            }
        }
    }

    /// Handles one message with `seqno`/`handle` from a packet.
    fn on_message(&mut self, seqno: u64, handle: u64, out: &mut Vec<Delivered>) {
        if seqno == self.next {
            self.deliver_and_drain(handle, out);
        } else if seqno > self.next && seqno - self.next < WINDOW as u64 {
            let i = slot_index(seqno);
            if self.window[i].is_none() {
                self.window[i] = Some(Slot { seqno, handle });
            }
        }
        // else: duplicate (below next) or beyond the window (recovered later).
    }

    fn clear_window(&mut self) {
        // Not a loop of `self.window[i] = None`: Aeneas (0d21a9ec) translates
        // that assignment inside a loop as `update i ()`, which is ill-typed.
        self.window = [None; WINDOW];
    }

    /// Processes one packet, appending deliveries in seqno order to `out`.
    pub fn on_packet(&mut self, p: &Packet, out: &mut Vec<Delivered>) -> Outcome {
        let mut outcome = Outcome::Accepted;
        match self.session {
            None => {
                let foreign = match self.cfg.pinned {
                    Some(s) => s != p.session,
                    None => false,
                };
                if foreign {
                    return Outcome::ForeignSession;
                }
                self.session = Some(p.session);
                self.next = match self.cfg.start_seqno {
                    Some(s) => s,
                    None => p.seqno,
                };
                outcome = Outcome::NewSession;
            }
            Some(s) => {
                if s != p.session {
                    if self.cfg.pinned.is_some() || self.ended_at.is_none() {
                        return Outcome::ForeignSession;
                    }
                    // The followed session ended: switch to the new one from seqno 1.
                    self.lost += self.high_water.saturating_sub(self.next);
                    self.clear_window();
                    self.session = Some(p.session);
                    self.next = 1;
                    self.high_water = 0;
                    self.ended_at = None;
                    self.has_req = false;
                    outcome = Outcome::NewSession;
                }
            }
        }

        let count = match p.kind {
            Kind::Messages => p.count,
            _ => 0,
        };
        self.high_water = max_u64(self.high_water, p.seqno + count);

        let mut i = 0;
        while i < count {
            self.on_message(p.seqno + i, p.first_handle + i, out);
            i += 1;
        }

        if p.kind == Kind::EndOfSession {
            if self.ended_at.is_none() && outcome == Outcome::Accepted {
                outcome = Outcome::EndOfSession;
            }
            self.ended_at = Some(p.seqno);
        }

        if !self.cfg.recover {
            self.skip_all_gaps(out);
        }
        outcome
    }

    fn has_gap(&self) -> bool {
        self.session.is_some() && self.next < self.high_water
    }

    /// First buffered seqno after `next`, or `high_water` if none.
    fn gap_end(&self) -> u64 {
        let mut end = self.high_water;
        let mut i = 0;
        while i < WINDOW {
            let cell = self.window[i];
            match cell {
                Some(slot) => {
                    if slot.seqno > self.next && slot.seqno < end {
                        end = slot.seqno;
                    }
                }
                None => {}
            }
            i += 1;
        }
        end
    }

    /// Abandons the gap at `next` and delivers whatever follows it.
    fn skip_gap(&mut self, out: &mut Vec<Delivered>) {
        let resume = self.gap_end();
        if resume > self.next {
            self.lost += resume - self.next;
            self.next = resume;
            match self.take(resume) {
                Some(h) => self.deliver_and_drain(h, out),
                None => {}
            }
        }
    }

    fn skip_all_gaps(&mut self, out: &mut Vec<Delivered>) {
        // Each skip strictly advances `next` toward `high_water`.
        while self.has_gap() {
            self.skip_gap(out);
        }
    }

    /// The re-request to send at time `now`, if any. Gives up on a gap after
    /// `max_retries` attempts, delivering what follows it.
    pub fn poll_request(&mut self, now: u64, out: &mut Vec<Delivered>) -> Option<Request> {
        if !self.cfg.recover || !self.has_gap() {
            self.has_req = false;
            return None;
        }
        if self.has_req && self.req_seqno == self.next {
            if now - self.req_at < self.cfg.retry_interval_ns {
                return None;
            }
            if self.req_attempts >= self.cfg.max_retries {
                self.skip_gap(out);
                self.has_req = false;
                if !self.has_gap() {
                    return None;
                }
                self.req_attempts = 0;
            }
            self.req_attempts += 1;
        } else {
            self.req_attempts = 1;
        }
        self.has_req = true;
        self.req_seqno = self.next;
        self.req_at = now;
        let missing = self.gap_end() - self.next;
        let count = if missing > MAX_REQUEST { MAX_REQUEST } else { missing };
        match self.session {
            Some(session) => Some(Request { session, seqno: self.next, count }),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sess(c: u8) -> Session {
        Session { bytes: [c; 10] }
    }

    fn cfg() -> Config {
        Config { pinned: None, start_seqno: None, recover: true, retry_interval_ns: 100, max_retries: 3 }
    }

    fn msgs(seqno: u64, count: u64) -> Packet {
        Packet { session: sess(b'A'), seqno, count, first_handle: seqno * 10, kind: Kind::Messages }
    }

    fn seqnos(out: &[Delivered]) -> Vec<u64> {
        out.iter().map(|d| d.seqno).collect()
    }

    #[test]
    fn gap_fill_in_order() {
        let mut t = new_tracker(cfg());
        let mut out = Vec::new();
        t.on_packet(&msgs(1, 1), &mut out);
        t.on_packet(&msgs(4, 2), &mut out);
        assert_eq!(seqnos(&out), [1]);
        let r = t.poll_request(0, &mut out).unwrap();
        assert_eq!((r.seqno, r.count), (2, 2));
        assert_eq!(t.poll_request(50, &mut out), None); // rate limited
        t.on_packet(&msgs(2, 2), &mut out);
        assert_eq!(seqnos(&out), [1, 2, 3, 4, 5]);
        assert_eq!(out[3], Delivered { seqno: 4, handle: 40 });
        assert_eq!(t.poll_request(500, &mut out), None);
    }

    #[test]
    fn gives_up_after_max_retries() {
        let mut t = new_tracker(cfg());
        let mut out = Vec::new();
        t.on_packet(&msgs(1, 1), &mut out);
        t.on_packet(&msgs(3, 1), &mut out);
        for k in 0..3 {
            assert!(t.poll_request(k * 200, &mut out).is_some());
        }
        assert_eq!(t.poll_request(600, &mut out), None);
        assert_eq!(seqnos(&out), [1, 3]);
        assert_eq!(t.lost, 1);
    }

    #[test]
    fn no_recovery_skips() {
        let mut t = new_tracker(Config { recover: false, ..cfg() });
        let mut out = Vec::new();
        t.on_packet(&msgs(1, 1), &mut out);
        t.on_packet(&msgs(4, 1), &mut out);
        t.on_packet(&Packet { kind: Kind::Heartbeat, count: 0, ..msgs(7, 0) }, &mut out);
        assert_eq!(seqnos(&out), [1, 4]);
        assert_eq!((t.lost, t.next), (4, 7));
    }

    #[test]
    fn beyond_window_is_dropped_then_recovered() {
        let mut t = new_tracker(cfg());
        let mut out = Vec::new();
        t.on_packet(&msgs(1, 1), &mut out);
        let far = 2 + WINDOW as u64;
        t.on_packet(&msgs(far, 1), &mut out); // outside the window: dropped
        let r = t.poll_request(0, &mut out).unwrap();
        assert_eq!((r.seqno, r.count), (2, WINDOW as u64 + 1));
        t.on_packet(&msgs(2, WINDOW as u64 + 1), &mut out);
        assert_eq!(*seqnos(&out).last().unwrap(), far);
        assert_eq!(t.lost, 0);
    }
}
