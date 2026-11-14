//! Consumer stream tracker for the native stream (ARCHITECTURE §8.3, §8.5).
//!
//! Turns frames that arrive lost, duplicated and reordered into in-order
//! delivery (G2): message *n* is delivered only after 1..*n*−1, unless the
//! tracker reports the gap as lost. Loss is reported when the cluster says a
//! range is UNAVAILABLE (aged out), when recovery is off, or after
//! `max_retries` unanswered RECOVER requests.
//!
//! Sans-I/O: the caller passes frames (already decoded by `krusty-wire`) and
//! the current time in, and gets deliveries and RECOVER requests out.
//! Messages are opaque `u64` handles (e.g. slots of a receive buffer pool).
//!
//! Evolved from the Aeneas spike (`spikes/aeneas-stream/subset`, whose
//! delivery step is proved in Lean): same subset rules (fixed reorder
//! window, no collections, closures or `unsafe`, no early exit from loops).
#![forbid(unsafe_code)]
// Subset style (ARCHITECTURE §11.3): explicit `match` and `while`.
#![allow(
    clippy::manual_map,
    clippy::needless_range_loop,
    clippy::collapsible_match,
    clippy::single_match
)]

/// Reorder window: messages buffered ahead of the next expected seqno.
/// Messages further ahead are dropped and recovered later.
pub const WINDOW: usize = 64;
/// Most messages one RECOVER asks for.
pub const MAX_RECOVER: u64 = 65_535;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `count` messages `seqno..seqno+count`; message `i` has handle `first_handle + i`.
    Data { count: u64, first_handle: u64 },
    /// The next seqno is `seqno`.
    Heartbeat,
    /// The stream ended; its next seqno is `seqno`.
    EndOfStream,
    /// Messages `seqno..earliest_retained` are gone for good.
    Unavailable { earliest_retained: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame {
    pub stream_id: u64,
    pub seqno: u64,
    pub kind: Kind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivered {
    pub seqno: u64,
    pub handle: u64,
}

/// Ask the cluster to resend `count` messages from `from`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recover {
    pub stream_id: u64,
    pub from: u64,
    pub count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Accepted,
    /// First frame of a stream we now follow.
    NewStream,
    /// End of stream (first time seen).
    EndOfStream,
    /// A frame of a stream we are not following; ignored.
    Foreign,
    /// A frame whose seqno range overflows u64; ignored (`krusty-wire`
    /// never decodes one).
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Follow only this stream (None: the first one seen).
    pub pinned: Option<u64>,
    /// Start delivering at this seqno (None: the first frame's seqno).
    pub start_seqno: Option<u64>,
    /// Send RECOVER for gaps; if false, gaps are skipped and counted as lost.
    pub recover: bool,
    pub retry_interval_ns: u64,
    pub max_retries: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Slot {
    seqno: u64,
    handle: u64,
}

pub struct Tracker {
    cfg: Config,
    stream: Option<u64>,
    /// Next seqno to deliver (meaningful once `stream` is Some).
    next: u64,
    /// Every seqno below this is known to exist.
    high_water: u64,
    /// `window[s % WINDOW]` may hold seqno `s` for `next < s < next + WINDOW`.
    window: [Option<Slot>; WINDOW],
    ended_at: Option<u64>,
    lost: u64,
    req_seqno: u64,
    req_at: u64,
    req_attempts: u32,
    has_req: bool,
}

fn slot_index(seqno: u64) -> usize {
    (seqno % (WINDOW as u64)) as usize
}

fn min_u64(a: u64, b: u64) -> u64 {
    if a < b { a } else { b }
}

fn max_u64(a: u64, b: u64) -> u64 {
    if a > b { a } else { b }
}

impl Tracker {
    pub fn new(cfg: Config) -> Tracker {
        Tracker {
            cfg,
            stream: None,
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

    pub fn stream(&self) -> Option<u64> {
        self.stream
    }

    /// Next seqno to deliver, once following a stream.
    pub fn next_seqno(&self) -> Option<u64> {
        match self.stream {
            Some(_) => Some(self.next),
            None => None,
        }
    }

    /// Messages reported lost so far.
    pub fn lost(&self) -> u64 {
        self.lost
    }

    /// End of stream seen and everything before it delivered or lost.
    pub fn is_complete(&self) -> bool {
        match self.ended_at {
            Some(end) => self.next >= end,
            None => false,
        }
    }

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
    /// `next` stays below `u64::MAX` because every seqno seen is (frames
    /// whose range overflows are rejected).
    fn deliver_and_drain(&mut self, handle: u64, out: &mut Vec<Delivered>) {
        out.push(Delivered {
            seqno: self.next,
            handle,
        });
        self.next += 1;
        let mut more = true;
        while more {
            match self.take(self.next) {
                Some(h) => {
                    out.push(Delivered {
                        seqno: self.next,
                        handle: h,
                    });
                    self.next += 1;
                }
                None => more = false,
            }
        }
    }

    fn on_message(&mut self, seqno: u64, handle: u64, out: &mut Vec<Delivered>) {
        if seqno == self.next {
            self.deliver_and_drain(handle, out);
        } else if seqno > self.next && seqno - self.next < WINDOW as u64 {
            let i = slot_index(seqno);
            if self.window[i].is_none() {
                self.window[i] = Some(Slot { seqno, handle });
            }
        }
        // else: duplicate, or beyond the window (recovered later).
    }

    fn reset_window(&mut self) {
        self.window = [None; WINDOW];
    }

    /// Processes one frame, appending deliveries in seqno order to `out`.
    pub fn on_frame(&mut self, f: &Frame, out: &mut Vec<Delivered>) -> Outcome {
        let count = match f.kind {
            Kind::Data { count, .. } => count,
            _ => 0,
        };
        let end = match f.seqno.checked_add(count) {
            Some(e) => e,
            None => return Outcome::Invalid,
        };
        if let Kind::Data {
            count,
            first_handle,
        } = f.kind
            && first_handle.checked_add(count).is_none()
        {
            return Outcome::Invalid;
        }
        if f.seqno == 0 {
            return Outcome::Invalid;
        }

        let mut outcome = Outcome::Accepted;
        match self.stream {
            None => {
                let foreign = match self.cfg.pinned {
                    Some(s) => s != f.stream_id,
                    None => false,
                };
                if foreign {
                    return Outcome::Foreign;
                }
                self.stream = Some(f.stream_id);
                self.next = match self.cfg.start_seqno {
                    Some(s) => s,
                    None => f.seqno,
                };
                outcome = Outcome::NewStream;
            }
            Some(s) => {
                if s != f.stream_id {
                    if self.cfg.pinned.is_some() || self.ended_at.is_none() {
                        return Outcome::Foreign;
                    }
                    // The followed stream ended: switch to the new one from seqno 1.
                    self.lost += self.high_water.saturating_sub(self.next);
                    self.reset_window();
                    self.stream = Some(f.stream_id);
                    self.next = 1;
                    self.high_water = 0;
                    self.ended_at = None;
                    self.has_req = false;
                    outcome = Outcome::NewStream;
                }
            }
        }

        self.high_water = max_u64(self.high_water, end);
        match f.kind {
            Kind::Data {
                count,
                first_handle,
            } => {
                let mut i = 0;
                while i < count {
                    self.on_message(f.seqno + i, first_handle + i, out);
                    i += 1;
                }
            }
            Kind::Heartbeat => {}
            Kind::EndOfStream => {
                if self.ended_at.is_none() && outcome == Outcome::Accepted {
                    outcome = Outcome::EndOfStream;
                }
                self.ended_at = Some(f.seqno);
            }
            Kind::Unavailable { earliest_retained } => {
                // Gone for good: give up on everything before it (G2: reported as lost).
                self.high_water = max_u64(self.high_water, earliest_retained);
                self.skip_until(earliest_retained, out);
            }
        }

        if !self.cfg.recover {
            self.skip_until(self.high_water, out);
        }
        outcome
    }

    fn has_gap(&self) -> bool {
        self.stream.is_some() && self.next < self.high_water
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

    /// Abandons the gap at `next` (but not past `limit`) and delivers
    /// whatever follows it.
    fn skip_gap(&mut self, limit: u64, out: &mut Vec<Delivered>) {
        let resume = min_u64(self.gap_end(), limit);
        if resume > self.next {
            self.lost += resume - self.next;
            self.next = resume;
            match self.take(resume) {
                Some(h) => self.deliver_and_drain(h, out),
                None => {}
            }
        }
    }

    /// Skips gaps until `next` reaches `target` (each step jumps to a
    /// buffered message or to `target`, so this runs at most WINDOW+1 times).
    fn skip_until(&mut self, target: u64, out: &mut Vec<Delivered>) {
        while self.stream.is_some() && self.next < target {
            self.skip_gap(target, out);
        }
        self.has_req = false;
    }

    /// The RECOVER to send at time `now_ns`, if any. Gives up on a gap after
    /// `max_retries` attempts, delivering what follows it.
    pub fn poll_recover(&mut self, now_ns: u64, out: &mut Vec<Delivered>) -> Option<Recover> {
        if !self.cfg.recover || !self.has_gap() {
            self.has_req = false;
            return None;
        }
        if self.has_req && self.req_seqno == self.next {
            if now_ns.saturating_sub(self.req_at) < self.cfg.retry_interval_ns {
                return None;
            }
            if self.req_attempts >= self.cfg.max_retries {
                let hw = self.high_water;
                self.skip_gap(hw, out);
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
        self.req_at = now_ns;
        let missing = self.gap_end() - self.next;
        match self.stream {
            Some(stream_id) => Some(Recover {
                stream_id,
                from: self.next,
                count: min_u64(missing, MAX_RECOVER),
            }),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests;
