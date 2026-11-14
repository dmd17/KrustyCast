//! Receiver-side MoldUDP64 stream state: in-order delivery, gap detection,
//! re-request scheduling and session handling. Pure logic, no sockets.

use crate::protocol::{MoldKind, MoldPacket, RequestPacket, Session, MAX_MESSAGES_PER_PACKET};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TrackerConfig {
    /// Only accept this session. None = lock onto the first session seen.
    pub session: Option<Session>,
    /// First seqno to deliver. None = start at the first packet seen (late join);
    /// Some(1) = replay the whole session via re-requests.
    pub start_seqno: Option<u64>,
    /// Whether gaps are re-requested. If false, gaps are skipped immediately.
    pub recover: bool,
    /// Give up on a gap once this many out-of-order messages are buffered behind it.
    pub max_buffered: usize,
    /// How long to wait before re-sending the same request.
    pub retry_interval: Duration,
    /// Give up on a gap after this many requests for it.
    pub max_retries: u32,
}

impl Default for TrackerConfig {
    fn default() -> Self {
        TrackerConfig {
            session: None,
            start_seqno: None,
            recover: true,
            max_buffered: 1_000_000,
            retry_interval: Duration::from_millis(100),
            max_retries: 50,
        }
    }
}

/// What a packet meant, for logging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Accepted,
    /// First packet of a session we just locked onto; delivery starts at `start`.
    NewSession { session: Session, start: u64 },
    /// End-of-session marker (first time seen).
    EndOfSession,
    /// Packet from a session we're not following; ignored.
    ForeignSession(Session),
    /// Packet whose seqno range overflows u64 (never produced by `decode`); ignored.
    Invalid,
}

#[derive(Debug)]
pub struct StreamTracker {
    cfg: TrackerConfig,
    session: Option<Session>,
    /// Next seqno to deliver.
    next: Option<u64>,
    /// Every seqno below this is known to exist.
    high_water: u64,
    /// Messages received ahead of `next`.
    buffered: BTreeMap<u64, Vec<u8>>,
    /// Final next-seqno announced by end-of-session.
    ended_at: Option<u64>,
    /// (seqno requested, when, how many times).
    last_request: Option<(u64, Instant, u32)>,
    lost: u64,
}

impl StreamTracker {
    pub fn new(cfg: TrackerConfig) -> Self {
        StreamTracker {
            cfg,
            session: None,
            next: None,
            high_water: 0,
            buffered: BTreeMap::new(),
            ended_at: None,
            last_request: None,
            lost: 0,
        }
    }

    pub fn session(&self) -> Option<Session> {
        self.session
    }

    pub fn next_seqno(&self) -> Option<u64> {
        self.next
    }

    /// Messages given up on (skipped gaps).
    pub fn lost(&self) -> u64 {
        self.lost
    }

    /// True once end-of-session was seen and every message before it delivered.
    pub fn is_complete(&self) -> bool {
        matches!((self.ended_at, self.next), (Some(end), Some(next)) if next >= end)
    }

    /// Processes one downstream packet (live or retransmitted), calling
    /// `deliver(seqno, data)` for each message that is now in order.
    pub fn on_packet(&mut self, p: MoldPacket, mut deliver: impl FnMut(u64, Vec<u8>)) -> Outcome {
        if !p.has_valid_range() {
            return Outcome::Invalid;
        }
        let mut outcome = Outcome::Accepted;
        match self.session {
            None => {
                if self.cfg.session.is_some_and(|s| s != p.session) {
                    return Outcome::ForeignSession(p.session);
                }
                self.session = Some(p.session);
                let start = self.cfg.start_seqno.unwrap_or(p.seqno);
                self.next = Some(start);
                outcome = Outcome::NewSession { session: p.session, start };
            }
            Some(s) if s != p.session => {
                // Follow a new session only after the current one has ended (and isn't pinned).
                if self.cfg.session.is_some() || self.ended_at.is_none() {
                    return Outcome::ForeignSession(p.session);
                }
                self.lost += self.outstanding();
                *self = StreamTracker { lost: self.lost, ..StreamTracker::new(self.cfg.clone()) };
                self.session = Some(p.session);
                self.next = Some(1);
                outcome = Outcome::NewSession { session: p.session, start: 1 };
            }
            _ => {}
        }

        self.high_water = self.high_water.max(p.next_seqno());
        let mut next = self.next.unwrap();
        match p.kind {
            MoldKind::Messages(msgs) => {
                for (i, data) in msgs.into_iter().enumerate() {
                    let s = p.seqno + i as u64;
                    if s == next {
                        deliver(s, data);
                        next += 1;
                        while let Some(d) = self.buffered.remove(&next) {
                            deliver(next, d);
                            next += 1;
                        }
                    } else if s > next {
                        self.buffered.entry(s).or_insert(data);
                    }
                    // s < next: duplicate, drop.
                }
            }
            MoldKind::Heartbeat => {}
            MoldKind::EndOfSession => {
                if self.ended_at.is_none() && outcome == Outcome::Accepted {
                    outcome = Outcome::EndOfSession;
                }
                self.ended_at = Some(p.seqno);
            }
        }
        self.next = Some(next);

        if !self.cfg.recover {
            while self.has_gap() {
                self.skip_gap(&mut deliver);
            }
        } else if self.buffered.len() > self.cfg.max_buffered {
            self.skip_gap(&mut deliver);
        }
        outcome
    }

    /// The re-request to send now, if any. Call after each packet and periodically.
    /// May skip a gap (delivering what follows it) after too many retries.
    pub fn poll_request(&mut self, now: Instant, mut deliver: impl FnMut(u64, Vec<u8>)) -> Option<RequestPacket> {
        if !self.cfg.recover || !self.has_gap() {
            self.last_request = None;
            return None;
        }
        let (session, next) = (self.session?, self.next?);
        let attempts = match self.last_request {
            Some((s, t, n)) if s == next => {
                if now.duration_since(t) < self.cfg.retry_interval {
                    return None;
                }
                if n >= self.cfg.max_retries {
                    self.skip_gap(&mut deliver);
                    self.last_request = None;
                    return self.poll_request(now, deliver);
                }
                n + 1
            }
            _ => 1,
        };
        self.last_request = Some((next, now, attempts));
        let gap_end = self.buffered.keys().next().copied().unwrap_or(self.high_water);
        let count = (gap_end - next).min(MAX_MESSAGES_PER_PACKET as u64) as u16;
        Some(RequestPacket { session, seqno: next, count })
    }

    fn has_gap(&self) -> bool {
        self.next.is_some_and(|n| n < self.high_water)
    }

    fn outstanding(&self) -> u64 {
        self.next.map_or(0, |n| self.high_water.saturating_sub(n))
    }

    /// Abandons the gap at `next` and delivers whatever is buffered after it.
    fn skip_gap(&mut self, deliver: &mut impl FnMut(u64, Vec<u8>)) {
        let Some(mut next) = self.next else { return };
        let resume = self.buffered.keys().next().copied().unwrap_or(self.high_water);
        if resume <= next {
            return;
        }
        self.lost += resume - next;
        next = resume;
        while let Some(d) = self.buffered.remove(&next) {
            deliver(next, d);
            next += 1;
        }
        self.next = Some(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(name: &str) -> Session {
        Session::new(name).unwrap()
    }

    fn msgs(seqno: u64, data: &[&str]) -> MoldPacket {
        MoldPacket {
            session: s("A"),
            seqno,
            kind: MoldKind::Messages(data.iter().map(|d| d.as_bytes().to_vec()).collect()),
        }
    }

    fn hb(seqno: u64) -> MoldPacket {
        MoldPacket { session: s("A"), seqno, kind: MoldKind::Heartbeat }
    }

    fn eos(seqno: u64) -> MoldPacket {
        MoldPacket { session: s("A"), seqno, kind: MoldKind::EndOfSession }
    }

    /// Feeds packets, returning the delivered (seqno, text) pairs.
    fn feed(t: &mut StreamTracker, packets: Vec<MoldPacket>) -> Vec<(u64, String)> {
        let mut out = Vec::new();
        for p in packets {
            t.on_packet(p, |n, d| out.push((n, String::from_utf8(d).unwrap())));
        }
        out
    }

    fn texts(v: &[(u64, String)]) -> Vec<&str> {
        v.iter().map(|(_, s)| s.as_str()).collect()
    }

    #[test]
    fn seqnos_at_the_top_of_the_range() {
        // The last representable range is fine; one past it is ignored, not a panic.
        let top = u64::MAX - 2;
        let mut t = StreamTracker::new(TrackerConfig::default());
        let got = feed(&mut t, vec![msgs(top, &["a", "b"])]);
        assert_eq!(got, [(top, "a".to_string()), (top + 1, "b".to_string())]);
        assert_eq!(t.next_seqno(), Some(u64::MAX));
        assert_eq!(t.on_packet(msgs(u64::MAX, &["c"]), |_, _| panic!()), Outcome::Invalid);
        assert_eq!(t.next_seqno(), Some(u64::MAX));
        assert_eq!(t.on_packet(hb(u64::MAX), |_, _| panic!()), Outcome::Accepted);
    }

    #[test]
    fn in_order_delivery_and_late_join() {
        let mut t = StreamTracker::new(TrackerConfig::default());
        let out = feed(&mut t, vec![msgs(5, &["e", "f"]), msgs(7, &["g"])]);
        assert_eq!(out, vec![(5, "e".into()), (6, "f".into()), (7, "g".into())]);
        assert_eq!(t.next_seqno(), Some(8));
        assert_eq!(t.poll_request(Instant::now(), |_, _| {}), None);
    }

    #[test]
    fn gap_is_requested_then_filled_in_order() {
        let mut t = StreamTracker::new(TrackerConfig::default());
        let now = Instant::now();
        let out = feed(&mut t, vec![msgs(1, &["a"]), msgs(4, &["d"]), msgs(5, &["e"])]);
        assert_eq!(texts(&out), ["a"]);
        let req = t.poll_request(now, |_, _| {}).unwrap();
        assert_eq!((req.seqno, req.count), (2, 2));
        // Rate-limited until the retry interval passes.
        assert_eq!(t.poll_request(now, |_, _| {}), None);
        assert!(t.poll_request(now + Duration::from_millis(150), |_, _| {}).is_some());

        // Partial retransmission: only 2 arrives. Next request asks for 3.
        let out = feed(&mut t, vec![msgs(2, &["b"])]);
        assert_eq!(texts(&out), ["b"]);
        let req = t.poll_request(now, |_, _| {}).unwrap();
        assert_eq!((req.seqno, req.count), (3, 1));

        // Retransmission overlapping already-delivered messages.
        let out = feed(&mut t, vec![msgs(2, &["b", "c", "d"])]);
        assert_eq!(texts(&out), ["c", "d", "e"]);
        assert_eq!(t.poll_request(now, |_, _| {}), None);
        assert_eq!(t.lost(), 0);
    }

    #[test]
    fn heartbeat_reveals_tail_loss() {
        let mut t = StreamTracker::new(TrackerConfig::default());
        feed(&mut t, vec![msgs(1, &["a"]), hb(4)]);
        let req = t.poll_request(Instant::now(), |_, _| {}).unwrap();
        assert_eq!((req.seqno, req.count), (2, 2));
    }

    #[test]
    fn replay_from_start() {
        let cfg = TrackerConfig { start_seqno: Some(1), ..Default::default() };
        let mut t = StreamTracker::new(cfg);
        assert!(feed(&mut t, vec![hb(10)]).is_empty());
        let req = t.poll_request(Instant::now(), |_, _| {}).unwrap();
        assert_eq!((req.session, req.seqno, req.count), (s("A"), 1, 9));
    }

    #[test]
    fn end_of_session() {
        let mut t = StreamTracker::new(TrackerConfig::default());
        let mut outcome = Outcome::Accepted;
        for p in [msgs(1, &["a"]), eos(3)] {
            outcome = t.on_packet(p, |_, _| {});
        }
        assert_eq!(outcome, Outcome::EndOfSession);
        assert!(!t.is_complete()); // seqno 2 still missing
        feed(&mut t, vec![msgs(2, &["b"])]);
        assert!(t.is_complete());
        assert_eq!(t.on_packet(eos(3), |_, _| {}), Outcome::Accepted);
    }

    #[test]
    fn session_switching() {
        let mut t = StreamTracker::new(TrackerConfig::default());
        feed(&mut t, vec![msgs(1, &["a"])]);
        let b = MoldPacket { session: s("B"), seqno: 1, kind: MoldKind::Messages(vec![b"x".to_vec()]) };
        // Session A is still live: B is ignored.
        assert_eq!(t.on_packet(b.clone(), |_, _| {}), Outcome::ForeignSession(s("B")));
        // After A ends, B is followed from seqno 1.
        t.on_packet(eos(2), |_, _| {});
        let mut got = vec![];
        assert_eq!(t.on_packet(b, |n, d| got.push((n, d))), Outcome::NewSession { session: s("B"), start: 1 });
        assert_eq!(got, vec![(1, b"x".to_vec())]);

        // A pinned session never switches.
        let cfg = TrackerConfig { session: Some(s("B")), ..Default::default() };
        let mut t = StreamTracker::new(cfg);
        assert_eq!(t.on_packet(msgs(1, &["a"]), |_, _| {}), Outcome::ForeignSession(s("A")));
        assert_eq!(t.session(), None);
    }

    #[test]
    fn no_recovery_skips_gaps() {
        let cfg = TrackerConfig { recover: false, ..Default::default() };
        let mut t = StreamTracker::new(cfg);
        let out = feed(&mut t, vec![msgs(1, &["a"]), msgs(4, &["d"]), hb(7)]);
        assert_eq!(texts(&out), ["a", "d"]);
        assert_eq!(t.lost(), 4); // 2, 3, 5, 6
        assert_eq!(t.next_seqno(), Some(7));
        assert_eq!(t.poll_request(Instant::now(), |_, _| {}), None);
    }

    #[test]
    fn gives_up_after_max_retries() {
        let cfg = TrackerConfig { max_retries: 2, ..Default::default() };
        let mut t = StreamTracker::new(cfg);
        feed(&mut t, vec![msgs(1, &["a"]), msgs(3, &["c"])]);
        let mut now = Instant::now();
        let mut got = vec![];
        for _ in 0..2 {
            assert!(t.poll_request(now, |_, _| {}).is_some());
            now += Duration::from_millis(150);
        }
        assert_eq!(t.poll_request(now, |n, _| got.push(n)), None);
        assert_eq!(got, vec![3]);
        assert_eq!(t.lost(), 1);
    }

    #[test]
    fn buffer_cap_skips_gap() {
        let cfg = TrackerConfig { max_buffered: 2, ..Default::default() };
        let mut t = StreamTracker::new(cfg);
        let out = feed(&mut t, vec![msgs(1, &["a"]), msgs(3, &["c"]), msgs(4, &["d"]), msgs(5, &["e"])]);
        assert_eq!(texts(&out), ["a", "c", "d", "e"]);
        assert_eq!(t.lost(), 1);
    }
}
