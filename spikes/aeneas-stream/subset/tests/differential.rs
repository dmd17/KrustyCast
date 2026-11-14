//! Differential test: stage B (restricted subset) must deliver exactly what
//! stage A (today's src/stream.rs) delivers, packet by packet, on randomized
//! streams with reordering and duplication that stay within the window.

use stream_as_is::protocol::{MoldKind, MoldPacket, Session as SessionA};
use stream_as_is::stream::{StreamTracker, TrackerConfig};
use stream_subset::*;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Packets as (seqno, count), covering 1..=n, at most 4 messages each.
fn packets(rng: &mut Lcg, n: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut s = 1;
    while s <= n {
        let c = (1 + rng.below(4)).min(n - s + 1);
        out.push((s, c));
        s += c;
    }
    // Bounded reordering (each packet moves at most 8 places) plus duplicates,
    // so at most 32 messages are ever missing ahead of one: within WINDOW = 64.
    // Sorting by (8·index + jitter in 0..64) moves each packet at most 8 places.
    let mut keyed: Vec<(u64, (u64, u64))> =
        out.into_iter().enumerate().map(|(i, p)| (i as u64 * 8 + rng.below(64), p)).collect();
    keyed.sort_by_key(|(k, _)| *k);
    let out: Vec<(u64, u64)> = keyed.into_iter().map(|(_, p)| p).collect();
    let mut with_dups = Vec::new();
    for p in out {
        with_dups.push(p);
        if rng.below(5) == 0 {
            with_dups.push(p);
        }
    }
    with_dups
}

#[test]
fn subset_tracker_matches_original() {
    let session_a = SessionA::new("A").unwrap();
    let session_b = Session { bytes: session_a.0 };
    for seed in 0..2000u64 {
        let mut rng = Lcg(seed);
        let n = 1 + rng.below(200);
        let mut a = StreamTracker::new(TrackerConfig { start_seqno: Some(1), ..Default::default() });
        let mut b = new_tracker(Config {
            pinned: None,
            start_seqno: Some(1),
            recover: true,
            retry_interval_ns: 100_000_000,
            max_retries: 50,
        });
        let (mut got_a, mut got_b) = (Vec::new(), Vec::new());
        for (seqno, count) in packets(&mut rng, n) {
            let data = (0..count).map(|i| (seqno + i).to_le_bytes().to_vec()).collect();
            let pa = MoldPacket { session: session_a, seqno, kind: MoldKind::Messages(data) };
            a.on_packet(pa, |s, d| got_a.push((s, u64::from_le_bytes(d.try_into().unwrap()))));
            let pb = Packet { session: session_b, seqno, count, first_handle: seqno, kind: Kind::Messages };
            let mut out = Vec::new();
            b.on_packet(&pb, &mut out);
            got_b.extend(out.iter().map(|d| (d.seqno, d.handle)));
            assert_eq!(got_a, got_b, "seed {seed}: diverged after packet ({seqno}, {count})");
        }
        let all: Vec<(u64, u64)> = (1..=n).map(|s| (s, s)).collect();
        assert_eq!(got_b, all, "seed {seed}: not everything delivered in order");
    }
}
