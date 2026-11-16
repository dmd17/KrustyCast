use super::*;

fn cfg() -> Config {
    Config {
        pinned: None,
        start_seqno: None,
        recover: true,
        retry_interval_ns: 100,
        gap_policy: GapPolicy::GiveUpAfter(3),
    }
}

fn data(seqno: u64, count: u64) -> Frame {
    Frame {
        stream_id: 7,
        seqno,
        kind: Kind::Data {
            count,
            first_handle: seqno + 1000,
        },
    }
}

fn seqnos(out: &[Delivered]) -> Vec<u64> {
    out.iter().map(|d| d.seqno).collect()
}

#[test]
fn gap_fill_in_order() {
    let mut t = Tracker::new(cfg());
    let mut out = Vec::new();
    assert_eq!(t.on_frame(&data(1, 1), &mut out), Outcome::NewStream);
    t.on_frame(&data(4, 2), &mut out);
    assert_eq!(seqnos(&out), [1]);
    let r = t.poll_recover(0, &mut out).unwrap();
    assert_eq!((r.stream_id, r.from, r.count), (7, 2, 2));
    assert_eq!(t.poll_recover(50, &mut out), None); // rate limited
    t.on_frame(&data(2, 2), &mut out);
    assert_eq!(seqnos(&out), [1, 2, 3, 4, 5]);
    assert_eq!(
        out[3],
        Delivered {
            seqno: 4,
            handle: 1004
        }
    );
    assert_eq!(t.poll_recover(500, &mut out), None);
    assert_eq!(t.lost(), 0);
}

#[test]
fn gives_up_after_max_retries() {
    let mut t = Tracker::new(cfg());
    let mut out = Vec::new();
    t.on_frame(&data(1, 1), &mut out);
    t.on_frame(&data(3, 1), &mut out);
    for k in 0..3 {
        assert!(t.poll_recover(k * 200, &mut out).is_some());
    }
    assert_eq!(t.poll_recover(600, &mut out), None);
    assert_eq!(seqnos(&out), [1, 3]);
    assert_eq!(t.lost(), 1);
}

#[test]
fn waits_forever_under_the_wait_policy() {
    let mut t = Tracker::new(Config {
        gap_policy: GapPolicy::Wait,
        ..cfg()
    });
    let mut out = Vec::new();
    t.on_frame(&data(1, 1), &mut out);
    t.on_frame(&data(3, 1), &mut out);
    for k in 0..1000 {
        let r = t.poll_recover(k * 200, &mut out).expect("keeps asking");
        assert_eq!(r.from, 2);
    }
    assert_eq!(seqnos(&out), [1]);
    assert_eq!(t.lost(), 0);
    t.on_frame(&data(2, 1), &mut out);
    assert_eq!(seqnos(&out), [1, 2, 3]);
}

#[test]
fn unavailable_skips_and_reports_loss_but_keeps_what_arrived() {
    let mut t = Tracker::new(cfg());
    let mut out = Vec::new();
    t.on_frame(&data(1, 1), &mut out);
    t.on_frame(&data(5, 1), &mut out); // 2..4 missing, 5 buffered
    t.on_frame(&data(9, 1), &mut out); // 6..8 missing, 9 buffered
    // The cluster no longer has anything before 8.
    t.on_frame(
        &Frame {
            stream_id: 7,
            seqno: 2,
            kind: Kind::Unavailable {
                earliest_retained: 8,
            },
        },
        &mut out,
    );
    assert_eq!(seqnos(&out), [1, 5]);
    assert_eq!(t.next_seqno(), Some(8));
    assert_eq!(t.lost(), 3 + 2); // 2..4 and 6..7
    // 8 is still recoverable.
    let r = t.poll_recover(0, &mut out).unwrap();
    assert_eq!((r.from, r.count), (8, 1));
    t.on_frame(&data(8, 1), &mut out);
    assert_eq!(seqnos(&out), [1, 5, 8, 9]);
}

#[test]
fn no_recovery_skips() {
    let mut t = Tracker::new(Config {
        recover: false,
        ..cfg()
    });
    let mut out = Vec::new();
    t.on_frame(&data(1, 1), &mut out);
    t.on_frame(&data(4, 1), &mut out);
    t.on_frame(
        &Frame {
            stream_id: 7,
            seqno: 7,
            kind: Kind::Heartbeat,
        },
        &mut out,
    );
    assert_eq!(seqnos(&out), [1, 4]);
    assert_eq!((t.lost(), t.next_seqno()), (4, Some(7)));
}

#[test]
fn foreign_streams_and_overflow_are_ignored() {
    let mut t = Tracker::new(Config {
        pinned: Some(7),
        ..cfg()
    });
    let mut out = Vec::new();
    let other = Frame {
        stream_id: 8,
        ..data(1, 1)
    };
    assert_eq!(t.on_frame(&other, &mut out), Outcome::Foreign);
    let top = Frame {
        stream_id: 7,
        seqno: u64::MAX,
        kind: Kind::Data {
            count: 1,
            first_handle: 0,
        },
    };
    assert_eq!(t.on_frame(&top, &mut out), Outcome::Invalid);
    let zero = Frame {
        stream_id: 7,
        seqno: 0,
        kind: Kind::Heartbeat,
    };
    assert_eq!(t.on_frame(&zero, &mut out), Outcome::Invalid);
    assert!(out.is_empty());
    assert_eq!(t.stream(), None);
}

#[test]
fn end_of_stream_then_next_stream() {
    let mut t = Tracker::new(cfg());
    let mut out = Vec::new();
    t.on_frame(&data(1, 2), &mut out);
    let eos = Frame {
        stream_id: 7,
        seqno: 3,
        kind: Kind::EndOfStream,
    };
    assert_eq!(t.on_frame(&eos, &mut out), Outcome::EndOfStream);
    assert!(t.is_complete());
    let next = Frame {
        stream_id: 9,
        seqno: 1,
        kind: Kind::Data {
            count: 1,
            first_handle: 5,
        },
    };
    assert_eq!(t.on_frame(&next, &mut out), Outcome::NewStream);
    assert_eq!(
        out.last(),
        Some(&Delivered {
            seqno: 1,
            handle: 5
        })
    );
}

/// Randomized: a lossy, duplicating, reordering channel plus a server that
/// answers RECOVER (also lossily) and sometimes declares an old range
/// unavailable. Whatever happens, deliveries are strictly increasing with
/// correct handles, and delivered + lost accounts for every message.
#[test]
fn lossy_channel_with_recovery() {
    struct Lcg(u64);
    impl Lcg {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % n
        }
    }
    for seed in 0..500u64 {
        let mut rng = Lcg(seed);
        let n = 1 + rng.below(300);
        let mut t = Tracker::new(Config {
            start_seqno: Some(1),
            ..cfg()
        });
        let mut out = Vec::new();
        let mut in_flight: Vec<Frame> = Vec::new();
        let mut s = 1;
        while s <= n {
            let c = (1 + rng.below(5)).min(n - s + 1);
            in_flight.push(data(s, c));
            s += c;
        }
        in_flight.push(Frame {
            stream_id: 7,
            seqno: n + 1,
            kind: Kind::EndOfStream,
        });
        let mut now = 0;
        let mut unavailable_from = None;
        for _round in 0..10_000 {
            if t.is_complete() {
                break;
            }
            now += 50;
            // Deliver a random in-flight frame, maybe drop or duplicate it.
            if !in_flight.is_empty() {
                let i = rng.below(in_flight.len() as u64) as usize;
                let f = in_flight.swap_remove(i);
                match rng.below(10) {
                    0..=2 => {} // lost
                    3 => {
                        t.on_frame(&f, &mut out);
                        in_flight.push(f); // duplicated later
                    }
                    _ => {
                        t.on_frame(&f, &mut out);
                    }
                }
            }
            if let Some(r) = t.poll_recover(now, &mut out) {
                // Once in a while the server has aged out the start of the range.
                if unavailable_from.is_none() && rng.below(20) == 0 {
                    let earliest = r.from + 1 + rng.below(r.count);
                    unavailable_from = Some(earliest);
                    in_flight.push(Frame {
                        stream_id: 7,
                        seqno: r.from,
                        kind: Kind::Unavailable {
                            earliest_retained: earliest,
                        },
                    });
                } else {
                    in_flight.push(data(r.from, r.count.min(n + 1 - r.from)));
                }
            }
            // Heartbeats expose tail loss.
            if rng.below(8) == 0 {
                in_flight.push(Frame {
                    stream_id: 7,
                    seqno: n + 1,
                    kind: Kind::EndOfStream,
                });
            }
        }
        assert!(
            t.is_complete(),
            "seed {seed}: stuck at {:?} of {n}",
            t.next_seqno()
        );
        for w in out.windows(2) {
            assert!(w[0].seqno < w[1].seqno, "seed {seed}: out of order");
        }
        for d in &out {
            assert_eq!(d.handle, d.seqno + 1000, "seed {seed}");
        }
        assert_eq!(
            out.len() as u64 + t.lost(),
            n,
            "seed {seed}: delivered + lost != total"
        );
    }
}

#[test]
fn follows_the_current_stream_and_a_new_one_after_its_end() {
    let mut t = Tracker::new(cfg());
    let mut out = Vec::new();
    assert!(t.follows(7) && t.follows(8));
    t.on_frame(&data(1, 1), &mut out); // stream 7
    assert!(t.follows(7) && !t.follows(8));
    t.on_frame(
        &Frame {
            stream_id: 7,
            seqno: 2,
            kind: Kind::EndOfStream,
        },
        &mut out,
    );
    assert!(t.follows(8));
    let pinned = Tracker::new(Config {
        pinned: Some(7),
        ..cfg()
    });
    assert!(pinned.follows(7) && !pinned.follows(8));
}
