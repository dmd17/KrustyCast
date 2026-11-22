use super::*;
use krusty_core::{Config as CoreConfig, Core};
use krusty_wire::ingress::Submit;
use std::collections::{HashMap, HashSet};

fn cfg(client_id: u32) -> Config {
    Config {
        client_id,
        window: 4,
        retry_interval_ns: 1_000,
        max_attempts: 1_000,
    }
}

fn fb(client_id: u32, incarnation: u64, client_seqno: u64, outcome: Outcome) -> Feedback {
    Feedback {
        client_id,
        incarnation,
        client_seqno,
        outcome,
    }
}

#[test]
fn window_limits_unanswered_messages() {
    let mut s = Session::new(cfg(1), 10);
    for h in 0..4 {
        assert_eq!(s.submit(Append::Unconditional, h), Ok(h + 1));
    }
    assert_eq!(
        s.submit(Append::Unconditional, 9),
        Err(SubmitError::WindowFull)
    );
    let mut ev = Vec::new();
    // Answering a later one does not open the window: the oldest is still out.
    s.on_feedback(
        &fb(1, 10, 2, Outcome::Accepted { global_seqno: 7 }),
        0,
        &mut ev,
    );
    assert_eq!(
        ev,
        [Event::Accepted {
            client_seqno: 2,
            handle: 1,
            global_seqno: 7
        }]
    );
    assert_eq!(
        s.submit(Append::Unconditional, 9),
        Err(SubmitError::WindowFull)
    );
    s.on_feedback(
        &fb(1, 10, 1, Outcome::Accepted { global_seqno: 6 }),
        0,
        &mut ev,
    );
    assert_eq!(s.submit(Append::Unconditional, 9), Ok(5));
}

#[test]
fn resends_on_timeout_and_from_nak_and_waits_on_busy() {
    let mut s = Session::new(cfg(1), 10);
    let (mut tx, mut ev) = (Vec::new(), Vec::new());
    s.submit(Append::Unconditional, 100).unwrap();
    s.submit(Append::Unconditional, 101).unwrap();
    s.poll(0, &mut tx, &mut ev);
    assert_eq!(
        tx.iter().map(|t| t.client_seqno).collect::<Vec<_>>(),
        [1, 2]
    );
    tx.clear();
    s.poll(500, &mut tx, &mut ev);
    assert!(tx.is_empty()); // not yet due
    s.on_feedback(&fb(1, 10, 2, Outcome::Nak), 600, &mut ev);
    s.poll(600, &mut tx, &mut ev);
    assert_eq!(tx.iter().map(|t| t.client_seqno).collect::<Vec<_>>(), [2]);
    tx.clear();
    s.on_feedback(
        &fb(1, 10, 1, Outcome::Busy { retry_after_us: 5 }),
        700,
        &mut ev,
    );
    s.poll(1_500, &mut tx, &mut ev);
    assert!(tx.is_empty()); // held until 5_700
    s.poll(5_700, &mut tx, &mut ev);
    assert_eq!(
        tx.iter().map(|t| t.client_seqno).collect::<Vec<_>>(),
        [1, 2]
    );
    // Feedback for another incarnation is ignored.
    s.on_feedback(
        &fb(1, 9, 1, Outcome::Accepted { global_seqno: 1 }),
        0,
        &mut ev,
    );
    assert!(ev.is_empty());
}

#[test]
fn fails_explicitly_after_max_attempts_and_on_expiry() {
    let mut s = Session::new(
        Config {
            max_attempts: 2,
            ..cfg(1)
        },
        10,
    );
    let (mut tx, mut ev) = (Vec::new(), Vec::new());
    s.submit(Append::Unconditional, 100).unwrap();
    for t in [0, 1_000, 2_000] {
        s.poll(t, &mut tx, &mut ev);
    }
    assert_eq!(tx.len(), 2);
    assert_eq!(
        ev,
        [Event::Failed {
            client_seqno: 1,
            handle: 100,
            reason: FailReason::NoAnswer
        }]
    );
    ev.clear();
    s.submit(Append::Unconditional, 101).unwrap();
    s.on_feedback(
        &fb(
            1,
            10,
            2,
            Outcome::SessionExpired {
                min_incarnation: 50,
            },
        ),
        0,
        &mut ev,
    );
    assert_eq!(
        ev,
        [Event::Failed {
            client_seqno: 2,
            handle: 101,
            reason: FailReason::SessionExpired
        }]
    );
    assert_eq!(s.must_restart(), Some(50));
    assert_eq!(
        s.submit(Append::Unconditional, 102),
        Err(SubmitError::MustRestart)
    );
    s.restart(50, &mut ev);
    assert_eq!(s.submit(Append::Unconditional, 102), Ok(1));
}

/// Several sessions talk to a real krusty-core over a network that loses,
/// duplicates and reorders in both directions. Every message must end in
/// exactly one event, be published at most once, and (with plenty of
/// attempts and no expiry) be accepted at exactly the stream position its
/// ACCEPTED names.
#[test]
fn sessions_against_the_core_over_a_lossy_network() {
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
    for seed in 0..200u64 {
        let mut rng = Lcg(seed);
        let mut core = Core::new(CoreConfig {
            outcome_ring: 4,
            ..CoreConfig::DEFAULT
        });
        let clients = 3u32;
        let per_client = 30u64;
        let mut sessions: Vec<Session> = (0..clients)
            .map(|c| Session::new(cfg(c), 1000 + c as u64))
            .collect();
        let mut submitted = vec![0u64; clients as usize];
        let mut to_core: Vec<Transmit> = Vec::new();
        let mut to_client: Vec<Feedback> = Vec::new();
        let mut owner: Vec<u32> = Vec::new(); // client of each in-flight Transmit
        let mut published: HashMap<u64, (u32, u64)> = HashMap::new(); // global seqno -> (client, handle)
        let mut finished: HashMap<(u32, u64), Event> = HashMap::new(); // (client, handle) -> event
        let (mut log, mut now) = (0u64, 0u64);
        for _ in 0..200_000 {
            if finished.len() == (clients as u64 * per_client) as usize {
                break;
            }
            now += 100;
            let c = rng.below(clients as u64) as usize;
            // Submit while the window allows.
            while submitted[c] < per_client {
                let h = submitted[c];
                match sessions[c].submit(Append::Unconditional, h) {
                    Ok(_) => submitted[c] += 1,
                    Err(_) => break,
                }
            }
            let (mut tx, mut ev) = (Vec::new(), Vec::new());
            sessions[c].poll(now, &mut tx, &mut ev);
            // (client, event) pairs finished in this step.
            let mut done: Vec<(u32, Event)> = ev.drain(..).map(|e| (c as u32, e)).collect();
            for t in tx {
                to_core.push(t);
                owner.push(c as u32);
            }
            // Network → core: lose 20%, duplicate 10%, any order.
            if !to_core.is_empty() {
                let i = rng.below(to_core.len() as u64) as usize;
                let (t, who) = (to_core.swap_remove(i), owner.swap_remove(i));
                let r = rng.below(10);
                if r >= 2 {
                    if r == 2 {
                        to_core.push(t);
                        owner.push(who);
                    }
                    log += 1;
                    let s = Submit {
                        client_id: who,
                        incarnation: t.incarnation,
                        client_seqno: t.client_seqno,
                        append: t.append,
                        payload: &[],
                    };
                    let e = core.apply(log, now, &s).unwrap();
                    if let Some(p) = e.publish {
                        assert!(published.insert(p.seqno, (who, t.handle)).is_none());
                    }
                    if let Some(f) = e.reply {
                        to_client.push(f);
                    }
                }
            }
            // Network → client: lose 20%, any order.
            if !to_client.is_empty() {
                let f = to_client.swap_remove(rng.below(to_client.len() as u64) as usize);
                if rng.below(10) >= 2 {
                    sessions[f.client_id as usize].on_feedback(&f, now, &mut ev);
                    done.extend(ev.drain(..).map(|e| (f.client_id, e)));
                }
            }
            for (client, e) in done {
                let handle = match e {
                    Event::Accepted { handle, .. }
                    | Event::Rejected { handle, .. }
                    | Event::Failed { handle, .. } => handle,
                };
                assert!(
                    finished.insert((client, handle), e).is_none(),
                    "seed {seed}: two events for one message"
                );
            }
        }
        assert_eq!(
            finished.len() as u64,
            clients as u64 * per_client,
            "seed {seed}: unfinished messages"
        );
        // Exactly once: every message accepted, at the position its event names.
        let mut seen = HashSet::new();
        for ((client, handle), e) in &finished {
            match e {
                Event::Accepted { global_seqno, .. } => {
                    assert_eq!(
                        published.get(global_seqno),
                        Some(&(*client, *handle)),
                        "seed {seed}"
                    );
                    seen.insert(*global_seqno);
                }
                other => panic!("seed {seed}: unexpected {other:?}"),
            }
        }
        assert_eq!(
            seen.len(),
            published.len(),
            "seed {seed}: published something never acknowledged"
        );
    }
}

/// `poll` scans everything in flight only when a resend may be due; that
/// must send exactly what scanning every time (the earlier algorithm, and
/// the reference here) sends. Both sessions get the same submits,
/// outcomes and polls at the same times; their transmissions and events
/// must agree at every step.
#[test]
fn polling_only_when_due_sends_what_scanning_every_time_sends() {
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
        let c = Config {
            client_id: 3,
            window: 1 + rng.below(16) as usize,
            retry_interval_ns: 100 + rng.below(1_000),
            max_attempts: 2 + rng.below(5) as u32,
        };
        let (mut a, mut b) = (Session::new(c, 7), Session::new(c, 7));
        let mut now = 0u64;
        let mut handle = 0u64;
        for step in 0..400 {
            now += rng.below(300);
            match rng.below(6) {
                0 | 1 => {
                    let ra = a.submit(Append::Unconditional, handle);
                    let rb = b.submit(Append::Unconditional, handle);
                    assert_eq!(ra, rb, "seed {seed} step {step}");
                    handle += 1;
                }
                2 | 3 => {
                    // An outcome for some seqno near the window.
                    let s = 1 + rng.below(a.next_seqno.max(1) + 2);
                    let outcome = match rng.below(8) {
                        0 => Outcome::Nak,
                        1 => Outcome::Busy {
                            retry_after_us: rng.below(3),
                        },
                        2 => Outcome::NotLeader { leader: 0 },
                        _ => Outcome::Accepted { global_seqno: s },
                    };
                    let (mut ea, mut eb) = (Vec::new(), Vec::new());
                    a.on_feedback(&fb(3, 7, s, outcome), now, &mut ea);
                    b.on_feedback(&fb(3, 7, s, outcome), now, &mut eb);
                    assert_eq!(ea, eb, "seed {seed} step {step}");
                }
                _ => {
                    let (mut ta, mut tb, mut ea, mut eb) =
                        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
                    a.poll(now, &mut ta, &mut ea);
                    b.next_due_ns = 0; // the reference scans every time
                    b.poll(now, &mut tb, &mut eb);
                    assert_eq!(ta, tb, "seed {seed} step {step}: transmissions");
                    assert_eq!(ea, eb, "seed {seed} step {step}: events");
                }
            }
            assert_eq!(a.in_flight(), b.in_flight());
        }
    }
}
