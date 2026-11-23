//! `Core::decided` is what applying a resend would answer: whenever it
//! returns an outcome for a message, applying that message (a resend) replies
//! with exactly that outcome. A leader answers resends of decided messages
//! with it instead of logging them again (krusty-node's cluster shell).
use krusty_core::{Config, Core};
use krusty_wire::ingress::{Append, Submit};

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

#[test]
fn decided_answers_what_applying_the_resend_answers() {
    let mut answered = 0;
    for seed in 0..300u64 {
        let mut rng = Lcg(seed);
        let mut core = Core::new(Config {
            outcome_ring: 8,
            max_clients: 4,
            ..Config::DEFAULT
        });
        let mut next = [1u64; 3];
        let mut inc = [1u64; 3];
        for i in 1..=400u64 {
            let c = rng.below(3) as usize;
            if rng.below(40) == 0 {
                inc[c] += 1; // a new incarnation starts at 1
                next[c] = 1;
            }
            // In order, a resend of something recent, or a gap.
            let seq = match rng.below(4) {
                0 | 1 => next[c],
                2 => next[c].saturating_sub(1 + rng.below(10)).max(1),
                _ => next[c] + 1 + rng.below(3),
            };
            let append = if rng.below(3) == 0 {
                Append::Conditional {
                    key: rng.below(2),
                    expected_version: rng.below(3),
                }
            } else {
                Append::Unconditional
            };
            let s = Submit {
                client_id: c as u32 + 1,
                incarnation: inc[c],
                client_seqno: seq,
                append,
                payload: b"",
            };
            let d = core.decided(s.client_id, s.incarnation, s.client_seqno);
            let e = core.apply(i, i * 1000, &s).unwrap();
            if let Some(o) = d {
                let reply = e.reply.expect("a decided message is answered");
                assert_eq!(reply.outcome, o, "seed {seed} entry {i}");
                assert_eq!(reply.client_seqno, seq);
                assert!(e.publish.is_none(), "a resend must not be sequenced again");
                answered += 1;
            }
            if seq == next[c] {
                next[c] += 1;
            }
            // Never an outcome for what is not decided yet.
            assert_eq!(core.decided(s.client_id, s.incarnation, next[c] + 1), None);
        }
    }
    assert!(
        answered > 1000,
        "the test must exercise decided resends ({answered})"
    );
}
