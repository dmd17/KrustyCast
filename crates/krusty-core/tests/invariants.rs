//! Random workloads against tiny capacities (so eviction, restarts,
//! resends, gaps and a full key table all happen), checking the
//! guarantees the TLA+ spec checks (spec/tla/Sequencer.tla):
//! G4 at-most-once, G5 conditional append, G6 session FIFO, outcomes
//! true and stable, and a gap-free published stream.
use krusty_core::{Config, Core, Effects};
use krusty_wire::feedback::Outcome;
use krusty_wire::ingress::{Append, Submit};
use proptest::prelude::*;
use std::collections::HashMap;

#[derive(Clone, Debug)]
struct Op {
    client: u32,
    /// Index into the client's increasing incarnations.
    inc: usize,
    seq: u64,
    cond: Option<(u64, u64)>,
    advance_ns: u64,
}

fn op() -> impl Strategy<Value = Op> {
    (
        0..4u32,
        0..3usize,
        1..7u64,
        proptest::option::of((0..3u64, 0..4u64)),
        prop_oneof![Just(0u64), 0..3_000_000_000u64],
    )
        .prop_map(|(client, inc, seq, cond, advance_ns)| Op {
            client,
            inc,
            seq,
            cond,
            advance_ns,
        })
}

fn incarnation(client: u32, idx: usize) -> u64 {
    // Increasing per client; deliberately interleaved across clients.
    1 + idx as u64 * 10 + client as u64
}

type Id = (u32, u64, u64);
/// (stream seqno, who submitted it, (key, version reached) if conditional)
type Pub = (u64, Id, Option<(u64, u64)>);

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, ..ProptestConfig::default() })]
    #[test]
    fn guarantees_hold(ops in proptest::collection::vec(op(), 1..120)) {
        let mut core = Core::new(Config { max_clients: 2, outcome_ring: 4, max_keys: 2, evict_idle_ns: 1_000_000_000 });
        let mut now = 0u64;
        let mut published: Vec<Pub> = Vec::new();
        let mut outcomes: HashMap<Id, Outcome> = HashMap::new();
        for (i, o) in ops.iter().enumerate() {
            now += o.advance_ns;
            let id: Id = (o.client, incarnation(o.client, o.inc), o.seq);
            let append = match o.cond {
                Some((key, expected_version)) => Append::Conditional { key, expected_version },
                None => Append::Unconditional,
            };
            let s = Submit { client_id: id.0, incarnation: id.1, client_seqno: id.2, append, payload: b"" };
            let Effects { publish, reply } = core.apply(i as u64 + 1, now, &s).unwrap();
            if let Some(p) = publish {
                // Gap-free stream, timestamps from the leader clock.
                prop_assert_eq!(p.seqno, published.len() as u64 + 1);
                prop_assert_eq!(p.meta.timestamp_ns, now);
                prop_assert_eq!(p.meta.client_id, o.client);
                published.push((p.seqno, id, p.meta.conditional));
            }
            if let Some(f) = reply {
                prop_assert_eq!((f.client_id, f.incarnation), (id.0, id.1));
                match f.outcome {
                    Outcome::Accepted { .. } | Outcome::Rejected { .. } => {
                        prop_assert_eq!(f.client_seqno, id.2);
                        // Stable: a resend never gets a different answer.
                        if let Some(prev) = outcomes.insert(id, f.outcome) {
                            prop_assert_eq!(prev, f.outcome, "{:?}", id);
                        }
                        // True: ACCEPTED(g) names this message.
                        if let Outcome::Accepted { global_seqno } = f.outcome {
                            prop_assert_eq!(published[global_seqno as usize - 1].1, id);
                        }
                    }
                    Outcome::Nak | Outcome::Busy { .. } | Outcome::SessionExpired { .. } => {
                        prop_assert!(publish.is_none());
                    }
                    Outcome::NotLeader { .. } => prop_assert!(false, "core never says NOT_LEADER"),
                }
            }
            if publish.is_some() {
                let accepted = matches!(reply.map(|f| f.outcome), Some(Outcome::Accepted { .. }));
                prop_assert!(accepted);
            }
        }
        // G4: each (client, incarnation, seq) published at most once.
        let mut seen = std::collections::HashSet::new();
        for (_, id, _) in &published {
            prop_assert!(seen.insert(*id), "published twice: {:?}", id);
        }
        // G5: per key, versions reached are 1, 2, 3, ... in stream order.
        let mut versions: HashMap<u64, u64> = HashMap::new();
        for (_, _, c) in &published {
            if let Some((key, v)) = c {
                let e = versions.entry(*key).or_insert(0);
                prop_assert_eq!(*v, *e + 1);
                *e = *v;
            }
        }
        for (key, v) in &versions {
            prop_assert_eq!(core.version(*key), *v);
        }
        // G6: within a session, seqs strictly increase in stream order.
        let mut last: HashMap<(u32, u64), u64> = HashMap::new();
        for (_, (c, inc, seq), _) in &published {
            if let Some(prev) = last.insert((*c, *inc), *seq) {
                prop_assert!(*seq > prev);
            }
        }
    }
}

fn submit(client_id: u32, incarnation: u64, client_seqno: u64) -> Submit<'static> {
    Submit {
        client_id,
        incarnation,
        client_seqno,
        append: Append::Unconditional,
        payload: b"",
    }
}

#[test]
fn eviction_raises_the_floor_and_expires_late_resends() {
    let mut core = Core::new(Config {
        max_clients: 1,
        outcome_ring: 4,
        max_keys: 4,
        evict_idle_ns: 10,
    });
    let a = core.apply(1, 100, &submit(1, 5, 1)).unwrap();
    assert!(a.publish.is_some());
    // Table full and client 1 not idle long enough: the newcomer is BUSY.
    let b = core.apply(2, 105, &submit(2, 7, 1)).unwrap();
    assert_eq!(
        b.reply.unwrap().outcome,
        Outcome::Busy { retry_after_us: 0 }
    );
    // Later, client 1 is idle: evicted, floor = its incarnation (5).
    let c = core.apply(3, 200, &submit(2, 7, 1)).unwrap();
    assert!(c.publish.is_some());
    assert_eq!(core.incarnation_floor(), 5);
    // A late resend from client 1's evicted session must not be sequenced again.
    let d = core.apply(4, 201, &submit(1, 5, 1)).unwrap();
    assert_eq!(d.publish, None);
    assert_eq!(
        d.reply.unwrap().outcome,
        Outcome::SessionExpired { min_incarnation: 6 }
    );
}

#[test]
fn newer_incarnation_retires_the_old_one() {
    let mut core = Core::new(Config::DEFAULT);
    core.apply(1, 1, &submit(1, 10, 1)).unwrap();
    // The new incarnation must start at seqno 1.
    let r = core.apply(2, 2, &submit(1, 11, 2)).unwrap().reply.unwrap();
    assert_eq!((r.outcome, r.client_seqno), (Outcome::Nak, 1));
    assert!(
        core.apply(3, 3, &submit(1, 11, 1))
            .unwrap()
            .publish
            .is_some()
    );
    // The old incarnation is now expired, even for a resend of its seqno 1.
    let r = core.apply(4, 4, &submit(1, 10, 1)).unwrap().reply.unwrap();
    assert_eq!(
        r.outcome,
        Outcome::SessionExpired {
            min_incarnation: 12
        }
    );
}

#[test]
fn entries_must_be_applied_in_order() {
    let mut core = Core::new(Config::DEFAULT);
    assert!(core.apply(2, 0, &submit(1, 1, 1)).is_err());
    assert!(core.apply(1, 0, &submit(1, 1, 1)).is_ok());
    assert!(core.apply(1, 0, &submit(1, 1, 2)).is_err());
}
