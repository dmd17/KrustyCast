//! Recovery after a possible power loss in mode (a) (spec/RAFT-VERIFICATION.md,
//! "Decision"; spec/tla/RaftKCRecover.tla, rule "last"): the node grants no
//! vote and starts no election until an Append from a leader of its restart
//! term or later leaves it holding that leader's whole log as of sending
//! (`last`).
use krusty_raft::{Config, Entries, HardState, Message, NoEntries, Outbox, Raft, RaftLog, Role};

const MS: u64 = 1_000_000;

struct Es(Vec<(u64, Vec<u8>)>);

impl Entries for Es {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }
    fn term(&self, k: u64) -> u64 {
        self.0[k as usize].0
    }
    fn data(&self, k: u64) -> &[u8] {
        &self.0[k as usize].1
    }
}

/// Node 2 of {1, 2, 3}, restarted in term 3 with an empty log (its
/// unsynced entries lost), recovering.
fn recovering() -> Raft {
    let mut c = Config::new(2, vec![1, 2, 3]);
    c.seed = 2;
    let hs = HardState {
        term: 3,
        voted_for: None,
    };
    let mut r = Raft::new(c, hs, RaftLog::new(0, 0), 0, 0);
    r.recover_after_power_loss();
    assert!(r.recovering());
    r
}

fn granted(out: &Outbox) -> Option<bool> {
    out.sends.iter().find_map(|(_, m)| match m {
        Message::VoteReply { granted, .. } | Message::PreVoteReply { granted, .. } => {
            Some(*granted)
        }
        _ => None,
    })
}

fn append(r: &mut Raft, term: u64, prev: u64, prev_term: u64, es: Es, last: u64) -> Outbox {
    let mut out = Outbox::default();
    let count = es.len();
    r.on_message(
        1,
        Message::Append {
            term,
            prev_index: prev,
            prev_term,
            count,
            commit: 0,
            last,
        },
        &es,
        &mut out,
    );
    out
}

#[test]
fn a_recovering_node_neither_votes_nor_campaigns() {
    let mut r = recovering();
    let mut out = Outbox::default();
    r.on_message(
        1,
        Message::PreVote {
            term: 4,
            last_index: 9,
            last_term: 3,
        },
        &NoEntries,
        &mut out,
    );
    assert_eq!(
        granted(&out),
        Some(false),
        "pre-vote granted while recovering"
    );
    let mut out = Outbox::default();
    r.on_message(
        1,
        Message::Vote {
            term: 4,
            last_index: 9,
            last_term: 3,
        },
        &NoEntries,
        &mut out,
    );
    assert_eq!(granted(&out), Some(false), "vote granted while recovering");
    assert_eq!(r.hard_state().voted_for, None);
    // Election timeouts come and go: no pre-vote, no election.
    for k in 1..=20 {
        let mut out = Outbox::default();
        r.tick(k * 100 * MS, &mut out);
        assert!(
            !out.sends
                .iter()
                .any(|(_, m)| matches!(m, Message::PreVote { .. } | Message::Vote { .. })),
            "campaigned while recovering"
        );
        assert_eq!(r.role(), Role::Follower);
    }
    // Nor at a leader's request.
    let _ = append(&mut r, 4, 0, 0, Es(vec![]), 5);
    let mut out = Outbox::default();
    r.on_message(1, Message::TimeoutNow { term: 4 }, &NoEntries, &mut out);
    assert_eq!(
        r.role(),
        Role::Follower,
        "TimeoutNow started an election while recovering"
    );
    assert!(r.recovering());
}

#[test]
fn recovery_ends_only_once_the_node_holds_the_leaders_log() {
    let mut r = recovering();
    // The leader's log runs to 2; this Append brings index 1 only.
    let _ = append(&mut r, 3, 0, 0, Es(vec![(3, b"a".to_vec())]), 2);
    assert!(
        r.recovering(),
        "left recovery short of the leader's last index"
    );
    // A heartbeat that matches 1 is still short of 2.
    let _ = append(&mut r, 3, 1, 3, Es(vec![]), 2);
    assert!(r.recovering());
    // Index 2: the node now holds everything the leader had.
    let _ = append(&mut r, 3, 1, 3, Es(vec![(3, b"b".to_vec())]), 2);
    assert!(!r.recovering(), "holds the leader's log, still recovering");
    // And votes again (for an up-to-date candidate, in a new term).
    let mut out = Outbox::default();
    r.on_message(
        3,
        Message::Vote {
            term: 4,
            last_index: 2,
            last_term: 3,
        },
        &NoEntries,
        &mut out,
    );
    assert_eq!(granted(&out), Some(true));
}

#[test]
fn an_append_from_an_earlier_term_does_not_end_recovery() {
    let mut r = recovering();
    // A stale leader of term 2 is answered with a rejection (its term is
    // behind) and does not end recovery, however much it sends.
    let _ = append(&mut r, 2, 0, 0, Es(vec![(2, b"x".to_vec())]), 1);
    assert!(r.recovering());
}

#[test]
fn the_operator_can_end_recovery() {
    let mut r = recovering();
    r.end_recovery();
    let mut out = Outbox::default();
    r.on_message(
        1,
        Message::Vote {
            term: 4,
            last_index: 0,
            last_term: 0,
        },
        &NoEntries,
        &mut out,
    );
    assert_eq!(granted(&out), Some(true));
}
