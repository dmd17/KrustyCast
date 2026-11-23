//! A trace of the cluster shell, for tests (`ClusterConfig::trace`): one
//! TLA+ record per line, which spec/tla/TraceCluster.tla replays with
//! Cluster.tla's own actions (spec/tla/check-cluster-traces.sh).
//!
//! Records: `raft` (what the shell sees of Raft changed: term, whether it
//! leads, commit index, and its log from index `from` on), `submit` (a
//! SUBMIT and what the shell did with it), `apply` (a committed entry and
//! the outcome the shell sent for it, if any) and `crash` (the node
//! started again, its core restored to `applied`).

use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

/// What an entry or a SUBMIT carries, as Cluster.tla's messages.
pub(crate) enum Value {
    Noop,
    Msg {
        client_id: u32,
        incarnation: u64,
        seqno: u64,
    },
    /// Not modelled (a stream roll): replaying it fails.
    Other,
}

/// The outcome sent for an applied entry.
pub(crate) enum Sent {
    Nothing,
    Accepted(u64),
    Nak {
        expect: u64,
    },
    /// Not modelled (SESSION_EXPIRED, BUSY): replaying it fails.
    Other,
}

pub(crate) struct Tracer {
    w: BufWriter<File>,
    /// The log as the trace has it: the terms of entries `base + 1..`.
    base: u64,
    terms: Vec<u64>,
    /// Entries up to here are committed and cannot change.
    stable: u64,
    last: Option<(u64, bool, u64)>,
}

fn value(v: &Value) -> String {
    match v {
        Value::Noop => "[c |-> \"noop\", seq |-> 0]".into(),
        Value::Msg {
            client_id,
            incarnation,
            seqno,
        } => format!("[c |-> \"c{client_id}i{incarnation}\", seq |-> {seqno}]"),
        Value::Other => "[c |-> \"other\", seq |-> 0]".into(),
    }
}

fn out(sent: Sent) -> String {
    match sent {
        Sent::Nothing => "[k |-> \"none\"]".to_string(),
        Sent::Accepted(g) => format!("[k |-> \"acc\", g |-> {g}]"),
        Sent::Nak { expect } => format!("[k |-> \"nak\", expect |-> {expect}]"),
        Sent::Other => "[k |-> \"other\"]".to_string(),
    }
}

/// The outcome sent, as the trace has it.
pub(crate) fn sent_of(f: &krusty_wire::feedback::Feedback) -> Sent {
    use krusty_wire::feedback::Outcome;
    match f.outcome {
        Outcome::Accepted { global_seqno } => Sent::Accepted(global_seqno),
        Outcome::Nak => Sent::Nak {
            expect: f.client_seqno,
        },
        _ => Sent::Other,
    }
}

impl Tracer {
    /// `applied`: the index the core starts from (its snapshot's).
    pub fn open(path: &Path, applied: u64) -> io::Result<Tracer> {
        let restart = std::fs::metadata(path).is_ok_and(|m| m.len() > 0);
        let f = OpenOptions::new().create(true).append(true).open(path)?;
        let mut t = Tracer {
            w: BufWriter::new(f),
            base: applied,
            terms: Vec::new(),
            stable: applied,
            last: None,
        };
        if restart || applied > 0 {
            t.line(format!("[k |-> \"crash\", applied |-> {applied}]"));
        }
        Ok(t)
    }

    /// Each record is flushed: a node killed (tests/processes.rs) loses
    /// none of what it did before.
    fn line(&mut self, s: String) {
        let _ = writeln!(self.w, "{s}");
        let _ = self.w.flush();
    }

    fn top(&self) -> u64 {
        self.base + self.terms.len() as u64
    }

    /// The first index from which the log (`last` entries, `term_at`) no
    /// longer holds what the trace has; past the trace's end if it does.
    pub fn first_change(&self, last: u64, term_at: impl Fn(u64) -> Option<u64>) -> u64 {
        let top = self.top();
        let mut i = self.stable.max(self.base) + 1;
        while i <= top {
            let had = self.terms[(i - self.base - 1) as usize];
            if i > last || term_at(i).is_some_and(|t| t != had) {
                return i;
            }
            i += 1;
        }
        top + 1
    }

    /// Records Raft's state if it changed: the log from `from` on is
    /// `entries` (term, value).
    pub fn raft(
        &mut self,
        term: u64,
        lead: bool,
        commit: u64,
        from: u64,
        entries: &[(u64, Value)],
    ) {
        let changed = from <= self.top() || !entries.is_empty();
        if !changed && self.last == Some((term, lead, commit)) {
            return;
        }
        self.terms.truncate((from - 1 - self.base) as usize);
        let mut s = format!(
            "[k |-> \"raft\", term |-> {term}, lead |-> {}, commit |-> {commit}, from |-> {from}, entries |-> <<",
            if lead { "TRUE" } else { "FALSE" }
        );
        for (k, (t, v)) in entries.iter().enumerate() {
            if k > 0 {
                s.push_str(", ");
            }
            let _ = write!(s, "[t |-> {t}, m |-> {}]", value(v));
            self.terms.push(*t);
        }
        s.push_str(">>]");
        self.line(s);
        self.last = Some((term, lead, commit));
        self.stable = self.stable.max(commit.min(self.top()));
    }

    /// A SUBMIT dropped by the resend filter (`filtered`), answered
    /// NOT_LEADER (`notLeader`), or proposed at `i` in `term`.
    pub fn submit(&mut self, m: Value, res: &str, proposed: Option<(u64, u64)>) {
        let i = match proposed {
            Some((i, term)) => {
                debug_assert_eq!(i, self.top() + 1);
                self.terms.push(term);
                i
            }
            None => 0,
        };
        self.line(format!(
            "[k |-> \"submit\", m |-> {}, res |-> \"{res}\", i |-> {i}]",
            value(&m)
        ));
    }

    /// A resend of a decided message, answered from the core (not logged).
    pub fn answered(&mut self, m: Value, sent: Sent) {
        let out = out(sent);
        self.line(format!(
            "[k |-> \"submit\", m |-> {}, res |-> \"answered\", i |-> 0, out |-> {out}]",
            value(&m)
        ));
    }

    pub fn apply(&mut self, i: u64, sent: Sent) {
        let out = out(sent);
        self.line(format!("[k |-> \"apply\", i |-> {i}, out |-> {out}]"));
    }
}
