//! Model-based testing: replays traces generated from the Quint spec
//! (`../Sequencer.qnt`, module SequencerRace) through the real
//! `krusty_core::Core` and checks that it decides every logged message as
//! the spec does, and publishes exactly the spec's egress stream.
//!
//! Traces: `quint run ../Sequencer.qnt --main SequencerRace --n-traces N
//! --out-itf '../traces/t_{seq}.itf.json'` (see ../REPORT.md). Set
//! `QUINT_TRACES` to another directory to replay other traces.
use krusty_core::{Config, Core};
use krusty_wire::feedback::Outcome;
use krusty_wire::ingress::{Append, Submit};
use serde_json::Value;
use std::collections::HashMap;

/// A logged message, as the spec has it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Msg {
    c: String,
    inc: u64,
    seq: u64,
    mode: String,
    key: String,
    ver: u64,
}

/// A decision: ACCEPTED at a global seqno, or REJECTED at a version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decided {
    Acc(u64),
    Rej(u64),
}

fn int(v: &Value) -> u64 {
    v["#bigint"].as_str().expect("bigint").parse().unwrap()
}

fn msg(v: &Value) -> Msg {
    Msg {
        c: v["c"].as_str().unwrap().into(),
        inc: int(&v["inc"]),
        seq: int(&v["seq"]),
        mode: v["mode"].as_str().unwrap().into(),
        key: v["key"].as_str().unwrap().into(),
        ver: int(&v["ver"]),
    }
}

fn var<'a>(state: &'a Value, name: &str) -> &'a Value {
    let obj = state.as_object().unwrap();
    obj.iter()
        .find(|(k, _)| k.ends_with(&format!("::{name}")))
        .unwrap_or_else(|| panic!("no variable {name}"))
        .1
}

fn client_id(c: &str) -> u32 {
    c.trim_start_matches('c').parse().unwrap()
}

fn key_id(k: &str) -> u64 {
    k.trim_start_matches('k').parse::<u64>().unwrap() + 100
}

/// What the spec says: the log, the egress stream, and every decision.
struct Expected {
    log: Vec<Msg>,
    out: Vec<Msg>,
    decided: HashMap<(String, u64, u64), Decided>,
}

fn expected(trace: &Value) -> Expected {
    let last = trace["states"].as_array().unwrap().last().unwrap();
    let list = |v: &Value| v.as_array().unwrap().iter().map(msg).collect::<Vec<_>>();
    let mut decided = HashMap::new();
    for kv in var(last, "sess")["#map"].as_array().unwrap() {
        let k = kv[0]["#tup"].as_array().unwrap();
        let (c, inc) = (k[0].as_str().unwrap().to_string(), int(&k[1]));
        for r in kv[1]["res"]["#map"].as_array().unwrap() {
            let seq = int(&r[0]);
            let d = match r[1]["tag"].as_str().unwrap() {
                "Acc" => Decided::Acc(int(&r[1]["value"])),
                "Rej" => Decided::Rej(int(&r[1]["value"])),
                _ => continue,
            };
            decided.insert((c.clone(), inc, seq), d);
        }
    }
    Expected {
        log: list(var(last, "log")),
        out: list(var(last, "out")),
        decided,
    }
}

/// Replays `log` through the core: what it published, and what it decided
/// for each message (its first ACCEPTED or REJECTED).
fn replay(log: &[Msg]) -> (Vec<Msg>, HashMap<(String, u64, u64), Decided>) {
    let mut core = Core::new(Config::DEFAULT);
    let mut published = Vec::new();
    let mut decided = HashMap::new();
    for (i, m) in log.iter().enumerate() {
        let append = if m.mode == "c" {
            Append::Conditional {
                key: key_id(&m.key),
                expected_version: m.ver,
            }
        } else {
            Append::Unconditional
        };
        let s = Submit {
            client_id: client_id(&m.c),
            incarnation: m.inc,
            client_seqno: m.seq,
            append,
            payload: b"",
        };
        let e = core.apply(i as u64 + 1, i as u64 + 1, &s).expect("in order");
        if let Some(p) = e.publish {
            assert_eq!(p.seqno, published.len() as u64 + 1, "global seqnos are consecutive");
            assert_eq!(p.meta.client_id, client_id(&m.c));
            let cond = (m.mode == "c").then(|| (key_id(&m.key), m.ver + 1));
            assert_eq!(p.meta.conditional, cond, "publishes the key's next version");
            published.push(m.clone());
        }
        let d = match e.reply.map(|f| f.outcome) {
            Some(Outcome::Accepted { global_seqno }) => Some(Decided::Acc(global_seqno)),
            Some(Outcome::Rejected { current_version, .. }) => Some(Decided::Rej(current_version)),
            _ => None, // NAK for a gap: undecided, as in the spec
        };
        if let Some(d) = d {
            let first = *decided.entry((m.c.clone(), m.inc, m.seq)).or_insert(d);
            assert_eq!(first, d, "a resend got a different answer");
        }
    }
    (published, decided)
}

#[test]
fn core_decides_as_the_spec_does() {
    let dir = std::env::var("QUINT_TRACES").unwrap_or_else(|_| "../traces".into());
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{dir}: {e} (generate traces first: see REPORT.md)"))
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".itf.json"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no traces in {dir}");
    let (mut entries, mut accepted, mut rejected) = (0, 0, 0);
    for p in &paths {
        let trace: Value = serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap();
        let want = expected(&trace);
        let (published, decided) = replay(&want.log);
        assert_eq!(published, want.out, "{}: egress stream differs", p.display());
        assert_eq!(decided, want.decided, "{}: decisions differ", p.display());
        entries += want.log.len();
        accepted += want.decided.values().filter(|d| matches!(d, Decided::Acc(_))).count();
        rejected += want.decided.values().filter(|d| matches!(d, Decided::Rej(_))).count();
    }
    eprintln!(
        "{} traces, {entries} log entries, {accepted} accepted, {rejected} rejected: all as the spec",
        paths.len()
    );
    assert!(accepted > 0 && rejected > 0, "traces exercise both outcomes");
}
