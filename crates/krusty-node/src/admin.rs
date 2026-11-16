//! The admin endpoint (gap O3): a cold, unpinned stage that answers
//! `krusty-admin` over UDP. Requests and replies are short text, one
//! datagram each: a command line in, `key value` lines out (or
//! `error <reason>`), so it can also be driven by hand (`nc -u`).
//!
//! Commands:
//! - `status`: role, term, leader, commit and apply progress, stream,
//!   snapshot, clients and a few counters.
//! - `snapshot`: take a core snapshot now (with storage), and report its
//!   log index once the storage stage has it.
//!
//! The endpoint has no authentication (gap O4): bind it to loopback or to
//! a management network.
use crate::metrics::Metrics;
use crate::{Control, NodeConfig};
use std::fmt::Write;
use std::net::{SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// How long `snapshot` waits for the core stage before replying.
const SNAPSHOT_WAIT: Duration = Duration::from_secs(5);

pub(crate) fn admin_stage(
    cfg: NodeConfig,
    sock: UdpSocket,
    stats: Arc<Metrics>,
    control: Arc<Control>,
    stop: Arc<AtomicBool>,
) {
    let _ = sock.set_read_timeout(Some(Duration::from_millis(100)));
    let mut buf = [0u8; 512];
    while !stop.load(Ordering::Acquire) {
        let Ok((n, from)) = sock.recv_from(&mut buf) else {
            continue;
        };
        let req = String::from_utf8_lossy(&buf[..n]);
        let reply = answer(&cfg, &stats, &control, &stop, req.trim());
        let _ = sock.send_to(reply.as_bytes(), from);
    }
}

fn answer(
    cfg: &NodeConfig,
    stats: &Metrics,
    control: &Control,
    stop: &AtomicBool,
    req: &str,
) -> String {
    let mut words = req.split_whitespace();
    match (words.next(), words.next()) {
        (Some("status"), None) => status(cfg, stats),
        (Some("snapshot"), None) => snapshot(cfg, stats, control, stop),
        (Some(cmd), _) => format!("error unknown command {cmd:?}\n"),
        (None, _) => "error empty request\n".into(),
    }
}

fn status(cfg: &NodeConfig, s: &Metrics) -> String {
    let g = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
    let role = match (&cfg.cluster, g(&s.raft_role)) {
        (None, _) => "single",
        (Some(_), 0) => "follower",
        (Some(_), 1) => "pre-candidate",
        (Some(_), 2) => "candidate",
        (Some(_), _) => "leader",
    };
    let durability = match &cfg.storage {
        None => "memory",
        Some(st) if st.durability == crate::Durability::Sync => "b",
        Some(_) => "a",
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let uptime_s = now.saturating_sub(g(&s.started_unix_ns)) / 1_000_000_000;
    let mut out = String::new();
    let mut kv = |k: &str, v: &dyn std::fmt::Display| {
        let _ = writeln!(out, "{k} {v}");
    };
    kv("node", &g(&s.node_id));
    kv("role", &role);
    kv("term", &g(&s.raft_term));
    kv("leader", &g(&s.raft_leader));
    kv("commit", &g(&s.raft_commit));
    kv("applied", &g(&s.applied));
    kv("stream", &g(&s.stream_id));
    kv("next_seqno", &g(&s.stream_next));
    kv("durability", &durability);
    kv("snapshot_index", &g(&s.snapshot_index));
    kv("snapshots", &g(&s.snapshots));
    kv("clients", &g(&s.clients));
    kv("submits", &g(&s.submits));
    kv("published", &g(&s.published));
    kv("busy", &g(&s.busy));
    kv("not_leader", &g(&s.not_leader));
    kv("pid", &g(&s.pid));
    kv("uptime_s", &uptime_s);
    out
}

fn snapshot(cfg: &NodeConfig, s: &Metrics, control: &Control, stop: &AtomicBool) -> String {
    if cfg.storage.is_none() {
        return "error no storage: snapshots need [storage]\n".into();
    }
    let want = control.snapshot.fetch_add(1, Ordering::AcqRel) + 1;
    let deadline = Instant::now() + SNAPSHOT_WAIT;
    while control.snapshot_done.load(Ordering::Acquire) < want {
        if Instant::now() > deadline || stop.load(Ordering::Acquire) {
            return "error snapshot not taken yet (the storage stage is still writing the previous one)\n"
                .into();
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    format!(
        "ok\nsnapshot_index {}\n",
        s.snapshot_index.load(Ordering::Relaxed)
    )
}

/// Sends one admin command to `node` and returns the reply's `key value`
/// lines, or the error it reports (or that it did not answer).
pub fn request(
    node: SocketAddrV4,
    cmd: &str,
    timeout: Duration,
) -> Result<Vec<(String, String)>, String> {
    let sock = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    sock.set_read_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    sock.send_to(cmd.as_bytes(), node)
        .map_err(|e| e.to_string())?;
    let mut buf = [0u8; 4096];
    let n = sock.recv(&mut buf).map_err(|_| "no answer".to_string())?;
    let text = String::from_utf8_lossy(&buf[..n]).into_owned();
    if let Some(e) = text.strip_prefix("error ") {
        return Err(e.trim().to_string());
    }
    Ok(text
        .lines()
        .filter_map(|l| {
            let (k, v) = l.split_once(' ').unwrap_or((l, ""));
            (!k.is_empty()).then(|| (k.to_string(), v.to_string()))
        })
        .collect())
}
