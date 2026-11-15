//! Durable storage: restart from snapshot + log, dedup survives, and the
//! whole session can be recovered from disk. Both durability modes.
use krusty_log::LogConfig;
use krusty_node::{Durability, NodeConfig, StorageConfig};
use krusty_wire::feedback::{Feedback, Outcome};
use krusty_wire::ingress::{Append, Submit};
use krusty_wire::stream::{self, Frame};
use std::net::{SocketAddr, SocketAddrV4, UdpSocket};
use std::path::Path;
use std::time::{Duration, Instant};

fn v4(a: SocketAddr) -> SocketAddrV4 {
    match a {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    }
}

fn node_cfg(dir: &Path, feed: SocketAddrV4, durability: Durability) -> NodeConfig {
    let mut cfg = NodeConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        feed,
        "127.0.0.1:0".parse().unwrap(),
    );
    cfg.history = 64; // small, so recovery has to read the disk
    cfg.heartbeat_us = 5_000;
    cfg.storage = Some(StorageConfig {
        log: LogConfig {
            dir: dir.to_path_buf(),
            segment_bytes: 256 << 10,
            index_entries: 1024,
            retain_bytes: u64::MAX,
        },
        durability,
        snapshot_every: 700, // last snapshot at 1400: a restart replays 600 entries
    });
    cfg
}

/// Submits one message and waits for its decided outcome (resending on timeout).
fn submit(client: &UdpSocket, to: SocketAddrV4, inc: u64, seq: u64, payload: &[u8]) -> Outcome {
    let s = Submit {
        client_id: 7,
        incarnation: inc,
        client_seqno: seq,
        append: Append::Unconditional,
        payload,
    };
    let mut buf = [0u8; 2048];
    let n = s.encode(&mut buf).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline, "no answer for seqno {seq}");
        client.send_to(&buf[..n], to).unwrap();
        let mut fb = [0u8; 64];
        while let Ok(k) = client.recv(&mut fb) {
            let f = Feedback::decode(&fb[..k]).unwrap();
            if f.client_seqno == seq
                && matches!(
                    f.outcome,
                    Outcome::Accepted { .. } | Outcome::Rejected { .. }
                )
            {
                return f.outcome;
            }
        }
    }
}

fn payload(i: u64) -> Vec<u8> {
    format!("message {i:06}").into_bytes()
}

/// RECOVERs `from..from+count` and collects (seqno, payload, retransmit).
fn recover(recovery: SocketAddrV4, from: u64, count: u64) -> Vec<(u64, Vec<u8>)> {
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let mut buf = [0u8; 2048];
    let n = stream::encode_recover(&mut buf, 1, from, count).unwrap();
    sock.send_to(&buf[..n], recovery).unwrap();
    let mut got = Vec::new();
    while let Ok(k) = sock.recv(&mut buf) {
        if let Ok(Frame::Data(d)) = Frame::decode(&buf[..k]) {
            assert!(d.retransmit);
            for m in d.messages() {
                got.push((m.seqno, m.payload.to_vec()));
            }
        }
    }
    got
}

/// Zeroes the egress index slots of seqnos above `after`, as a power loss
/// may leave them: group commit syncs only the log segments, and the index
/// is synced fully only with a snapshot. (The layout is krusty-log's: a
/// 64-byte header, then a 16-byte slot per seqno.)
fn lose_index_after(dir: &Path, after: u64, per_file: u64) {
    for e in std::fs::read_dir(dir).unwrap() {
        let path = e.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap().to_owned();
        let Some(first) = name
            .strip_prefix("egress-")
            .and_then(|n| n.strip_suffix(".idx"))
            .map(|n| n.parse::<u64>().unwrap())
        else {
            continue;
        };
        let from = after.max(first - 1) + 1 - first; // first slot to lose
        if from >= per_file {
            continue;
        }
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let zeros = vec![0u8; ((per_file - from) * 16) as usize];
        std::os::unix::fs::FileExt::write_all_at(&f, &zeros, 64 + from * 16).unwrap();
    }
}

fn run(durability: Durability, tag: &str) {
    run_with(durability, tag, false)
}

fn run_with(durability: Durability, tag: &str, lose_index: bool) {
    let dir =
        std::env::temp_dir().join(format!("krusty-node-storage-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let feed = UdpSocket::bind("127.0.0.1:0").unwrap();
    let feed_addr = v4(feed.local_addr().unwrap());
    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();

    let node = krusty_node::start(node_cfg(&dir, feed_addr, durability)).unwrap();
    for i in 1..=2000u64 {
        assert_eq!(
            submit(&client, node.ingress_addr(), 1, i, &payload(i)),
            Outcome::Accepted { global_seqno: i }
        );
    }
    assert!(
        node.stats
            .snapshots
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 2
    );
    node.stop();
    if lose_index {
        // The last snapshot was taken after entry 1400 (= seqno 1400).
        lose_index_after(&dir, 1400, 1024);
    }

    // Restart on the same directory.
    let node = krusty_node::start(node_cfg(&dir, feed_addr, durability)).unwrap();
    // Dedup state survived: a resend (inside the client's window, i.e. the
    // last outcome_ring messages) gets its original answer, not a new seqno.
    assert_eq!(
        submit(&client, node.ingress_addr(), 1, 1950, &payload(1950)),
        Outcome::Accepted { global_seqno: 1950 }
    );
    assert_eq!(
        submit(&client, node.ingress_addr(), 1, 1999, &payload(1999)),
        Outcome::Accepted { global_seqno: 1999 }
    );
    // The sequence continues.
    assert_eq!(
        submit(&client, node.ingress_addr(), 1, 2001, &payload(2001)),
        Outcome::Accepted { global_seqno: 2001 }
    );
    // The whole session is recoverable from disk, byte for byte.
    let mut all = Vec::new();
    let mut from = 1;
    while from <= 2001 {
        let got = recover(node.recovery_addr(), from, 500);
        assert!(!got.is_empty(), "nothing recovered from {from}");
        from = got.last().unwrap().0 + 1;
        all.extend(got);
    }
    assert_eq!(all.len(), 2001);
    for (k, (seqno, p)) in all.iter().enumerate() {
        assert_eq!(*seqno, k as u64 + 1);
        assert_eq!(p, &payload(*seqno));
    }
    assert!(
        node.stats
            .recovered_from_disk
            .load(std::sync::atomic::Ordering::Relaxed)
            > 1800
    );
    node.stop();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn restart_and_disk_recovery_mode_a() {
    run(Durability::Async, "a");
}

#[test]
fn restart_and_disk_recovery_mode_b() {
    run(Durability::Sync, "b");
}

#[test]
fn restart_rebuilds_an_egress_index_lost_after_the_snapshot() {
    run_with(Durability::Sync, "lost-index", true);
}
