//! A 3-node cluster over loopback: replication, leader failover, a node
//! rejoining from its log, and the stream staying exactly-once throughout.
use krusty_client::{
    Consumer, ConsumerConfig, ConsumerEvent, Publisher, PublisherConfig, PublisherEvent,
};
use krusty_node::{ClusterConfig, Durability, Node, NodeConfig, StorageConfig};
use krusty_wire::ingress::Append;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

mod common {
    pub mod ports;
}
use common::ports::Ports;

/// One cluster at a time: three nodes already outnumber this machine's
/// spare cores.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct Cluster {
    _serial: std::sync::MutexGuard<'static, ()>,
    cfgs: Vec<NodeConfig>,
    nodes: Vec<Option<Node>>,
    dirs: Vec<PathBuf>,
    feed: SocketAddrV4,
}

impl Cluster {
    fn new(n: u32, tag: &str, durability: Durability) -> Cluster {
        Cluster::with(n, tag, durability, |_| {})
    }

    /// As `new`, with `adjust` applied to every node's config.
    fn with(
        n: u32,
        tag: &str,
        durability: Durability,
        adjust: impl Fn(&mut NodeConfig),
    ) -> Cluster {
        let serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let mut ports = Ports::new(1 + 3 * n as usize);
        let feed = ports.take();
        let repl: Vec<(u32, SocketAddrV4)> = (1..=n).map(|id| (id, ports.take())).collect();
        let mut cfgs = Vec::new();
        let mut dirs = Vec::new();
        for id in 1..=n {
            let dir = std::env::temp_dir()
                .join(format!("krusty-cluster-{tag}-{}-{id}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let mut cfg = NodeConfig::new(ports.take(), feed, ports.take());
            cfg.heartbeat_us = 5_000;
            cfg.admin = Some("127.0.0.1:0".parse().unwrap());
            cfg.storage = Some(StorageConfig {
                log: krusty_log::LogConfig::new(&dir),
                durability,
                snapshot_every: 500,
                core: None,
            });
            cfg.cluster = Some(ClusterConfig::new(id, repl.clone()));
            adjust(&mut cfg);
            cfgs.push(cfg);
            dirs.push(dir);
        }
        drop(ports);
        let mut c = Cluster {
            _serial: serial,
            cfgs,
            nodes: Vec::new(),
            dirs,
            feed,
        };
        for k in 0..n as usize {
            let node = krusty_node::start(c.cfgs[k].clone()).unwrap();
            c.nodes.push(Some(node));
        }
        c
    }

    fn ingress(&self) -> Vec<SocketAddrV4> {
        self.cfgs.iter().map(|c| c.ingress).collect()
    }

    fn recovery(&self) -> Vec<SocketAddrV4> {
        self.cfgs.iter().map(|c| c.recovery).collect()
    }

    fn leader(&self) -> Option<usize> {
        let leaders: Vec<usize> = (0..self.nodes.len())
            .filter(|&k| {
                self.nodes[k]
                    .as_ref()
                    .is_some_and(|n| n.stats.raft_role.load(Ordering::Relaxed) == 3)
            })
            .collect();
        // Two nodes may briefly both think they lead (different terms).
        (leaders.len() == 1).then(|| leaders[0])
    }

    fn wait_leader(&self, within: Duration) -> usize {
        let deadline = Instant::now() + within;
        loop {
            if let Some(k) = self.leader() {
                return k;
            }
            assert!(Instant::now() < deadline, "no leader");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn stop(&mut self, k: usize) {
        self.nodes[k].take().unwrap().stop();
    }

    fn restart(&mut self, k: usize) {
        self.nodes[k] = Some(krusty_node::start(self.cfgs[k].clone()).unwrap());
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in self.nodes.drain(..).flatten() {
            n.stop();
        }
        for d in &self.dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}

fn payload(i: u64) -> Vec<u8> {
    format!("message {i}").into_bytes()
}

/// Submits payloads `from..to` and waits for all outcomes; calls `tick`
/// between polls. Returns client seqno → global seqno and when the first
/// ACCEPTED arrived.
fn publish(
    p: &mut Publisher,
    from: u64,
    to: u64,
    mut tick: impl FnMut(),
) -> (HashMap<u64, u64>, Instant) {
    let mut accepted = HashMap::new();
    let mut first: Option<Instant> = None;
    let mut next = from;
    let deadline = Instant::now() + Duration::from_secs(30);
    while (accepted.len() as u64) < to - from {
        assert!(
            Instant::now() < deadline,
            "publisher stuck at {}",
            accepted.len()
        );
        while next < to {
            match p.submit(Append::Unconditional, &payload(next)) {
                Ok(_) => next += 1,
                Err(_) => break,
            }
        }
        p.poll(|e| match e {
            PublisherEvent::Accepted {
                client_seqno,
                global_seqno,
            } => {
                first.get_or_insert_with(Instant::now);
                assert!(accepted.insert(client_seqno, global_seqno).is_none());
            }
            other => panic!("unexpected {other:?}"),
        })
        .unwrap();
        tick();
    }
    (accepted, first.unwrap())
}

#[test]
fn failover_and_rejoin_keep_the_stream_exactly_once() {
    failover_and_rejoin(Durability::Async, "failover-a");
}

/// Mode (b): followers acknowledge only fsynced entries; the same story.
#[test]
fn failover_and_rejoin_in_mode_b() {
    failover_and_rejoin(Durability::Sync, "failover-b");
}

fn failover_and_rejoin(durability: Durability, tag: &str) {
    let mut c = Cluster::new(3, tag, durability);
    let first_leader = c.wait_leader(Duration::from_secs(5));
    let mut consumer = Consumer::new(ConsumerConfig {
        start_seqno: Some(1),
        ..ConsumerConfig::new(c.feed, c.recovery())
    })
    .unwrap();
    let mut got: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut lost = 0;
    let mut drain = |consumer: &mut Consumer, got: &mut Vec<(u64, Vec<u8>)>| {
        consumer
            .poll(|e| match e {
                ConsumerEvent::Message(d) => got.push((d.seqno, d.payload.to_vec())),
                ConsumerEvent::Lost { count } => lost += count,
                ConsumerEvent::EndOfStream => panic!("the stream must go on"),
            })
            .unwrap();
    };
    // Unpinned, three nodes share this machine's cores: a thread can stall
    // for a while, and in mode (b) that can cost a leader. Keep retrying
    // (outcomes, not latency, are under test here).
    let mut pcfg = PublisherConfig::new(1, c.ingress());
    pcfg.max_attempts = 1000;
    let mut p = Publisher::new(pcfg).unwrap();
    // Steady state.
    let (a1, _) = publish(&mut p, 1, 1001, || drain(&mut consumer, &mut got));
    // The leader goes away; the publisher finds the new one.
    let stopped_at = Instant::now();
    c.stop(first_leader);
    let (a2, first_after) = publish(&mut p, 1001, 2001, || drain(&mut consumer, &mut got));
    let failover = first_after - stopped_at;
    let second_leader = c.wait_leader(Duration::from_secs(5));
    assert_ne!(second_leader, first_leader);
    // The old leader comes back from its log and catches up as a follower.
    c.restart(first_leader);
    let (a3, _) = publish(&mut p, 2001, 2501, || drain(&mut consumer, &mut got));
    let deadline = Instant::now() + Duration::from_secs(20);
    while got.len() < 2500 {
        assert!(
            Instant::now() < deadline,
            "consumer stuck at {} (lost {lost})",
            got.len()
        );
        drain(&mut consumer, &mut got);
    }
    // Every accepted message is on the stream exactly once, in seqno order,
    // where its ACCEPTED said.
    let accepted: HashMap<u64, u64> = a1.into_iter().chain(a2).chain(a3).collect();
    assert_eq!(accepted.len(), 2500);
    let seqnos: HashSet<u64> = accepted.values().copied().collect();
    assert_eq!(seqnos.len(), 2500, "two messages share a seqno");
    assert_eq!(lost, 0);
    for (i, (seqno, p)) in got.iter().enumerate() {
        assert_eq!(*seqno, i as u64 + 1);
        let client_seqno = String::from_utf8_lossy(p)
            .strip_prefix("message ")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(accepted[&client_seqno], *seqno);
    }
    // The rejoined node applied everything too.
    let rejoined = c.nodes[first_leader].as_ref().unwrap();
    let leader = c.nodes[second_leader].as_ref().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while rejoined.stats.raft_commit.load(Ordering::Relaxed)
        < leader.stats.raft_commit.load(Ordering::Relaxed)
    {
        assert!(Instant::now() < deadline, "the rejoined node lags");
        std::thread::sleep(Duration::from_millis(5));
    }
    // Followers held their acknowledgements for fsync in mode (b) only.
    let held: u64 = c
        .nodes
        .iter()
        .flatten()
        .map(|n| n.stats.repl_held.load(Ordering::Relaxed))
        .sum();
    assert_eq!(held > 0, durability == Durability::Sync, "held {held}");
    eprintln!(
        "failover: first ACCEPTED {:?} after stopping the leader",
        failover
    );
    assert!(
        failover < Duration::from_secs(2),
        "failover took {failover:?}"
    );
}

#[test]
fn followers_redirect_clients_and_apply_the_same_log() {
    let c = Cluster::new(3, "redirect", Durability::Async);
    let leader = c.wait_leader(Duration::from_secs(5));
    // The publisher starts at node 1; if that is a follower, its NOT_LEADER
    // names the leader and the publisher goes there.
    let mut pcfg = PublisherConfig::new(7, c.ingress());
    pcfg.max_attempts = 1000;
    let mut p = Publisher::new(pcfg).unwrap();
    let (a, _) = publish(&mut p, 1, 101, || {});
    assert_eq!(a.len(), 100);
    let stat = |k: usize, f: fn(&krusty_node::metrics::Shared) -> u64| {
        f(&c.nodes[k].as_ref().unwrap().stats)
    };
    // (Unless leadership moved to node 1 meanwhile.)
    if leader != 0 && stat(0, |s| s.leader_terms.load(Ordering::Relaxed)) == 0 {
        assert!(stat(0, |s| s.not_leader.load(Ordering::Relaxed)) >= 1);
    }
    // Only a leader publishes, and only what it applied as leader: across
    // the nodes (leadership may move under load), each message once.
    let total: u64 = (0..3)
        .map(|k| stat(k, |s| s.published.load(Ordering::Relaxed)))
        .sum();
    assert_eq!(total, 100);
    // Every node applies the same entries.
    let deadline = Instant::now() + Duration::from_secs(5);
    let target = stat(leader, |s| s.applied.load(Ordering::Relaxed));
    for k in 0..3 {
        while stat(k, |s| s.applied.load(Ordering::Relaxed)) < target {
            assert!(Instant::now() < deadline, "node {} does not apply", k + 1);
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

/// Mode (b) with a publisher that retries every 50 µs, far inside the
/// commit latency (an fsync on two nodes): resends of messages still being
/// committed are not logged again, so the log holds about one entry per
/// message instead of dozens (BASELINE E3: the copies collapsed goodput).
#[test]
fn resends_of_messages_being_committed_are_not_logged_again() {
    let c = Cluster::new(3, "resend", Durability::Sync);
    let leader = c.wait_leader(Duration::from_secs(5));
    let mut pcfg = PublisherConfig::new(9, c.ingress());
    pcfg.retry_interval_ns = 50_000;
    pcfg.max_attempts = 1_000_000;
    let mut p = Publisher::new(pcfg).unwrap();
    let n = 2000;
    let (a, _) = publish(&mut p, 1, n + 1, || {});
    assert_eq!(a.len() as u64, n);
    let stat = |k: usize, f: fn(&krusty_node::metrics::Shared) -> u64| {
        f(&c.nodes[k].as_ref().unwrap().stats)
    };
    let filtered: u64 = (0..3)
        .map(|k| stat(k, |s| s.resends_filtered.load(Ordering::Relaxed)))
        .sum();
    assert!(filtered > 0, "no resend was filtered");
    // Entries applied: the messages and a no-op per term. A resend of a
    // message still being committed is dropped (the filter); a resend of
    // one already decided is answered from the leader's core (2026-11-23).
    // Before that, resends crossing their outcome were logged again: about
    // 1.75 entries per message here, 6.5 to 24 on a CI runner once batching
    // made resends cheap. Cluster.tla: at most 2 copies with the answer from
    // the core, more than 3 without.
    // A leader change adds a no-op, and resends of what the old leader had
    // proposed but not applied are logged again by the new one: on a CI
    // runner, 2123 entries for 2000 messages. The limit is far below what
    // the storm gave (6.5 to 24 per message).
    let applied = stat(leader, |s| s.applied.load(Ordering::Relaxed));
    let terms: u64 = (0..3)
        .map(|k| stat(k, |s| s.leader_terms.load(Ordering::Relaxed)))
        .sum();
    eprintln!(
        "{applied} log entries for {n} messages ({filtered} resends filtered, {terms} leader terms)"
    );
    assert!(
        applied <= n + n / 4,
        "{applied} log entries for {n} messages ({filtered} resends filtered, {terms} leader terms)"
    );
}

/// Go back N after a NAK: seq 2 arrives first and is NAKed, then seq 1 and
/// a burst of its resends, all before mode (b) can commit it. The resends
/// must not be logged again (Cluster.tla's FilterTracksHighestSeq: the
/// filter first kept the index of the highest seq's proposal, already
/// applied here, and let every resend of seq 1 through).
#[test]
fn resends_after_a_nak_are_not_logged_again() {
    use krusty_wire::feedback::{Feedback, Outcome};
    use krusty_wire::ingress::Submit;
    let c = Cluster::new(3, "resend-nak", Durability::Sync);
    let leader = c.wait_leader(Duration::from_secs(5));
    let to = c.cfgs[leader].ingress;
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let send = |seq: u64| {
        let mut b = [0u8; 256];
        let n = Submit {
            client_id: 11,
            incarnation: 1,
            client_seqno: seq,
            append: Append::Unconditional,
            payload: b"x",
        }
        .encode(&mut b)
        .unwrap();
        sock.send_to(&b[..n], to).unwrap();
    };
    let wait_for = |want: &dyn Fn(&Feedback) -> bool| {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut b = [0u8; 2048];
        loop {
            assert!(Instant::now() < deadline, "no such outcome");
            // (Outcomes may come several to a datagram.)
            if let Ok(n) = sock.recv(&mut b)
                && krusty_wire::batch::feedbacks(&b[..n])
                    .flatten()
                    .any(|f| want(&f))
            {
                return;
            }
        }
    };
    let applied = |c: &Cluster| {
        c.nodes[leader]
            .as_ref()
            .unwrap()
            .stats
            .applied
            .load(Ordering::Relaxed)
    };
    send(2);
    wait_for(&|f| matches!(f.outcome, Outcome::Nak));
    let before = applied(&c);
    send(1);
    for _ in 0..200 {
        send(1);
    }
    wait_for(&|f| f.client_seqno == 1 && matches!(f.outcome, Outcome::Accepted { .. }));
    // Let anything logged after the ACCEPTED apply too.
    std::thread::sleep(Duration::from_millis(200));
    let copies = applied(&c) - before;
    assert!(copies <= 3, "seq 1 was logged {copies} times for 201 sends");
}

/// `krusty-admin`'s endpoint: every node reports the same leader and,
/// once idle, the same commit and apply progress; a snapshot on request.
#[test]
fn admin_reports_status_and_takes_snapshots() {
    use krusty_node::admin::request;
    let c = Cluster::new(3, "admin", Durability::Async);
    let leader = c.wait_leader(Duration::from_secs(5));
    let mut pcfg = PublisherConfig::new(1, c.ingress());
    pcfg.max_attempts = 1000;
    let mut p = Publisher::new(pcfg).unwrap();
    publish(&mut p, 0, 200, || {});
    let admins: Vec<SocketAddrV4> = c
        .nodes
        .iter()
        .map(|n| n.as_ref().unwrap().admin_addr().unwrap())
        .collect();
    let get = |kv: &[(String, String)], k: &str| -> String {
        kv.iter().find(|(a, _)| a == k).unwrap().1.clone()
    };
    // Followers apply the last entries a moment after the leader.
    let deadline = Instant::now() + Duration::from_secs(5);
    let statuses = loop {
        let st: Vec<Vec<(String, String)>> = admins
            .iter()
            .map(|a| request(*a, "status", Duration::from_secs(1)).unwrap())
            .collect();
        let applied: HashSet<String> = st.iter().map(|kv| get(kv, "applied")).collect();
        if applied.len() == 1 || Instant::now() > deadline {
            break st;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    for (k, kv) in statuses.iter().enumerate() {
        assert_eq!(get(kv, "node"), (k + 1).to_string());
        assert_eq!(get(kv, "leader"), (leader + 1).to_string());
        let role = if k == leader { "leader" } else { "follower" };
        assert_eq!(get(kv, "role"), role);
        assert_eq!(get(kv, "next_seqno"), "201");
        assert_eq!(get(kv, "clients"), "1");
        assert_eq!(get(kv, "durability"), "a");
        assert_eq!(get(kv, "applied"), get(&statuses[0], "applied"));
    }
    // A snapshot now, at the applied index (no new entries since).
    for a in &admins {
        let kv = request(*a, "snapshot", Duration::from_secs(10)).unwrap();
        let st = request(*a, "status", Duration::from_secs(1)).unwrap();
        assert_eq!(get(&kv, "snapshot_index"), get(&st, "applied"));
    }
    // Asking again with nothing new applied is answered at once.
    let kv = request(admins[0], "snapshot", Duration::from_secs(10)).unwrap();
    assert_eq!(get(&kv, "snapshot_index"), get(&statuses[0], "applied"));
    assert!(request(admins[0], "bogus", Duration::from_secs(1)).is_err());
}

/// `krusty-admin transfer`: leadership moves to the named node, and the
/// publisher follows it with no gap or duplicate in the stream.
#[test]
fn admin_hands_over_leadership() {
    use krusty_node::admin::request;
    let c = Cluster::new(3, "handover", Durability::Async);
    let leader = c.wait_leader(Duration::from_secs(5));
    let admins: Vec<SocketAddrV4> = c
        .nodes
        .iter()
        .map(|n| n.as_ref().unwrap().admin_addr().unwrap())
        .collect();
    let mut pcfg = PublisherConfig::new(1, c.ingress());
    pcfg.max_attempts = 1000;
    let mut p = Publisher::new(pcfg).unwrap();
    let (first, _) = publish(&mut p, 0, 100, || {});
    let to = (leader + 1) % 3;
    // Asking a follower is refused, naming the leader.
    let e = request(
        admins[to],
        &format!("transfer {}", leader + 1),
        Duration::from_secs(5),
    )
    .unwrap_err();
    assert!(e.contains("not the leader"), "{e}");
    let kv = request(
        admins[leader],
        &format!("transfer {}", to + 1),
        Duration::from_secs(10),
    )
    .unwrap();
    assert!(
        kv.iter()
            .any(|(k, v)| k == "leader" && *v == (to + 1).to_string())
    );
    assert_eq!(c.wait_leader(Duration::from_secs(5)), to);
    let (second, _) = publish(&mut p, 100, 200, || {});
    let mut seqnos: Vec<u64> = first.values().chain(second.values()).copied().collect();
    seqnos.sort_unstable();
    assert_eq!(seqnos, (1..=200).collect::<Vec<_>>());
    assert!(request(admins[to], "transfer 9", Duration::from_secs(5)).is_err());
}

/// Session roll (`krusty-admin roll`): END_OF_SESSION on the old stream,
/// then the new stream from seqno 1 with a fresh core. A client from before
/// the roll is told to start a new incarnation (nothing is sequenced
/// twice); the old stream stays recoverable; a restarted node comes back
/// on the new stream.
#[test]
fn session_roll_starts_a_new_stream() {
    use krusty_node::admin::request;
    let mut c = Cluster::new(3, "roll", Durability::Async);
    let leader = c.wait_leader(Duration::from_secs(5));
    let admins: Vec<SocketAddrV4> = c
        .nodes
        .iter()
        .map(|n| n.as_ref().unwrap().admin_addr().unwrap())
        .collect();
    let get = |kv: &[(String, String)], k: &str| -> String {
        kv.iter().find(|(a, _)| a == k).unwrap().1.clone()
    };
    // A consumer following whatever stream the cluster is on.
    let mut consumer = Consumer::new(ConsumerConfig {
        start_seqno: Some(1),
        ..ConsumerConfig::new(c.feed, c.recovery())
    })
    .unwrap();
    let mut got: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut ends = 0;
    let drain = |consumer: &mut Consumer, got: &mut Vec<(u64, Vec<u8>)>, ends: &mut u32| {
        consumer
            .poll(|e| match e {
                ConsumerEvent::Message(d) => got.push((d.seqno, d.payload.to_vec())),
                ConsumerEvent::Lost { count } => panic!("lost {count}"),
                ConsumerEvent::EndOfStream => *ends += 1,
            })
            .unwrap();
    };
    let mut pcfg = PublisherConfig::new(1, c.ingress());
    pcfg.max_attempts = 1000;
    let mut p = Publisher::new(pcfg).unwrap();
    publish(&mut p, 0, 50, || {});
    let kv = request(admins[leader], "roll 9", Duration::from_secs(10)).unwrap();
    assert_eq!(get(&kv, "stream"), "9");
    assert!(
        request(admins[leader], "roll 9", Duration::from_secs(10)).is_err(),
        "ids are not reused"
    );
    // The publisher's incarnation predates the roll: its next message fails
    // and it restarts; after that, the new stream from seqno 1.
    let mut restarted = false;
    p.submit(Append::Unconditional, b"probe").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !restarted {
        assert!(Instant::now() < deadline, "no restart");
        p.poll(|e| match e {
            PublisherEvent::Failed { .. } => {}
            PublisherEvent::Restarted { .. } => restarted = true,
            other => panic!("unexpected {other:?}"),
        })
        .unwrap();
    }
    let (after, _) = publish(&mut p, 50, 80, || {});
    let mut seqnos: Vec<u64> = after.values().copied().collect();
    seqnos.sort_unstable();
    assert_eq!(seqnos, (1..=30).collect::<Vec<_>>());
    // The consumer: the old stream, its end, then the new one.
    let deadline = Instant::now() + Duration::from_secs(10);
    while got.len() < 80 {
        assert!(Instant::now() < deadline, "consumer stuck at {}", got.len());
        drain(&mut consumer, &mut got, &mut ends);
    }
    let expect: Vec<(u64, Vec<u8>)> = (0..50)
        .map(|i| (i + 1, payload(i)))
        .chain((50..80).map(|i| (i - 49, payload(i))))
        .collect();
    assert_eq!(got, expect);
    assert_eq!(ends, 1);
    // Every node is on stream 9; the old stream is still recoverable.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let st: Vec<_> = admins
            .iter()
            .map(|a| request(*a, "status", Duration::from_secs(1)).unwrap())
            .collect();
        if st
            .iter()
            .all(|kv| get(kv, "stream") == "9" && get(kv, "next_seqno") == "31")
        {
            break;
        }
        assert!(Instant::now() < deadline, "nodes did not converge: {st:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
    // The old stream's messages, asked for by stream id after the roll.
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let mut buf = [0u8; 2048];
    let k = krusty_wire::stream::encode_recover(&mut buf, 1, 1, 50).unwrap();
    sock.send_to(&buf[..k], c.recovery()[leader]).unwrap();
    let mut old: Vec<(u64, Vec<u8>)> = Vec::new();
    while let Ok(n) = sock.recv(&mut buf) {
        if let Ok(krusty_wire::stream::Frame::Data(d)) =
            krusty_wire::stream::Frame::decode(&buf[..n])
        {
            assert_eq!(d.stream_id, 1);
            old.extend(d.messages().map(|m| (m.seqno, m.payload.to_vec())));
        }
        if old.len() >= 50 {
            break;
        }
    }
    assert_eq!(old, expect[..50]);
    // A follower restarted from its log is back on stream 9.
    // (After a snapshot, so the streams table comes back from it.)
    let f = (leader + 1) % 3;
    request(admins[f], "snapshot", Duration::from_secs(10)).unwrap();
    c.stop(f);
    c.restart(f);
    let a = c.nodes[f].as_ref().unwrap().admin_addr().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(kv) = request(a, "status", Duration::from_secs(1))
            && get(&kv, "stream") == "9"
            && get(&kv, "next_seqno") == "31"
        {
            break;
        }
        assert!(Instant::now() < deadline, "restarted node not on stream 9");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The MoldUDP64 adapter across a roll, on a one-member cluster (how a
/// single node rolls): the configured session ends, and the next one is
/// named after the new stream id, from seqno 1.
#[test]
fn mold_session_follows_a_roll() {
    use krusty_node::admin::request;
    let mold = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    mold.set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let feed = match mold.local_addr().unwrap() {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let session = krusty_mold::Session::new("KC1").unwrap();
    let c = Cluster::with(1, "mold-roll", Durability::Async, |cfg| {
        cfg.mold = Some(krusty_node::MoldConfig {
            session,
            feed,
            rerequest: "127.0.0.1:0".parse().unwrap(),
        });
    });
    c.wait_leader(Duration::from_secs(5));
    let admin = c.nodes[0].as_ref().unwrap().admin_addr().unwrap();
    let mut pcfg = PublisherConfig::new(1, c.ingress());
    pcfg.max_attempts = 1000;
    let mut p = Publisher::new(pcfg).unwrap();
    publish(&mut p, 0, 5, || {});
    request(admin, "roll 9", Duration::from_secs(10)).unwrap();
    let mut restarted = false;
    p.submit(Append::Unconditional, b"probe").unwrap();
    while !restarted {
        p.poll(|e| {
            if let PublisherEvent::Restarted { .. } = e {
                restarted = true
            }
        })
        .unwrap();
    }
    publish(&mut p, 5, 8, || {});
    // (session, seqno, payload) of every message, and where sessions ended.
    let mut got: Vec<([u8; 10], u64, Vec<u8>)> = Vec::new();
    let mut ended: Vec<([u8; 10], u64)> = Vec::new();
    let mut buf = [0u8; 2048];
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.len() < 8 && Instant::now() < deadline {
        let Ok(n) = mold.recv(&mut buf) else { continue };
        let pk = moldudp::Packet::new(&buf[..n]);
        let sess = *pk.session_ident_raw();
        match pk.packet_kind() {
            moldudp::PacketKind::EndOfSession => ended.push((sess, pk.seq_num())),
            moldudp::PacketKind::Standard => {
                for (i, m) in pk.iter().enumerate() {
                    got.push((sess, pk.seq_num() + i as u64, m.data().to_vec()));
                }
            }
            moldudp::PacketKind::Heartbeat => {}
        }
    }
    let new = *b"0000000009";
    let expect: Vec<([u8; 10], u64, Vec<u8>)> = (0..5)
        .map(|i| (session.0, i + 1, payload(i)))
        .chain((5..8).map(|i| (new, i - 4, payload(i))))
        .collect();
    assert_eq!(got, expect);
    assert!(ended.contains(&(session.0, 6)), "{ended:?}");
}

/// Writes traces of each node's cluster shell for spec/tla/TraceCluster.tla
/// (spec/tla/check-cluster-traces.sh), across what the shell must get
/// right: resends faster than commits (the filter), a NAK and go-back-N,
/// duplicates of decided messages, the leader stopping (NOT_LEADER, a new
/// term), and the old leader restarting from a snapshot.
///   KRUSTY_CLUSTER_TRACE_DIR=dir cargo test -p krusty-client --test cluster -- --ignored traces
#[test]
#[ignore]
fn cluster_shell_traces() {
    use krusty_wire::feedback::{Feedback, Outcome};
    use krusty_wire::ingress::Submit;
    let dir = PathBuf::from(
        std::env::var_os("KRUSTY_CLUSTER_TRACE_DIR").expect("KRUSTY_CLUSTER_TRACE_DIR"),
    );
    std::fs::create_dir_all(&dir).unwrap();
    for id in 1..=3 {
        let _ = std::fs::remove_file(dir.join(format!("node{id}.trace")));
    }
    let mut c = Cluster::with(3, "trace", Durability::Sync, |cfg| {
        cfg.storage.as_mut().unwrap().snapshot_every = 40;
        let cl = cfg.cluster.as_mut().unwrap();
        cl.trace = Some(dir.join(format!("node{}.trace", cl.id)));
    });
    let leader = c.wait_leader(Duration::from_secs(5));
    // A client that retries every 50 µs, inside the commit latency.
    let mut pcfg = PublisherConfig::new(21, c.ingress());
    pcfg.retry_interval_ns = 50_000;
    pcfg.max_attempts = 1_000_000;
    let mut p = Publisher::new(pcfg).unwrap();
    publish(&mut p, 1, 41, || {});
    // A raw client: a gap (NAK), the missing message and a burst of its
    // resends, then duplicates of what was decided.
    let to = c.cfgs[leader].ingress;
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_millis(20)))
        .unwrap();
    let send = |seq: u64| {
        let mut b = [0u8; 256];
        let n = Submit {
            client_id: 23,
            incarnation: 1,
            client_seqno: seq,
            append: Append::Unconditional,
            payload: b"x",
        }
        .encode(&mut b)
        .unwrap();
        sock.send_to(&b[..n], to).unwrap();
    };
    let wait_for = |want: &dyn Fn(&Feedback) -> bool| {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut b = [0u8; 2048];
        loop {
            assert!(Instant::now() < deadline, "no such outcome");
            // (Outcomes may come several to a datagram.)
            if let Ok(n) = sock.recv(&mut b)
                && krusty_wire::batch::feedbacks(&b[..n])
                    .flatten()
                    .any(|f| want(&f))
            {
                return;
            }
        }
    };
    send(2);
    wait_for(&|f| matches!(f.outcome, Outcome::Nak));
    for _ in 0..30 {
        send(1);
    }
    wait_for(&|f| f.client_seqno == 1 && matches!(f.outcome, Outcome::Accepted { .. }));
    send(2);
    wait_for(&|f| f.client_seqno == 2 && matches!(f.outcome, Outcome::Accepted { .. }));
    send(1);
    send(2);
    wait_for(&|f| f.client_seqno == 2 && matches!(f.outcome, Outcome::Accepted { .. }));
    // The leader stops; the publisher finds the next one.
    c.stop(leader);
    publish(&mut p, 41, 81, || {});
    let second = c.wait_leader(Duration::from_secs(5));
    assert_ne!(second, leader);
    // The old leader restarts from its snapshot and log; a second
    // publisher.
    c.restart(leader);
    let mut pcfg = PublisherConfig::new(22, c.ingress());
    pcfg.retry_interval_ns = 50_000;
    pcfg.max_attempts = 1_000_000;
    let mut q = Publisher::new(pcfg).unwrap();
    publish(&mut q, 1, 41, || {});
    publish(&mut p, 81, 101, || {});
    // Let every node apply everything.
    std::thread::sleep(Duration::from_millis(300));
    drop(c);
    for id in 1..=3 {
        let t = std::fs::read_to_string(dir.join(format!("node{id}.trace"))).unwrap();
        assert!(t.lines().count() > 10, "node {id}: a short trace");
    }
}

/// A power loss in mode (a): a follower comes back in a new boot (its
/// recorded boot id no longer matches), so writes it acknowledged may be
/// gone. It recovers: no votes until it holds the leader's log, then votes
/// again, and the marker that outlives process crashes is removed
/// (spec/RAFT-VERIFICATION.md, "Decision").
#[test]
fn a_node_back_from_a_power_loss_recovers_before_voting() {
    let mut c = Cluster::new(3, "power", Durability::Async);
    let leader = c.wait_leader(Duration::from_secs(5));
    let mut pcfg = PublisherConfig::new(31, c.ingress());
    pcfg.max_attempts = 1000;
    let mut p = Publisher::new(pcfg).unwrap();
    publish(&mut p, 1, 201, || {});
    let f = (leader + 1) % 3;
    c.stop(f);
    // As if the machine had rebooted.
    std::fs::write(c.dirs[f].join("boot_id"), "another boot").unwrap();
    c.restart(f);
    let stat = |c: &Cluster, k: usize, f: fn(&krusty_node::metrics::Shared) -> u64| {
        f(&c.nodes[k].as_ref().unwrap().stats)
    };
    // (The core stage counts it when it starts, just after `restart`.)
    let deadline = Instant::now() + Duration::from_secs(2);
    while stat(&c, f, |s| s.raft_recoveries.load(Ordering::Relaxed)) == 0 {
        assert!(
            Instant::now() < deadline,
            "a node back from a power loss must start recovering"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    // The leader's heartbeats and Appends bring it up to date.
    let deadline = Instant::now() + Duration::from_secs(10);
    while stat(&c, f, |s| s.raft_recovering.load(Ordering::Relaxed)) == 1
        || c.dirs[f].join("recovering").exists()
    {
        assert!(Instant::now() < deadline, "still recovering");
        std::thread::sleep(Duration::from_millis(5));
    }
    // A plain process restart (same boot) does not recover.
    let g = (leader + 2) % 3;
    c.stop(g);
    c.restart(g);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        stat(&c, g, |s| s.raft_recoveries.load(Ordering::Relaxed)),
        0
    );
    // The cluster carries on, with the node voting again.
    publish(&mut p, 201, 301, || {});
}
