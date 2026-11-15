//! A 3-node cluster over loopback: replication, leader failover, a node
//! rejoining from its log, and the stream staying exactly-once throughout.
use krusty_client::{
    Consumer, ConsumerConfig, ConsumerEvent, Publisher, PublisherConfig, PublisherEvent,
};
use krusty_node::{ClusterConfig, Durability, Node, NodeConfig, StorageConfig};
use krusty_wire::ingress::Append;
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, SocketAddrV4, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

fn free_port() -> SocketAddrV4 {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    match s.local_addr().unwrap() {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    }
}

struct Cluster {
    cfgs: Vec<NodeConfig>,
    nodes: Vec<Option<Node>>,
    dirs: Vec<PathBuf>,
    feed: SocketAddrV4,
}

impl Cluster {
    fn new(n: u32, tag: &str) -> Cluster {
        let feed = free_port();
        let repl: Vec<(u32, SocketAddrV4)> = (1..=n).map(|id| (id, free_port())).collect();
        let mut cfgs = Vec::new();
        let mut dirs = Vec::new();
        for id in 1..=n {
            let dir = std::env::temp_dir()
                .join(format!("krusty-cluster-{tag}-{}-{id}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let mut cfg = NodeConfig::new(free_port(), feed, free_port());
            cfg.heartbeat_us = 5_000;
            cfg.storage = Some(StorageConfig {
                log: krusty_log::LogConfig::new(&dir),
                durability: Durability::Async,
                snapshot_every: 500,
            });
            cfg.cluster = Some(ClusterConfig::new(id, repl.clone()));
            cfgs.push(cfg);
            dirs.push(dir);
        }
        let mut c = Cluster {
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
    let mut c = Cluster::new(3, "failover");
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
    let mut p = Publisher::new(PublisherConfig::new(1, c.ingress())).unwrap();
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
    assert_eq!(rejoined.stats.raft_role.load(Ordering::Relaxed), 0);
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
    let c = Cluster::new(3, "redirect");
    let leader = c.wait_leader(Duration::from_secs(5));
    // The publisher starts at node 1; if that is a follower, its NOT_LEADER
    // names the leader and the publisher goes there.
    let mut p = Publisher::new(PublisherConfig::new(7, c.ingress())).unwrap();
    let (a, _) = publish(&mut p, 1, 101, || {});
    assert_eq!(a.len(), 100);
    let stat = |k: usize, f: fn(&krusty_node::metrics::Shared) -> u64| {
        f(&c.nodes[k].as_ref().unwrap().stats)
    };
    if leader != 0 {
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
