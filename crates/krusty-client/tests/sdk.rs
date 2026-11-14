//! The SDK against a real node over loopback.
use krusty_client::{
    Consumer, ConsumerConfig, ConsumerEvent, Publisher, PublisherConfig, PublisherEvent,
};
use krusty_node::{Durability, Node, NodeConfig, StorageConfig};
use krusty_wire::ingress::Append;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

fn free_port() -> SocketAddrV4 {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    match s.local_addr().unwrap() {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    }
}

fn payload(i: u64) -> Vec<u8> {
    format!("payload {i}").into_bytes()
}

/// Publishes `n` messages; returns client seqno → global seqno.
fn publish(node: &Node, client_id: u32, n: u64) -> HashMap<u64, u64> {
    let mut p = Publisher::new(PublisherConfig::new(client_id, vec![node.ingress_addr()])).unwrap();
    let mut accepted = HashMap::new();
    let mut next = 1;
    let deadline = Instant::now() + Duration::from_secs(30);
    while (accepted.len() as u64) < n {
        assert!(
            Instant::now() < deadline,
            "publisher stuck at {}",
            accepted.len()
        );
        while next <= n {
            match p.submit(Append::Unconditional, &payload(next)) {
                Ok(seq) => {
                    assert_eq!(seq, next);
                    next += 1;
                }
                Err(_) => break,
            }
        }
        p.poll(|e| match e {
            PublisherEvent::Accepted {
                client_seqno,
                global_seqno,
            } => {
                assert!(accepted.insert(client_seqno, global_seqno).is_none());
            }
            other => panic!("unexpected {other:?}"),
        })
        .unwrap();
    }
    accepted
}

/// Polls `c` until it has delivered through seqno `upto`.
fn consume(c: &mut Consumer, upto: u64) -> Vec<(u64, Vec<u8>)> {
    let mut got = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while got.last().map(|(s, _)| *s) != Some(upto) {
        assert!(
            Instant::now() < deadline,
            "consumer stuck at {:?}",
            got.last().map(|g: &(u64, Vec<u8>)| g.0)
        );
        c.poll(|e| match e {
            ConsumerEvent::Message(d) => got.push((d.seqno, d.payload.to_vec())),
            ConsumerEvent::Lost { count } => panic!("lost {count}"),
            ConsumerEvent::EndOfStream => {}
        })
        .unwrap();
    }
    got
}

#[test]
fn publish_and_consume_in_order() {
    let feed = free_port();
    let node = krusty_node::start(NodeConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        feed,
        "127.0.0.1:0".parse().unwrap(),
    ))
    .unwrap();
    let mut c = Consumer::new(ConsumerConfig {
        start_seqno: Some(1),
        ..ConsumerConfig::new(feed, vec![node.recovery_addr()])
    })
    .unwrap();
    let accepted = publish(&node, 1, 3000);
    let got = consume(&mut c, 3000);
    assert_eq!(got.len(), 3000);
    // Each delivery carries what its ACCEPTED named.
    let by_global: HashMap<u64, u64> = accepted.iter().map(|(c, g)| (*g, *c)).collect();
    for (i, (seqno, p)) in got.iter().enumerate() {
        assert_eq!(*seqno, i as u64 + 1);
        assert_eq!(p, &payload(by_global[seqno]));
    }
    node.stop();
}

#[test]
fn a_late_consumer_replays_the_session_from_disk() {
    let dir = std::env::temp_dir().join(format!("krusty-sdk-replay-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let feed = free_port();
    let mut cfg = NodeConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        feed,
        "127.0.0.1:0".parse().unwrap(),
    );
    cfg.history = 64; // the in-memory history holds only the tail
    cfg.heartbeat_us = 2_000;
    cfg.storage = Some(StorageConfig {
        log: krusty_log::LogConfig::new(&dir),
        durability: Durability::Async,
        snapshot_every: 1_000_000,
    });
    let node = krusty_node::start(cfg).unwrap();
    publish(&node, 1, 2000);
    // Joins after everything was published, and starts from the beginning.
    let mut c = Consumer::new(ConsumerConfig {
        start_seqno: Some(1),
        ..ConsumerConfig::new(feed, vec![node.recovery_addr()])
    })
    .unwrap();
    let got = consume(&mut c, 2000);
    assert_eq!(got.len(), 2000);
    for (i, (seqno, p)) in got.iter().enumerate() {
        assert_eq!(*seqno, i as u64 + 1);
        assert_eq!(p, &payload(*seqno));
    }
    assert!(node.stats.recovered_from_disk.load(Ordering::Relaxed) >= 1900);
    node.stop();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn consumers_missing_the_same_range_get_it_once_on_the_feed() {
    // Multicast kept on the loopback interface (see docs/BASELINE.md).
    let group = SocketAddrV4::new(Ipv4Addr::new(239, 77, 3, 3), free_port().port());
    let lo = Ipv4Addr::LOCALHOST;
    let mut cfg = NodeConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        group,
        "127.0.0.1:0".parse().unwrap(),
    );
    cfg.interface = lo;
    cfg.recover_suppress_us = 20_000;
    cfg.heartbeat_us = 2_000;
    let node = krusty_node::start(cfg).unwrap();
    publish(&node, 1, 500);
    // Three consumers join late and all miss 1..500.
    let mut cs: Vec<Consumer> = (0..3u64)
        .map(|k| {
            Consumer::new(ConsumerConfig {
                interface: lo,
                start_seqno: Some(1),
                max_recover_delay_ns: 2_000_000,
                seed: k + 1,
                ..ConsumerConfig::new(group, vec![node.recovery_addr()])
            })
            .unwrap()
        })
        .collect();
    let mut got = vec![0u64; 3];
    let deadline = Instant::now() + Duration::from_secs(20);
    while got.iter().any(|&g| g < 500) {
        assert!(Instant::now() < deadline, "stuck: {got:?}");
        for (k, c) in cs.iter_mut().enumerate() {
            c.poll(|e| {
                if let ConsumerEvent::Message(d) = e {
                    assert_eq!(d.seqno, got[k] + 1);
                    assert_eq!(d.payload, payload(d.seqno));
                    got[k] = d.seqno;
                }
            })
            .unwrap();
        }
    }
    let on_feed = node.stats.recover_on_feed.load(Ordering::Relaxed);
    let requests = node.stats.recover_requests.load(Ordering::Relaxed);
    eprintln!(
        "RECOVER requests {requests}, answered on the feed {on_feed}, suppressed {}",
        node.stats.recover_suppressed.load(Ordering::Relaxed)
    );
    assert!(
        on_feed >= 1,
        "several consumers asked for the same range: it should go out once on the feed"
    );
    node.stop();
}
