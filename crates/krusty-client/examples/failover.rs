//! Failover time (PRD NFR-6: leader crash to the first new message on the
//! feed, p99 ≤ 200 ms) on a 3-node cluster over loopback.
//!
//! `cargo run --release -p krusty-client --example failover -- [trials]`
//! (`FAILOVER_CORES=2,3,4`: pin node k's stages to core k; `FAILOVER_MAX_MS`:
//! exit 1 if a trial takes longer; `FAILOVER_TRACE`: print node states;
//! `FAILOVER_DURABILITY=b`: durability mode (b).)
//!
//! Each trial: a publisher sends steadily (one message every 200 µs) and a
//! consumer follows the feed; the leader is stopped (its threads end, it
//! sends nothing more: a crash as far as the others can tell); we measure
//! until the consumer delivers a message newer than anything it had, and
//! until a new leader exists. The stopped node then restarts from its log
//! and rejoins before the next trial.
use krusty_client::{
    Consumer, ConsumerConfig, ConsumerEvent, Publisher, PublisherConfig, PublisherEvent,
};
use krusty_node::{ClusterConfig, Durability, Node, NodeConfig, StorageConfig};
use krusty_wire::ingress::Append;
use std::net::SocketAddrV4;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

#[path = "../tests/common/ports.rs"]
mod ports;
use ports::Ports;

fn leader(nodes: &[Option<Node>]) -> Option<usize> {
    let l: Vec<usize> = (0..nodes.len())
        .filter(|&k| {
            nodes[k]
                .as_ref()
                .is_some_and(|n| n.stats.raft_role.load(Ordering::Relaxed) == 3)
        })
        .collect();
    (l.len() == 1).then(|| l[0])
}

fn pct(v: &[Duration], p: f64) -> Duration {
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn main() {
    let trials: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(30);
    let base = std::env::temp_dir().join(format!("krusty-failover-{}", std::process::id()));
    let durability = match std::env::var("FAILOVER_DURABILITY").as_deref() {
        Ok("b") => Durability::Sync,
        _ => Durability::Async,
    };
    let mut ports = Ports::new(10);
    let feed = ports.take();
    let repl: Vec<(u32, SocketAddrV4)> = (1..=3).map(|id| (id, ports.take())).collect();
    let cfgs: Vec<NodeConfig> = (1..=3u32)
        .map(|id| {
            let mut cfg = NodeConfig::new(ports.take(), feed, ports.take());
            cfg.heartbeat_us = 5_000;
            cfg.storage = Some(StorageConfig {
                log: krusty_log::LogConfig::new(base.join(id.to_string())),
                durability,
                snapshot_every: 100_000,
            });
            let mut cl = ClusterConfig::new(id, repl.clone());
            // FAILOVER_CORES=2,3,4: all of node k's stages share core k.
            if let Some(c) = std::env::var("FAILOVER_CORES").ok().and_then(|v| {
                v.split(',')
                    .nth(id as usize - 1)
                    .and_then(|c| c.parse().ok())
            }) {
                cfg.cores = [Some(c); 4];
                cl.core = Some(c);
            }
            cfg.cluster = Some(cl);
            cfg
        })
        .collect();
    drop(ports); // release the reserved ports for the nodes to bind
    let mut nodes: Vec<Option<Node>> = cfgs
        .iter()
        .map(|c| Some(krusty_node::start(c.clone()).unwrap()))
        .collect();
    let mut consumer = Consumer::new(ConsumerConfig {
        start_seqno: Some(1),
        ..ConsumerConfig::new(feed, cfgs.iter().map(|c| c.recovery).collect())
    })
    .unwrap();
    let mut pcfg = PublisherConfig::new(1, cfgs.iter().map(|c| c.ingress).collect());
    pcfg.failover_ns = 30_000_000;
    pcfg.retry_interval_ns = 10_000_000;
    pcfg.max_attempts = 1000;
    let mut p = Publisher::new(pcfg).unwrap();
    let mut seen = 0u64;
    let mut failed = 0u64;
    let mut next_send = Instant::now();
    // Runs the publisher and the consumer until `until` returns true.
    let mut run = |nodes: &[Option<Node>],
                   seen: &mut u64,
                   until: &mut dyn FnMut(&[Option<Node>], u64) -> bool| {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if Instant::now() >= next_send {
                let _ = p.submit(Append::Unconditional, b"tick");
                next_send += Duration::from_micros(200);
            }
            p.poll(|e| {
                if let PublisherEvent::Failed { .. } = e {
                    failed += 1;
                }
            })
            .unwrap();
            consumer
                .poll(|e| {
                    if let ConsumerEvent::Message(d) = e {
                        *seen = (*seen).max(d.seqno);
                    }
                })
                .unwrap();
            if until(nodes, *seen) {
                return true;
            }
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_micros(20));
        }
    };
    let mut to_feed = Vec::new();
    let mut to_leader = Vec::new();
    let trace = std::env::var("FAILOVER_TRACE").is_ok();
    for t in 0..trials {
        // Settle: one leader for a while, messages flowing.
        let mut since: Option<Instant> = None;
        let ok = run(&nodes, &mut seen, &mut |n, _| {
            if leader(n).is_some() {
                since.get_or_insert_with(Instant::now).elapsed() > Duration::from_millis(300)
            } else {
                since = None;
                false
            }
        });
        assert!(ok, "trial {t}: no stable leader");
        let k = leader(&nodes).unwrap();
        let victim = nodes[k].take().unwrap();
        let old_term = victim.stats.raft_term.load(Ordering::Relaxed);
        let vstats = victim.stats.clone();
        let t0 = Instant::now();
        // Stop in the background (its threads notice within a poll); the
        // others must not wait for it.
        let stopper = std::thread::spawn(move || victim.stop());
        let mut elected: Option<Duration> = None;
        let mut fed: Option<Duration> = None;
        let mut progress: Vec<(u64, Duration)> = Vec::new();
        let mut last_print = Instant::now();
        let ok = run(&nodes, &mut seen, &mut |n, s| {
            let now = t0.elapsed();
            if elected.is_none()
                && leader(n).is_some_and(|l| {
                    n[l].as_ref()
                        .unwrap()
                        .stats
                        .raft_term
                        .load(Ordering::Relaxed)
                        > old_term
                })
            {
                elected = Some(now);
            }
            // New messages: beyond anything the old leader assigned (known
            // once it has stopped; decided from the recorded progress).
            if progress.last().is_none_or(|p: &(u64, Duration)| p.0 < s) {
                progress.push((s, now));
            }
            if fed.is_none() && stopper.is_finished() {
                let old_next = vstats.stream_next.load(Ordering::Relaxed);
                fed = progress.iter().find(|p| p.0 >= old_next).map(|p| p.1);
            }
            if trace
                && now > Duration::from_millis(200)
                && last_print.elapsed() > Duration::from_millis(100)
            {
                last_print = Instant::now();
                let st: Vec<String> = n
                    .iter()
                    .map(|x| match x {
                        Some(x) => format!(
                            "r{} t{} c{} next{} nl{} in{} out{}",
                            x.stats.raft_role.load(Ordering::Relaxed),
                            x.stats.raft_term.load(Ordering::Relaxed),
                            x.stats.raft_commit.load(Ordering::Relaxed),
                            x.stats.stream_next.load(Ordering::Relaxed),
                            x.stats.not_leader.load(Ordering::Relaxed),
                            x.stats.repl_in.load(Ordering::Relaxed),
                            x.stats.repl_out.load(Ordering::Relaxed),
                        ),
                        None => "down".into(),
                    })
                    .collect();
                eprintln!(
                    "  {:>6.0} ms: seen {s} old_next {} | {}",
                    now.as_secs_f64() * 1e3,
                    vstats.stream_next.load(Ordering::Relaxed),
                    st.join(" | ")
                );
            }
            elected.is_some() && fed.is_some()
        });
        assert!(ok, "trial {t}: no failover");
        let (elected, fed) = (elected.unwrap(), fed.unwrap());
        to_feed.push(fed);
        to_leader.push(elected);
        eprintln!(
            "trial {t:>3}: node {} stopped; new leader after {:>6.1} ms, new messages after {:>6.1} ms",
            k + 1,
            elected.as_secs_f64() * 1e3,
            fed.as_secs_f64() * 1e3
        );
        nodes[k] = Some(krusty_node::start(cfgs[k].clone()).unwrap());
    }
    to_feed.sort();
    to_leader.sort();
    let ms = |d: Duration| d.as_secs_f64() * 1e3;
    println!(
        "failover over {trials} trials: new messages p50 {:.1} ms, p90 {:.1} ms, max {:.1} ms; \
         new leader p50 {:.1} ms, max {:.1} ms; {failed} messages failed",
        ms(pct(&to_feed, 0.5)),
        ms(pct(&to_feed, 0.9)),
        ms(*to_feed.last().unwrap()),
        ms(pct(&to_leader, 0.5)),
        ms(*to_leader.last().unwrap()),
    );
    for n in nodes.into_iter().flatten() {
        n.stop();
    }
    let _ = std::fs::remove_dir_all(&base);
    // FAILOVER_MAX_MS: fail (exit 1) if any trial took longer (CI smoke).
    if let Some(limit) = std::env::var("FAILOVER_MAX_MS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
    {
        let worst = ms(*to_feed.last().unwrap());
        if worst > limit || failed > 0 {
            println!("FAIL: worst {worst:.1} ms (limit {limit} ms), {failed} failed");
            std::process::exit(1);
        }
    }
}
