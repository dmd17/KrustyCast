//! Open-loop load generator for krusty-node (successor of `loadgen`).
//!
//! Each sender is a krusty-session client submitting on a fixed schedule;
//! latency is measured from each message's *scheduled* time (no coordinated
//! omission) to (a) its arrival on the feed and (b) its ACCEPTED. A run is
//! INVALID (exit 1) if the generator could not keep up or anything was lost.
//!
//!   krusty-load [--rate N] [--duration S] [--warmup S] [--senders K] [--payload B]
//!               [--pin-senders a,b] [--pin-receiver c] [--node-cores i,c,e,r] [--spin]
//!               [--combined] [--storage DIR] [--durability a|b]
//!               [--cluster N] [--cluster-cores c1,c2,..] [--sleep-us U]
//!               [--leader-cores i,c,e,repl[,store]] [--follower-cores c,repl,o[,store]/...]
//!
//! `--leader-cores`: node 1 gets shorter election timeouts (so it leads),
//! spins, and pins ingress, core, egress, replication (and optionally the
//! storage stage) there; the other
//! nodes sleep when idle, on `--cluster-cores` (indexed from node 2).
//! `--follower-cores` instead makes the other nodes spin too, with the core
//! stage on c, replication on repl, and ingress, egress and recovery
//! (idle on a follower) sharing o, and optionally storage on its own core;
//! one list per follower.
//!
//! `--cluster N` runs N replicated nodes in this process (storage under
//! `--storage`, default a temporary directory) and sends to the leader;
//! `--cluster-cores` puts all of node k's stages on core ck.
use hdrhistogram::Histogram;
use krusty_net::{RecvBatch, SendBatch};
use krusty_node::{Idle, NodeConfig};
use krusty_session::{Config as SessionConfig, Event, Session};
use krusty_wire::feedback::Feedback;
use krusty_wire::ingress::{Append, Submit};
use krusty_wire::stream::Frame;
use std::net::{SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

fn pin(core: Option<usize>) {
    if let Some(core) = core {
        // SAFETY: plain data; sched_setaffinity only reads it.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(core, &mut set);
            libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set);
        }
    }
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 100_000_000_000, 3).unwrap()
}

fn v4(a: SocketAddr) -> SocketAddrV4 {
    match a {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    }
}

struct Args {
    rate: u64,
    duration: u64,
    warmup: u64,
    senders: u64,
    payload: usize,
    pin_senders: Vec<usize>,
    pin_receiver: Option<usize>,
    node_cores: [Option<usize>; 4],
    spin: bool,
    combined: bool,
    storage: Option<std::path::PathBuf>,
    durability: krusty_node::Durability,
    cluster: u32,
    cluster_cores: Vec<usize>,
    sleep_us: Option<u64>,
    leader_cores: Vec<usize>,
    follower_cores: Vec<Vec<usize>>,
}

fn parse() -> Args {
    let mut a = Args {
        rate: 100_000,
        duration: 5,
        warmup: 1,
        senders: 1,
        payload: 64,
        pin_senders: Vec::new(),
        pin_receiver: None,
        node_cores: [None; 4],
        spin: false,
        combined: false,
        storage: None,
        durability: krusty_node::Durability::Async,
        cluster: 0,
        cluster_cores: Vec::new(),
        sleep_us: None,
        leader_cores: Vec::new(),
        follower_cores: Vec::new(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut v = || it.next().unwrap_or_else(|| panic!("{k} needs a value"));
        match k.as_str() {
            "--rate" => a.rate = v().parse().unwrap(),
            "--duration" => a.duration = v().parse().unwrap(),
            "--warmup" => a.warmup = v().parse().unwrap(),
            "--senders" => a.senders = v().parse().unwrap(),
            "--payload" => a.payload = v().parse().unwrap(),
            "--pin-senders" => a.pin_senders = v().split(',').map(|c| c.parse().unwrap()).collect(),
            "--pin-receiver" => a.pin_receiver = Some(v().parse().unwrap()),
            "--node-cores" => {
                for (i, c) in v().split(',').enumerate().take(4) {
                    a.node_cores[i] = Some(c.parse().unwrap());
                }
            }
            "--spin" => a.spin = true,
            "--combined" => a.combined = true,
            "--storage" => a.storage = Some(v().into()),
            "--cluster" => a.cluster = v().parse().unwrap(),
            "--sleep-us" => a.sleep_us = Some(v().parse().unwrap()),
            "--leader-cores" => {
                a.leader_cores = v().split(',').map(|c| c.parse().unwrap()).collect()
            }
            "--follower-cores" => {
                a.follower_cores = v()
                    .split('/')
                    .map(|f| {
                        let c: Vec<usize> = f.split(',').map(|c| c.parse().unwrap()).collect();
                        assert!(
                            matches!(c.len(), 3 | 4),
                            "--follower-cores c,repl,o[,store] per follower"
                        );
                        c
                    })
                    .collect()
            }
            "--cluster-cores" => {
                a.cluster_cores = v().split(',').map(|c| c.parse().unwrap()).collect()
            }
            "--durability" => {
                a.durability = match v().as_str() {
                    "a" => krusty_node::Durability::Async,
                    "b" => krusty_node::Durability::Sync,
                    other => panic!("--durability a|b, not {other}"),
                }
            }
            other => panic!("unknown argument {other}"),
        }
    }
    assert!(a.payload >= 24, "--payload must be at least 24");
    if a.combined {
        assert_eq!(a.senders, 1, "--combined needs exactly one sender");
    }
    a
}

struct SenderOut {
    ack: Histogram<u64>,
    feed: Histogram<u64>,
    feed_got: u64,
    late: u64,
    events: u64,
    failed: u64,
    elapsed: Duration,
}

#[allow(clippy::too_many_arguments)]
fn sender(
    k: u64,
    count: u64,
    warmup: u64,
    period_ns: f64,
    start: Instant,
    epoch: Instant,
    ingress: SocketAddrV4,
    payload_len: usize,
    core: Option<usize>,
    feed_sock: Option<UdpSocket>,
) -> SenderOut {
    pin(core);
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_nonblocking(true).unwrap();
    let _ = socket2::SockRef::from(&sock).set_recv_buffer_size(8 << 20);
    let mut session = Session::new(
        SessionConfig {
            client_id: k as u32 + 1,
            window: 64,
            retry_interval_ns: 5_000_000,
            max_attempts: 1000,
        },
        epoch.elapsed().as_nanos() as u64 + 1,
    );
    let due_of = |i: u64| start + Duration::from_nanos((i as f64 * period_ns) as u64);
    let mut out = SenderOut {
        ack: hist(),
        feed: hist(),
        feed_got: 0,
        late: 0,
        events: 0,
        failed: 0,
        elapsed: Duration::ZERO,
    };
    let mut rb = RecvBatch::new(64, 2048);
    let mut sb = SendBatch::new(64, 2048);
    let mut fbb = RecvBatch::new(64, 128);
    let (mut tx, mut ev) = (Vec::with_capacity(128), Vec::with_capacity(128));
    let mut payload = vec![0u8; payload_len];
    let mut submitted = 0u64;
    let feed_target = if feed_sock.is_some() { count } else { 0 };
    // Feed datagrams lost on the way (the generator does not recover them)
    // would otherwise keep it waiting forever; the run is then INVALID.
    let mut drain_until: Option<Instant> = None;
    while out.events < count || out.feed_got < feed_target {
        if submitted == count && out.events == count {
            let d = *drain_until.get_or_insert_with(|| {
                out.elapsed = start.elapsed();
                Instant::now() + Duration::from_secs(2)
            });
            if Instant::now() > d {
                break;
            }
        }
        if let Some(fs) = &feed_sock {
            read_feed(fs, &mut rb, epoch, warmup, &mut out.feed, &mut out.feed_got);
        }
        let now = Instant::now();
        // Submit everything that is due (if the window allows).
        while submitted < count && due_of(submitted) <= now {
            if session.submit(Append::Unconditional, submitted).is_err() {
                break; // window full: the delay counts against latency
            }
            if now - due_of(submitted) > Duration::from_micros(100) {
                out.late += 1;
            }
            submitted += 1;
        }
        let now_ns = (Instant::now() - epoch).as_nanos() as u64;
        session.poll(now_ns, &mut tx, &mut ev);
        for t in tx.drain(..) {
            let due_ns = (due_of(t.handle) - epoch).as_nanos() as u64;
            payload[0..8].copy_from_slice(&due_ns.to_le_bytes());
            payload[8..16].copy_from_slice(&t.handle.to_le_bytes());
            payload[16..24].copy_from_slice(&k.to_le_bytes());
            let s = Submit {
                client_id: k as u32 + 1,
                incarnation: t.incarnation,
                client_seqno: t.client_seqno,
                append: t.append,
                payload: &payload,
            };
            if sb.is_full() {
                let _ = sb.flush(&sock);
            }
            sb.push(ingress, |b| s.encode(b).unwrap());
        }
        if !sb.is_empty() {
            let _ = sb.flush(&sock);
        }
        let n = fbb.recv(&sock).unwrap_or(0);
        for i in 0..n {
            if let Ok(f) = Feedback::decode(fbb.get(i).0) {
                session.on_feedback(&f, now_ns, &mut ev);
            }
        }
        let t = Instant::now();
        for e in ev.drain(..) {
            out.events += 1;
            match e {
                Event::Accepted { handle, .. } => {
                    if handle >= warmup {
                        out.ack
                            .saturating_record((t - due_of(handle)).as_nanos().max(1) as u64);
                    }
                }
                _ => out.failed += 1,
            }
        }
        std::hint::spin_loop();
    }
    if drain_until.is_none() {
        out.elapsed = start.elapsed();
    }
    out
}

fn read_feed(
    sock: &UdpSocket,
    rb: &mut RecvBatch,
    epoch: Instant,
    warmup: u64,
    h: &mut Histogram<u64>,
    got: &mut u64,
) -> usize {
    let n = rb.recv(sock).unwrap_or(0);
    let now = (Instant::now() - epoch).as_nanos() as u64;
    for i in 0..n {
        if let Ok(Frame::Data(d)) = Frame::decode(rb.get(i).0) {
            for m in d.messages() {
                *got += 1;
                let due = u64::from_le_bytes(m.payload[0..8].try_into().unwrap());
                let handle = u64::from_le_bytes(m.payload[8..16].try_into().unwrap());
                if handle >= warmup {
                    h.saturating_record(now.saturating_sub(due).max(1));
                }
            }
        }
    }
    n
}

fn main() {
    let a = parse();
    let feed_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let _ = socket2::SockRef::from(&feed_sock).set_recv_buffer_size(16 << 20);
    feed_sock.set_nonblocking(true).unwrap();
    let mut cfg = NodeConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        v4(feed_sock.local_addr().unwrap()),
        "127.0.0.1:0".parse().unwrap(),
    );
    cfg.cores = a.node_cores;
    if a.spin {
        cfg.idle = Idle::Spin;
    } else if let Some(us) = a.sleep_us {
        cfg.idle = Idle::Sleep { sleep_us: us };
    }
    cfg.core.outcome_ring = 64;
    if let Some(dir) = &a.storage {
        let _ = std::fs::remove_dir_all(dir); // a fresh log per run
        cfg.storage = Some(krusty_node::StorageConfig {
            log: krusty_log::LogConfig::new(dir),
            durability: a.durability,
            snapshot_every: 1_000_000,
            core: None,
        });
    }
    let mut followers = Vec::new();
    let node = if a.cluster == 0 {
        krusty_node::start(cfg).unwrap()
    } else {
        let base = a.storage.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("krusty-load-{}", std::process::id()))
        });
        let _ = std::fs::remove_dir_all(&base);
        let free = || {
            v4(UdpSocket::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap())
        };
        let members: Vec<(u32, SocketAddrV4)> = (1..=a.cluster).map(|id| (id, free())).collect();
        let mut nodes: Vec<krusty_node::Node> = (1..=a.cluster)
            .map(|id| {
                let mut c = cfg.clone();
                c.ingress = free();
                c.recovery = free();
                c.storage = Some(krusty_node::StorageConfig {
                    log: krusty_log::LogConfig::new(base.join(id.to_string())),
                    durability: a.durability,
                    snapshot_every: 1_000_000,
                    core: None,
                });
                let mut cl = krusty_node::ClusterConfig::new(id, members.clone());
                if !a.leader_cores.is_empty() {
                    if id == 1 {
                        let lc = &a.leader_cores;
                        c.cores = [Some(lc[0]), Some(lc[1]), Some(lc[2]), None];
                        cl.core = lc.get(3).copied();
                        if let Some(st) = &mut c.storage {
                            st.core = lc.get(4).copied();
                        }
                        c.idle = Idle::Spin;
                        cl.election_min_us = 20_000;
                        cl.election_max_us = 30_000;
                    } else if let Some(fc) = a.follower_cores.get(id as usize - 2) {
                        c.idle = Idle::Spin;
                        c.cores = [Some(fc[2]), Some(fc[0]), Some(fc[2]), Some(fc[2])];
                        cl.core = Some(fc[1]);
                        if let Some(st) = &mut c.storage {
                            st.core = fc.get(3).copied();
                        }
                    } else {
                        c.idle = Idle::Sleep {
                            sleep_us: a.sleep_us.unwrap_or(50),
                        };
                        if let Some(&core) = a.cluster_cores.get(id as usize - 2) {
                            c.cores = [Some(core); 4];
                            cl.core = Some(core);
                        }
                    }
                } else if let Some(&core) = a.cluster_cores.get(id as usize - 1) {
                    c.cores = [Some(core); 4];
                    cl.core = Some(core);
                }
                c.cluster = Some(cl);
                krusty_node::start(c).unwrap()
            })
            .collect();
        // Wait for a leader, then send to it.
        let deadline = Instant::now() + Duration::from_secs(5);
        let k = loop {
            let l: Vec<usize> = (0..nodes.len())
                .filter(|&k| nodes[k].stats.raft_role.load(Ordering::Relaxed) == 3)
                .collect();
            if l.len() == 1 {
                break l[0];
            }
            assert!(Instant::now() < deadline, "no leader");
            std::thread::sleep(Duration::from_millis(5));
        };
        let leader = nodes.remove(k);
        followers = nodes;
        leader
    };
    let ingress = node.ingress_addr();

    let per_sender = (a.warmup + a.duration) * a.rate / a.senders;
    let warmup = a.warmup * a.rate / a.senders;
    let total = per_sender * a.senders;
    let epoch = Instant::now();
    let start = epoch + Duration::from_millis(300);
    let period_ns = 1e9 * a.senders as f64 / a.rate as f64;

    let sending = Arc::new(AtomicBool::new(true));
    let rx_sending = sending.clone();
    let pin_receiver = a.pin_receiver;
    // --combined: sender 0 also reads the feed (one isolated core does both).
    let mut combined_feed = if a.combined {
        Some(feed_sock.try_clone().unwrap())
    } else {
        None
    };
    let combined = a.combined;
    let receiver = std::thread::spawn(move || {
        if combined {
            return (hist(), 0); // sender 0 reads the feed instead
        }
        pin(pin_receiver);
        let mut rb = RecvBatch::new(64, 2048);
        let mut h = hist();
        let mut got = 0u64;
        let mut idle_since = None::<Instant>;
        while got < total {
            let n = read_feed(&feed_sock, &mut rb, epoch, warmup, &mut h, &mut got);
            if n == 0 {
                if !rx_sending.load(Ordering::Relaxed) {
                    let t = *idle_since.get_or_insert_with(Instant::now);
                    if t.elapsed() > Duration::from_secs(2) {
                        break;
                    }
                }
                std::hint::spin_loop();
                continue;
            }
            idle_since = None;
        }
        (h, got)
    });

    let senders: Vec<_> = (0..a.senders)
        .map(|k| {
            let core = if a.pin_senders.is_empty() {
                None
            } else {
                Some(a.pin_senders[k as usize % a.pin_senders.len()])
            };
            let payload = a.payload;
            let fs = if k == 0 { combined_feed.take() } else { None };
            std::thread::spawn(move || {
                sender(
                    k,
                    per_sender,
                    warmup,
                    period_ns,
                    start + Duration::from_nanos(k * 1_000_000_000 / a.rate),
                    epoch,
                    ingress,
                    payload,
                    core,
                    fs,
                )
            })
        })
        .collect();
    let mut ack = hist();
    let mut feed_c = hist();
    let mut got_c = 0;
    let (mut late, mut failed, mut elapsed) = (0, 0, Duration::ZERO);
    for s in senders {
        let o = s.join().unwrap();
        ack.add(&o.ack).unwrap();
        feed_c.add(&o.feed).unwrap();
        got_c += o.feed_got;
        late += o.late;
        failed += o.failed;
        elapsed = elapsed.max(o.elapsed);
    }
    sending.store(false, Ordering::Relaxed);
    let (feed, got) = if a.combined {
        (feed_c, got_c)
    } else {
        receiver.join().unwrap()
    };
    let busy = node.stats.busy.load(Ordering::Relaxed);
    let leader_terms = node.stats.leader_terms.load(Ordering::Relaxed);
    node.stop();
    for f in followers {
        f.stop();
    }

    let achieved = total as f64 / elapsed.as_secs_f64();
    println!(
        "offered {} msg/s from {} sender(s), {} B payload, {} s (+{} s warm-up); node {}",
        a.rate,
        a.senders,
        a.payload,
        a.duration,
        a.warmup,
        if a.spin {
            "spinning"
        } else {
            "sleeping when idle"
        }
    );
    if a.cluster > 0 {
        println!(
            "replicated: {} nodes, the leader led {} term(s)",
            a.cluster, leader_terms
        );
    }
    println!(
        "achieved {:.0} msg/s; {} submits >100 µs late; feed {} of {}; {} failed; {} BUSY",
        achieved, late, got, total, failed, busy
    );
    let invalid = achieved < 0.98 * a.rate as f64 || got < total || failed > 0;
    if invalid {
        println!("INVALID RUN: the generator fell behind or messages were lost");
    }
    println!("latency from scheduled send, µs:");
    println!(
        "{:<14}{:>9}{:>9}{:>9}{:>9}{:>9}{:>10}",
        "", "p50", "p90", "p99", "p99.9", "p99.99", "max"
    );
    for (name, h) in [("to feed", &feed), ("to ACCEPTED", &ack)] {
        let q = |p: f64| h.value_at_quantile(p) as f64 / 1000.0;
        println!(
            "{:<14}{:>9.1}{:>9.1}{:>9.1}{:>9.1}{:>9.1}{:>10.1}",
            name,
            q(0.5),
            q(0.9),
            q(0.99),
            q(0.999),
            q(0.9999),
            h.max() as f64 / 1000.0
        );
    }
    if invalid {
        std::process::exit(1);
    }
}
