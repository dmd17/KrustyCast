//! Open-loop load generator for krusty-node (successor of `loadgen`).
//!
//! Each sender is a krusty-session client submitting on a fixed schedule;
//! latency is measured from each message's *scheduled* time (no coordinated
//! omission) to (a) its arrival on the feed and (b) its ACCEPTED. A run is
//! INVALID (exit 1) if the generator could not keep up or anything was lost.
//!
//!   krusty-load [--rate N] [--duration S] [--warmup S] [--senders K] [--payload B]
//!               [--pin-senders a,b] [--pin-receiver c] [--node-cores i,c,e,r] [--spin]
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
    while out.events < count || out.feed_got < feed_target {
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
    out.elapsed = start.elapsed();
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
    }
    cfg.core.outcome_ring = 64;
    let node = krusty_node::start(cfg).unwrap();
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
    node.stop();

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
