//! Open-loop load generator: submit → feed latency (GAP-ANALYSIS E1).
//!
//! Sends client messages on a fixed schedule (message *i* is due at
//! `start + i / rate`) and listens on the multicast feed. Latency is measured
//! from each message's *scheduled* send time to its arrival on the feed, so
//! when the sender or the system stalls, the queueing delay is counted
//! (no coordinated omission). The latency from the *actual* send time is
//! reported alongside to show the difference.
//!
//! By default it starts a sequencer in-process (loopback); `--sequencer`
//! points it at an external one instead.
//!
//! Numbers from a laptop are for regression tracking only; PRD §7 targets
//! need the reference servers.

use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use krustycast::journal::FsyncPolicy;
use krustycast::net;
use krustycast::protocol::{
    ClientMessage, Feedback, MoldKind, MoldPacket, SequencedMessage, Session,
};
use krustycast::sequencer::{self, SequencerConfig};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Fsync {
    Batch,
    Never,
}

#[derive(Parser, Debug)]
#[command(name = "loadgen", about = "Open-loop submit-to-feed latency benchmark")]
struct Args {
    /// Messages per second (offered load)
    #[arg(long, default_value_t = 100_000)]
    rate: u64,

    /// Measured seconds (after warm-up)
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Warm-up seconds (sent and received, not recorded)
    #[arg(long, default_value_t = 2)]
    warmup: u64,

    /// Payload bytes per message (at least 16: two timestamps)
    #[arg(long, default_value_t = 64)]
    payload: usize,

    /// External sequencer input address. Without it, a sequencer runs in-process.
    #[arg(long)]
    sequencer: Option<SocketAddrV4>,

    /// Multicast group of the feed
    #[arg(long, default_value = "239.77.1.1:17201")]
    feed: SocketAddrV4,

    /// Local interface address for multicast
    #[arg(long, default_value = "0.0.0.0")]
    interface: Ipv4Addr,

    /// In-process sequencer only: journal file (none = in memory)
    #[arg(long)]
    journal: Option<PathBuf>,

    /// In-process sequencer only: when the journal is synced
    #[arg(long, value_enum, default_value_t = Fsync::Batch)]
    fsync: Fsync,

    /// Sender threads, each a separate client with its share of the rate
    #[arg(long, default_value_t = 1)]
    senders: u64,

    /// Pin sender threads to these cores (comma-separated, used round-robin)
    #[arg(long, value_delimiter = ',')]
    pin_sender: Vec<usize>,

    /// Pin the feed receiver thread to this core
    #[arg(long)]
    pin_receiver: Option<usize>,

    /// Client id of the first sender (the others follow)
    #[arg(long, default_value_t = 4242)]
    sender_id: u32,
}

fn pin(core: Option<usize>) {
    if let Some(core) = core {
        // SAFETY: cpu_set_t is plain data; sched_setaffinity reads it.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(core, &mut set);
            if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) != 0 {
                eprintln!("warning: could not pin to core {core}");
            }
        }
    }
}

/// Waits until `deadline`: sleeps while far away, spins for the last stretch.
fn wait_until(deadline: Instant) {
    loop {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        let left = deadline - now;
        if left > Duration::from_micros(200) {
            std::thread::sleep(left - Duration::from_micros(100));
        } else {
            std::hint::spin_loop();
        }
    }
}

struct Stats {
    /// From scheduled send time (coordinated-omission correct).
    scheduled: Histogram<u64>,
    /// From actual send time (what a closed-loop tool would report).
    actual: Histogram<u64>,
    received: u64,
    gaps: u64,
}

fn new_hist() -> Histogram<u64> {
    // 1 ns .. 100 s, 3 significant digits.
    Histogram::new_with_bounds(1, 100_000_000_000, 3).unwrap()
}

struct SenderSetup {
    sender_id: u32,
    target: SocketAddr,
    count: u64,
    start: Instant,
    period_ns: f64,
    payload: usize,
    epoch: Instant,
    pin: Option<usize>,
}

#[derive(Default)]
struct SenderResult {
    elapsed: Duration,
    late: u64,
    resent: u64,
    naks: u64,
    accepted: u64,
    other: u64,
}

/// One open-loop client: sends `count` messages on schedule, going back to
/// the NAKed seqno when the sequencer reports a gap.
fn run_sender(s: SenderSetup) -> std::io::Result<SenderResult> {
    // Not connected: feedback comes from the sequencer's output socket,
    // whose address differs from `target`.
    let sock = UdpSocket::bind("127.0.0.1:0")?;
    socket2::SockRef::from(&sock).set_recv_buffer_size(16 << 20)?;
    let fb_sock = sock.try_clone()?;
    fb_sock.set_read_timeout(Some(Duration::from_millis(100)))?;
    // Latest NAK seen: resend from this seqno (0 = none).
    let nak_from = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let (fb_nak, fb_done) = (nak_from.clone(), done.clone());
    let feedback = std::thread::spawn(move || {
        let (mut accepted, mut naks, mut other) = (0u64, 0u64, 0u64);
        let mut buf = [0u8; 64];
        let mut quiet = 0;
        loop {
            match fb_sock.recv(&mut buf) {
                Ok(n) => match Feedback::decode(&buf[..n]) {
                    Some(Feedback::Accepted { .. }) => accepted += 1,
                    Some(Feedback::Nak { expected }) => {
                        naks += 1;
                        fb_nak.store(expected, Ordering::Relaxed);
                    }
                    _ => other += 1,
                },
                Err(_) if fb_done.load(Ordering::Relaxed) => {
                    quiet += 1;
                    if quiet > 10 {
                        break;
                    }
                }
                Err(_) => {}
            }
        }
        (accepted, naks, other)
    });

    pin(s.pin);
    let incarnation = s.epoch.elapsed().as_nanos() as u64
        ^ (u64::from(std::process::id()) << 32)
        ^ u64::from(s.sender_id);
    let mut payload = vec![0u8; s.payload];
    let mut msg = ClientMessage {
        sender_id: s.sender_id,
        incarnation,
        sender_seqno: 0,
        target_id: s.sender_id,
        target_seq: 0,
        payload: Vec::new(),
    };
    let due_of = |i: u64| s.start + Duration::from_nanos((i as f64 * s.period_ns) as u64);
    let mut send = |i: u64, msg: &mut ClientMessage| -> std::io::Result<()> {
        // Message i (seqno i+1) always carries its *scheduled* time, also
        // when resent, so recovery delay shows up in the latency.
        let due_ns = (due_of(i) - s.epoch).as_nanos() as u64;
        let now_ns = s.epoch.elapsed().as_nanos() as u64;
        payload[0..8].copy_from_slice(&due_ns.to_le_bytes());
        payload[8..16].copy_from_slice(&now_ns.to_le_bytes());
        msg.sender_seqno = i + 1;
        msg.target_seq = i + 1;
        msg.payload.clone_from(&payload);
        sock.send_to(&msg.encode(), s.target)?;
        Ok(())
    };
    let mut r = SenderResult::default();
    let (mut last_nak, mut last_resend) = (0u64, s.start);
    let mut i = 0;
    while i < s.count {
        // Go back N on a NAK, like the real client: the sequencer NAKs
        // every message after a gap, so act on a given NAK once per 1 ms.
        let nak = nak_from.load(Ordering::Relaxed);
        if nak != 0
            && nak <= i
            && (nak != last_nak || last_resend.elapsed() > Duration::from_millis(1))
        {
            for j in (nak - 1)..i {
                send(j, &mut msg)?;
                r.resent += 1;
            }
            last_nak = nak;
            last_resend = Instant::now();
        }
        let due = due_of(i);
        wait_until(due);
        if due.elapsed() > Duration::from_micros(100) {
            r.late += 1;
        }
        send(i, &mut msg)?;
        i += 1;
    }
    r.elapsed = s.start.elapsed();
    done.store(true, Ordering::Relaxed);
    (r.accepted, r.naks, r.other) = feedback.join().unwrap();
    Ok(r)
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    assert!(args.payload >= 16, "--payload must be at least 16");
    assert!(args.rate > 0);

    let seq_handle;
    let target: SocketAddr = match args.sequencer {
        Some(a) => {
            seq_handle = None;
            a.into()
        }
        None => {
            let h = sequencer::start(SequencerConfig {
                input_addr: "127.0.0.1:0".parse().unwrap(),
                output_addr: args.feed,
                rerequest_addr: "127.0.0.1:0".parse().unwrap(),
                interface: args.interface,
                heartbeat_interval: Duration::from_millis(100),
                end_of_session_linger: Duration::from_millis(100),
                journal: args.journal.clone(),
                fsync: match args.fsync {
                    Fsync::Batch => FsyncPolicy::EveryBatch,
                    Fsync::Never => FsyncPolicy::Never,
                },
                ..SequencerConfig::new(Session::new("LOADGEN").unwrap())
            })?;
            let a = h.input_local_addr();
            seq_handle = Some(h);
            a
        }
    };

    let feed = net::bind_receiver(args.feed, args.interface)?;
    socket2::SockRef::from(&feed).set_recv_buffer_size(16 << 20)?;
    feed.set_read_timeout(Some(Duration::from_millis(100)))?;

    let senders = args.senders.max(1);
    let per_sender = (args.warmup + args.duration) * args.rate / senders;
    let total = per_sender * senders;
    let warmup_per_sender = args.warmup * args.rate / senders;
    let epoch = Instant::now();
    let start = epoch + Duration::from_millis(200);
    let sending = Arc::new(AtomicBool::new(true));

    // Feed receiver.
    let rx_sending = sending.clone();
    let first_id = args.sender_id;
    let ids = first_id..first_id + senders as u32;
    let pin_receiver = args.pin_receiver;
    let receiver = std::thread::spawn(move || {
        pin(pin_receiver);
        let mut st = Stats {
            scheduled: new_hist(),
            actual: new_hist(),
            received: 0,
            gaps: 0,
        };
        let mut next_seqno: Option<u64> = None;
        let mut buf = vec![0u8; 65536];
        let mut idle_since: Option<Instant> = None;
        while st.received < total {
            let n = match feed.recv(&mut buf) {
                Ok(n) => n,
                Err(_) => {
                    if !rx_sending.load(Ordering::Relaxed) {
                        let t = *idle_since.get_or_insert_with(Instant::now);
                        if t.elapsed() > Duration::from_secs(1) {
                            break;
                        }
                    }
                    continue;
                }
            };
            let now_ns = epoch.elapsed().as_nanos() as u64;
            let Some(p) = MoldPacket::decode(&buf[..n]) else {
                continue;
            };
            let MoldKind::Messages(msgs) = p.kind else {
                continue;
            };
            if let Some(expect) = next_seqno
                && p.seqno > expect
            {
                st.gaps += p.seqno - expect;
            }
            next_seqno = Some(p.seqno + msgs.len() as u64);
            for m in msgs {
                let Some(sm) = SequencedMessage::decode(&m) else {
                    continue;
                };
                if !ids.contains(&sm.sender_id) || sm.payload.len() < 16 {
                    continue;
                }
                let index = sm.target_seq - 1;
                st.received += 1;
                // Only our own messages count as activity (heartbeats don't).
                idle_since = None;
                if index < warmup_per_sender {
                    continue;
                }
                let scheduled = u64::from_le_bytes(sm.payload[0..8].try_into().unwrap());
                let actual = u64::from_le_bytes(sm.payload[8..16].try_into().unwrap());
                st.scheduled
                    .saturating_record(now_ns.saturating_sub(scheduled).max(1));
                st.actual
                    .saturating_record(now_ns.saturating_sub(actual).max(1));
            }
        }
        st
    });

    let pins: Vec<usize> = args.pin_sender.clone();
    let mut handles = Vec::new();
    for k in 0..senders {
        let s = SenderSetup {
            sender_id: first_id + k as u32,
            target,
            count: per_sender,
            // Senders interleave: sender k is offset by k periods of the total rate.
            start: start + Duration::from_nanos(k * 1_000_000_000 / args.rate),
            period_ns: 1_000_000_000f64 * senders as f64 / args.rate as f64,
            payload: args.payload,
            epoch,
            pin: if pins.is_empty() {
                None
            } else {
                Some(pins[k as usize % pins.len()])
            },
        };
        handles.push(std::thread::spawn(move || run_sender(s)));
    }
    let mut sum = SenderResult::default();
    for h in handles {
        let r = h.join().unwrap()?;
        sum.late += r.late;
        sum.resent += r.resent;
        sum.naks += r.naks;
        sum.accepted += r.accepted;
        sum.other += r.other;
        sum.elapsed = sum.elapsed.max(r.elapsed);
    }
    let send_elapsed = sum.elapsed;
    let (behind, resent, naks, accepted, other) =
        (sum.late, sum.resent, sum.naks, sum.accepted, sum.other);
    sending.store(false, Ordering::Relaxed);

    let st = receiver.join().unwrap();
    if let Some(h) = seq_handle {
        h.shutdown();
    }

    let measured = total - warmup_per_sender * senders;
    println!(
        "offered {} msg/s from {} sender(s) for {} s (+{} s warm-up), payload {} B, {}",
        args.rate,
        senders,
        args.duration,
        args.warmup,
        args.payload,
        if args.sequencer.is_some() {
            "external sequencer"
        } else {
            "in-process sequencer"
        }
    );
    println!(
        "sent {} in {:.2} s ({:.0} msg/s achieved); {} sends >100 µs late; {} resent",
        total,
        send_elapsed.as_secs_f64(),
        total as f64 / send_elapsed.as_secs_f64(),
        behind,
        resent
    );
    let achieved = total as f64 / send_elapsed.as_secs_f64();
    let invalid = achieved < 0.98 * args.rate as f64;
    if invalid {
        println!(
            "INVALID RUN: the generator only sustained {:.0} of {} msg/s; the latencies below \
             include its own lag (add --senders)",
            achieved, args.rate
        );
    }
    println!(
        "feed: received {} of {} ({} missing, {} seqno gaps); feedback: {} NAK, {} accepted, {} other",
        st.received,
        total,
        total - st.received.min(total),
        st.gaps,
        naks,
        accepted,
        other
    );
    println!("latency over {} measured messages, µs:", measured);
    println!(
        "{:<22}{:>9}{:>9}{:>9}{:>9}{:>9}{:>10}",
        "", "p50", "p90", "p99", "p99.9", "p99.99", "max"
    );
    for (name, h) in [
        ("from scheduled send", &st.scheduled),
        ("from actual send", &st.actual),
    ] {
        let q = |p: f64| h.value_at_quantile(p) as f64 / 1000.0;
        println!(
            "{:<22}{:>9.1}{:>9.1}{:>9.1}{:>9.1}{:>9.1}{:>10.1}",
            name,
            q(0.5),
            q(0.9),
            q(0.99),
            q(0.999),
            q(0.9999),
            h.max() as f64 / 1000.0
        );
    }
    if st.received < total || invalid {
        std::process::exit(1);
    }
    Ok(())
}
