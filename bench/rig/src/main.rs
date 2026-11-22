//! One open-loop load generator, in Rust, for Aeron Cluster and KrustyCast
//! (docs/BASELINE.md E3). It replaces aeron-io/benchmarks' Java
//! `LoadTestRig` for both, so neither client side runs a JVM. It keeps
//! that rig's method so results stay comparable:
//! - messages are due on a fixed schedule (one per send, `--rate` a
//!   second), each stamped with its scheduled time; latency is from then
//!   to its answer (Aeron's echo, KrustyCast's ACCEPTED);
//! - a warm-up of `--warmup` seconds in 10 chunks, the histogram reset
//!   after each, then one measured run of `--duration` seconds; 100 ms of
//!   grace to send what is due, 3 s to receive what was sent;
//! - output: the HdrHistogram percentile distribution in µs, in the
//!   format the Java rig prints, and its warning when the rate was not met.
//!
//!   e3-rig aeron  --rate R --duration S --warmup W [--size 64] [--rig-cpu 15]
//!                 [--aeron-dir D] [--ingress CHANNEL] [--egress CHANNEL]
//!   e3-rig krusty --rate R --duration S --warmup W [--size 64] [--rig-cpu 15]
//!                 --nodes a:p,b:p,c:p [--io-cpu 7] [--mtu 1408] [--window 1024] [--retry-ms 50]
//!
//! Aeron: the rig thread uses Aeron's C client (the media driver is a
//! separate `aeronmd` process, pinned by the caller), and speaks the
//! Cluster client protocol itself (connect, session messages, keep-alive).
//! KrustyCast: the rig thread runs the client session (`krusty-session`);
//! an I/O thread, standing where Aeron's media driver stands, packs
//! SUBMITs into datagrams and receives outcomes.
mod aeron;
mod aeron_c;
mod krusty;

use hdrhistogram::Histogram;
use std::time::Instant;

/// What the rig drives.
pub trait Transceiver {
    /// Sends up to `n` messages stamped `ts` (ns on the rig's clock);
    /// returns how many went (fewer under back pressure).
    fn send(&mut self, n: usize, ts: u64) -> usize;
    /// Polls once; calls `got(ts)` for each message answered.
    fn receive(&mut self, got: &mut dyn FnMut(u64));
}

/// The value at the end of every payload (the Java rig checks it too).
pub const CHECKSUM: u64 = 0x4B43_5249_4745_3352; // "KCRIGE3R"

pub fn now_ns(epoch: Instant) -> u64 {
    epoch.elapsed().as_nanos() as u64
}

pub fn pin(cpu: Option<usize>) {
    let Some(c) = cpu else { return };
    // SAFETY: a zeroed cpu_set_t is valid; sched_setaffinity(0) is this thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(c, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

struct Result {
    sent: u64,
    received: u64,
}

/// LoadTestRig.send: `secs` seconds at `rate`, one message per send.
fn run(
    t: &mut dyn Transceiver,
    epoch: Instant,
    rate: u64,
    secs_ns: u64,
    hist: &mut Histogram<u64>,
) -> Result {
    let interval = 1_000_000_000 / rate;
    let total = secs_ns * rate / 1_000_000_000;
    let start = now_ns(epoch);
    let stop = start + secs_ns;
    let deadline = stop + 100_000_000;
    let mut sent = 0u64;
    let received = std::cell::Cell::new(0u64);
    let mut ts = start;
    let mut got = |sent_at: u64| {
        let lat = now_ns(epoch).saturating_sub(sent_at);
        hist.saturating_record(lat.max(1));
        received.set(received.get() + 1);
    };
    while sent < total {
        let n = t.send(1, ts) as u64;
        sent += n;
        if sent == total {
            break;
        }
        let mut now = now_ns(epoch);
        if n == 1 {
            ts += interval;
            while now < ts && now < stop {
                t.receive(&mut got);
                now = now_ns(epoch);
            }
        } else {
            t.receive(&mut got);
        }
        if now >= deadline {
            break;
        }
        if now >= stop {
            t.receive(&mut got);
        }
    }
    let until = now_ns(epoch) + 3_000_000_000;
    while received.get() < sent && now_ns(epoch) < until {
        t.receive(&mut got);
    }
    Result {
        sent,
        received: received.get(),
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let system = args.next().expect("aeron | krusty");
    let mut rate = 100_000u64;
    let mut duration = 60u64;
    let mut warmup = 10u64;
    let mut size = 64usize;
    let mut rig_cpu = None;
    let mut io_cpu = None;
    let mut aeron_dir = "/dev/shm/kb-aeron-client".to_string();
    let mut ingress = "aeron:udp?endpoint=localhost:20000".to_string();
    let mut egress = "aeron:udp?endpoint=localhost:24000".to_string();
    let mut nodes = Vec::new();
    let mut mtu = 1408usize;
    let mut window = 1024usize;
    let mut retry_ms = 50u64;
    while let Some(k) = args.next() {
        let mut v = || args.next().unwrap_or_else(|| panic!("{k} needs a value"));
        match k.as_str() {
            "--rate" => rate = v().parse().unwrap(),
            "--duration" => duration = v().parse().unwrap(),
            "--warmup" => warmup = v().parse().unwrap(),
            "--size" => size = v().parse().unwrap(),
            "--rig-cpu" => rig_cpu = Some(v().parse().unwrap()),
            "--io-cpu" => io_cpu = Some(v().parse().unwrap()),
            "--aeron-dir" => aeron_dir = v(),
            "--ingress" => ingress = v(),
            "--egress" => egress = v(),
            "--nodes" => nodes = v().split(',').map(|a| a.parse().unwrap()).collect(),
            "--mtu" => mtu = v().parse().unwrap(),
            "--window" => window = v().parse().unwrap(),
            "--retry-ms" => retry_ms = v().parse().unwrap(),
            other => panic!("unknown argument {other}"),
        }
    }
    assert!(size >= 16, "--size: at least 16 (timestamp and checksum)");
    let epoch = Instant::now();
    // The transport's threads (Aeron's client conductor, KrustyCast's I/O
    // thread) are created first, with the process's CPUs; then this thread
    // moves to its own, as the Java rig does.
    let mut t: Box<dyn Transceiver> = match system.as_str() {
        "aeron" => Box::new(aeron::AeronCluster::connect(
            &aeron_dir, &ingress, &egress, size, epoch,
        )),
        "krusty" => Box::new(krusty::Krusty::start(
            nodes, size, mtu, window, retry_ms, io_cpu, epoch,
        )),
        other => panic!("system: aeron | krusty, not {other}"),
    };
    pin(rig_cpu);
    let mut hist = Histogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
    println!(
        "Running warmup for {warmup} s at {rate} msg/s, {size} B payload, in 10 chunks ({system})..."
    );
    for _ in 0..10 {
        run(t.as_mut(), epoch, rate, warmup * 100_000_000, &mut hist);
        hist.reset();
    }
    println!("Running measurement for {duration} s at {rate} msg/s...");
    let r = run(t.as_mut(), epoch, rate, duration * 1_000_000_000, &mut hist);
    println!("\nHistogram of RTT latencies in MICROSECONDS.");
    print_distribution(&hist);
    let expected = duration * rate;
    if r.sent != expected {
        println!(
            "\n*** WARNING: Target message rate not achieved: expected to send {expected} messages in total but managed to send only {} messages (loss {:.4}%)!",
            r.sent,
            100.0 - 100.0 * r.sent as f64 / expected as f64
        );
    }
    if r.received != r.sent {
        println!(
            "\n*** WARNING: Not all messages were received after 3s deadline: expected {} vs received {}!",
            r.sent, r.received
        );
    }
    drop(t);
}

/// As HdrHistogram's outputPercentileDistribution (5 ticks per half
/// distance), values in µs.
fn print_distribution(h: &Histogram<u64>) {
    println!(
        "{:>12} {:>14} {:>10} {:>14}\n",
        "Value", "Percentile", "TotalCount", "1/(1-Percentile)"
    );
    let mut total = 0u64;
    for v in h.iter_quantiles(5) {
        total += v.count_since_last_iteration();
        let value = h.highest_equivalent(v.value_iterated_to()) as f64 / 1000.0;
        let q = v.quantile_iterated_to();
        if q < 1.0 {
            println!("{value:12.3} {q:2.12} {total:10} {:14.2}", 1.0 / (1.0 - q));
        } else {
            println!("{value:12.3} {q:2.12} {total:10}");
        }
    }
    println!(
        "#[Mean    = {:12.3}, StdDeviation   = {:12.3}]",
        h.mean() / 1000.0,
        h.stdev() / 1000.0
    );
    println!(
        "#[Max     = {:12.3}, Total count    = {:12}]",
        h.max() as f64 / 1000.0,
        h.len()
    );
}
