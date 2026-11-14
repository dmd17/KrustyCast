//! Reads a running node's shared-memory metrics (gap O2).
//!
//!   krusty-stat [--file /dev/shm/krusty-1.stats] [--watch SECONDS]
//!
//! Counters are printed with their rate over the interval; histograms as
//! approximate quantiles (upper bounds of power-of-two buckets).
use krusty_node::metrics::{Metrics, quantile};
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn fmt_ns(ns: Option<u64>) -> String {
    match ns {
        None => "-".into(),
        Some(n) if n < 1_000 => format!("{n}ns"),
        Some(n) if n < 1_000_000 => format!("{:.1}µs", n as f64 / 1e3),
        Some(n) => format!("{:.1}ms", n as f64 / 1e6),
    }
}

fn main() {
    let mut file = PathBuf::from("/dev/shm/krusty-1.stats");
    let mut watch: Option<u64> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--file" => file = args.next().expect("--file PATH").into(),
            "--watch" => {
                watch = Some(
                    args.next()
                        .expect("--watch SECONDS")
                        .parse()
                        .expect("seconds"),
                )
            }
            other => panic!("unknown argument {other}"),
        }
    }
    let m = Metrics::open(&file).unwrap_or_else(|e| {
        eprintln!("error: {}: {e}", file.display());
        std::process::exit(2);
    });
    let mut prev = m.counters();
    let mut t = Instant::now();
    loop {
        let interval = match watch {
            Some(s) => {
                std::thread::sleep(Duration::from_secs(s));
                Some(t.elapsed().as_secs_f64())
            }
            None => None,
        };
        t = Instant::now();
        let now = m.counters();
        println!(
            "{} (pid {}, stream {})",
            file.display(),
            m.pid.load(std::sync::atomic::Ordering::Relaxed),
            m.stream_id.load(std::sync::atomic::Ordering::Relaxed)
        );
        for ((name, v), (_, p)) in now.iter().zip(&prev) {
            match interval {
                Some(dt) => println!("  {name:<18} {v:>14} {:>12.0}/s", (v - p) as f64 / dt),
                None => println!("  {name:<18} {v:>14}"),
            }
        }
        println!(
            "  {:<18} {:>9} {:>9} {:>9} {:>9} {:>9}",
            "histogram", "count", "p50", "p99", "p99.99", "max"
        );
        for (name, h) in m.histograms() {
            let b = h.snapshot();
            let count: u64 = b.iter().sum();
            println!(
                "  {name:<18} {count:>9} {:>9} {:>9} {:>9} {:>9}",
                fmt_ns(quantile(&b, 0.5)),
                fmt_ns(quantile(&b, 0.99)),
                fmt_ns(quantile(&b, 0.9999)),
                fmt_ns(quantile(&b, 1.0))
            );
        }
        prev = now;
        if watch.is_none() {
            return;
        }
    }
}
