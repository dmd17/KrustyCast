//! Device fsync latency: append a small record and fdatasync, repeatedly
//! (the floor for durability mode (b), PRD NFR-9).
//!
//!   cargo run --release -p krusty-log --example fsync -- DIR [COUNT]
use krusty_log::{Log, LogConfig};
use std::time::Instant;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("DIR");
    let count: usize = args.next().map(|c| c.parse().unwrap()).unwrap_or(5000);
    let _ = std::fs::remove_dir_all(&dir);
    let mut log = Log::open(LogConfig::new(&dir)).unwrap().log;
    let payload = [7u8; 112]; // a 64-byte message as a SUBMIT
    let mut ns: Vec<u64> = Vec::with_capacity(count);
    for i in 0..count {
        let t = Instant::now();
        log.append(i as u64, &payload).unwrap();
        log.sync().unwrap();
        ns.push(t.elapsed().as_nanos() as u64);
    }
    ns.sort_unstable();
    let q = |p: f64| ns[((ns.len() - 1) as f64 * p) as usize] as f64 / 1000.0;
    println!(
        "append + fdatasync, {count} times, µs: p50 {:.1}  p90 {:.1}  p99 {:.1}  p99.9 {:.1}  max {:.1}",
        q(0.5),
        q(0.9),
        q(0.99),
        q(0.999),
        ns[ns.len() - 1] as f64 / 1000.0
    );
    drop(log);
    std::fs::remove_dir_all(&dir).unwrap();
}
