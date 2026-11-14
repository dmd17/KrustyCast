//! How long a node takes to come back: builds a log of N entries (with
//! krusty-core and krusty-log directly), then times `krusty_node::start`,
//! which restores (snapshot + replay of the suffix) before it returns.
//!
//!   cargo run --release -p krusty-node --example restore_time -- DIR ENTRIES SNAPSHOT_AT
//!
//! SNAPSHOT_AT = 0 means no snapshot (replay everything).
use krusty_core::Core;
use krusty_log::{Log, LogConfig};
use krusty_node::{Durability, NodeConfig, StorageConfig};
use krusty_wire::ingress::{Append, Submit};
use std::time::Instant;

fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let dir = std::path::PathBuf::from(&a[0]);
    let n: u64 = a[1].parse().unwrap();
    let snap_at: u64 = a[2].parse().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = NodeConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:9".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    );
    let st = StorageConfig {
        log: LogConfig::new(&dir),
        durability: Durability::Async,
        snapshot_every: u64::MAX,
    };
    cfg.storage = Some(st.clone());

    let t = Instant::now();
    let mut log = Log::open(st.log.clone()).unwrap().log;
    let mut core = Core::new(cfg.core);
    let payload = [5u8; 64];
    let mut buf = [0u8; 256];
    let mut next = [0u64; 100];
    for i in 1..=n {
        let c = (i % 100) as u32;
        next[c as usize] += 1;
        let s = Submit {
            client_id: c,
            incarnation: 1,
            client_seqno: next[c as usize],
            append: Append::Unconditional,
            payload: &payload,
        };
        let k = s.encode(&mut buf).unwrap();
        let e = core.apply(i, i * 1000, &s).unwrap();
        let pos = log.append(i * 1000, &buf[..k]).unwrap();
        if let Some(p) = e.publish {
            log.index_published(p.seqno, pos);
        }
        if i == snap_at {
            let mut b = Vec::new();
            core.snapshot(&mut b);
            log.write_snapshot(i, &b).unwrap();
        }
    }
    log.sync().unwrap();
    drop(log);
    let bytes: u64 = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum();
    println!(
        "built {n} entries ({:.0} MiB on disk) in {:.1} s",
        bytes as f64 / (1 << 20) as f64,
        t.elapsed().as_secs_f64()
    );

    let t = Instant::now();
    let o = Log::open(st.log.clone()).unwrap();
    println!("  Log::open alone: {:.3} s", t.elapsed().as_secs_f64());
    drop(o);
    let t = Instant::now();
    let h = krusty_ring::history::History::new(cfg.history, 32 + cfg.max_payload);
    println!("  History::new alone: {:.3} s", t.elapsed().as_secs_f64());
    drop(h);
    let t = Instant::now();
    let node = krusty_node::start(cfg).unwrap();
    let restore = t.elapsed();
    node.stop();
    println!(
        "restart with snapshot at {snap_at}: replayed {} entries, ready in {:.3} s",
        n - snap_at,
        restore.as_secs_f64()
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
