//! Operates running nodes through their admin endpoints (gap O3).
//!
//!   krusty-admin [--node ip:port]... [--raw] status
//!   krusty-admin [--node ip:port]...         snapshot
//!
//! `--node` names an admin endpoint (`admin =` in the node's config) and
//! may repeat; the default is 127.0.0.1:7010. `status` prints one row per
//! node (or every field with `--raw`); `snapshot` asks each node for a core
//! snapshot now. Exits non-zero if any node fails or does not answer.
use krusty_node::admin::request;
use std::net::SocketAddrV4;
use std::process::exit;
use std::time::Duration;

fn usage() -> ! {
    eprintln!("usage: krusty-admin [--node ip:port]... [--raw] (status | snapshot)");
    exit(2)
}

fn field<'a>(kv: &'a [(String, String)], k: &str) -> &'a str {
    kv.iter()
        .find(|(a, _)| a == k)
        .map_or("-", |(_, v)| v.as_str())
}

fn main() {
    let mut nodes: Vec<SocketAddrV4> = Vec::new();
    let mut raw = false;
    let mut cmd = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--node" => match args.next().and_then(|v| v.parse().ok()) {
                Some(n) => nodes.push(n),
                None => usage(),
            },
            "--raw" => raw = true,
            "status" | "snapshot" if cmd.is_none() => cmd = Some(a),
            _ => usage(),
        }
    }
    let Some(cmd) = cmd else { usage() };
    if nodes.is_empty() {
        nodes.push("127.0.0.1:7010".parse().unwrap());
    }
    let mut failed = false;
    match cmd.as_str() {
        "status" => {
            if !raw {
                println!(
                    "{:<21} {:>4} {:<13} {:>5} {:>6} {:>10} {:>10} {:>8} {:>10} {:>10} {:>7} {:>8}",
                    "NODE",
                    "ID",
                    "ROLE",
                    "TERM",
                    "LEADER",
                    "COMMIT",
                    "APPLIED",
                    "STREAM",
                    "NEXT",
                    "SNAPSHOT",
                    "CLIENTS",
                    "UPTIME"
                );
            }
            for n in &nodes {
                match request(*n, "status", Duration::from_secs(1)) {
                    Ok(kv) if raw => {
                        println!("# {n}");
                        for (k, v) in &kv {
                            println!("{k} {v}");
                        }
                    }
                    Ok(kv) => println!(
                        "{:<21} {:>4} {:<13} {:>5} {:>6} {:>10} {:>10} {:>8} {:>10} {:>10} {:>7} {:>7}s",
                        n.to_string(),
                        field(&kv, "node"),
                        field(&kv, "role"),
                        field(&kv, "term"),
                        field(&kv, "leader"),
                        field(&kv, "commit"),
                        field(&kv, "applied"),
                        field(&kv, "stream"),
                        field(&kv, "next_seqno"),
                        field(&kv, "snapshot_index"),
                        field(&kv, "clients"),
                        field(&kv, "uptime_s"),
                    ),
                    Err(e) => {
                        failed = true;
                        println!("{:<21} {e}", n.to_string());
                    }
                }
            }
        }
        "snapshot" => {
            for n in &nodes {
                match request(*n, "snapshot", Duration::from_secs(10)) {
                    Ok(kv) => println!(
                        "{n}: snapshot at log index {}",
                        field(&kv, "snapshot_index")
                    ),
                    Err(e) => {
                        failed = true;
                        println!("{n}: {e}");
                    }
                }
            }
        }
        _ => unreachable!(),
    }
    if failed {
        exit(1);
    }
}
