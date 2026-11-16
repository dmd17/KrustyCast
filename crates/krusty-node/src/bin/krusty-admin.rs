//! Operates running nodes through their admin endpoints (gap O3).
//!
//!   krusty-admin [--node ip:port]... [--raw] status
//!   krusty-admin [--node ip:port]...         snapshot
//!   krusty-admin [--node ip:port]...         transfer ID
//!   krusty-admin [--node ip:port]...         roll [STREAM]
//!
//! `--node` names an admin endpoint (`admin =` in the node's config) and
//! may repeat; the default is 127.0.0.1:7010. `status` prints one row per
//! node (or every field with `--raw`); `snapshot` asks each node for a core
//! snapshot now; `transfer` finds the leader among the nodes named and asks
//! it to hand leadership to member ID. Exits non-zero if any node fails or
//! does not answer. `roll` asks the leader for a session roll: END_OF_SESSION
//! on the current stream, then stream STREAM (default: current + 1) from
//! seqno 1 with a fresh core; clients must start new incarnations.
use krusty_node::admin::request;
use std::net::SocketAddrV4;
use std::process::exit;
use std::time::Duration;

fn usage() -> ! {
    eprintln!(
        "usage: krusty-admin [--node ip:port]... [--raw] (status | snapshot | transfer ID | roll [STREAM])"
    );
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
    let mut target: Option<u32> = None;
    let mut stream: Option<u64> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--node" => match args.next().and_then(|v| v.parse().ok()) {
                Some(n) => nodes.push(n),
                None => usage(),
            },
            "--raw" => raw = true,
            "status" | "snapshot" if cmd.is_none() => cmd = Some(a),
            "roll" if cmd.is_none() => {
                cmd = Some(a);
                if let Some(v) = args.next() {
                    if v == "--node" {
                        usage(); // flags go before the command
                    }
                    stream = Some(v.parse().unwrap_or_else(|_| usage()));
                }
            }
            "transfer" if cmd.is_none() => {
                target = Some(
                    args.next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
                cmd = Some(a);
            }
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
        "transfer" => {
            let to = target.unwrap();
            let leader = nodes.iter().find(|n| {
                request(**n, "status", Duration::from_secs(1))
                    .is_ok_and(|kv| field(&kv, "role") == "leader")
            });
            match leader {
                None => {
                    failed = true;
                    println!("no leader among the nodes given");
                }
                Some(l) => match request(*l, &format!("transfer {to}"), Duration::from_secs(10)) {
                    Ok(_) => println!("{l}: leadership handed to node {to}"),
                    Err(e) => {
                        failed = true;
                        println!("{l}: {e}");
                    }
                },
            }
        }
        "roll" => {
            let leader = nodes.iter().find(|n| {
                request(**n, "status", Duration::from_secs(1))
                    .is_ok_and(|kv| field(&kv, "role") == "leader")
            });
            let cmd = match stream {
                Some(s) => format!("roll {s}"),
                None => "roll".to_string(),
            };
            match leader {
                None => {
                    failed = true;
                    println!("no leader among the nodes given");
                }
                Some(l) => match request(*l, &cmd, Duration::from_secs(10)) {
                    Ok(kv) => println!("{l}: now on stream {}", field(&kv, "stream")),
                    Err(e) => {
                        failed = true;
                        println!("{l}: {e}");
                    }
                },
            }
        }
        _ => unreachable!(),
    }
    if failed {
        exit(1);
    }
}
