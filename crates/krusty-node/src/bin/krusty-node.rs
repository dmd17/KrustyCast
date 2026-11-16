//! KrustyCast node: a single node or one cluster member, as configured.
//!
//!   krusty-node [--config node.toml]
//!               [--ingress 0.0.0.0:7000] [--feed 239.1.1.1:7001] [--interface 10.0.0.5]
//!               [--recovery 0.0.0.0:7002] [--stream-id N] [--spin] [--cores 2,3,4,5]
//!               [--mold-session NAME --mold-feed 239.1.1.2:7004 [--mold-rerequest 0.0.0.0:7003]]
//!
//! Flags override the file. See `config/node.example.toml` for every key.
use krusty_node::config::FileConfig;
use krusty_node::{Idle, MoldConfig, NodeConfig};
use std::net::SocketAddrV4;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut cfg = NodeConfig::new(
        "0.0.0.0:7000".parse().unwrap(),
        "127.0.0.1:7001".parse().unwrap(),
        "0.0.0.0:7002".parse().unwrap(),
    );
    if let Some(i) = args.iter().position(|a| a == "--config") {
        let path = args
            .get(i + 1)
            .unwrap_or_else(|| fail("--config needs a path"));
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| fail(format!("{path}: {e}")));
        let file = FileConfig::parse(&text).unwrap_or_else(|e| fail(format!("{path}: {e}")));
        file.apply(&mut cfg)
            .unwrap_or_else(|e| fail(format!("{path}: {e}")));
    }
    let mut mold_session = cfg.mold.map(|m| m.session);
    let mut mold_feed = cfg.mold.map(|m| m.feed);
    let mut mold_rerequest: SocketAddrV4 = cfg
        .mold
        .map(|m| m.rerequest)
        .unwrap_or("0.0.0.0:7003".parse().unwrap());
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut val = || {
            it.next()
                .unwrap_or_else(|| fail(format!("{a} needs a value")))
                .as_str()
        };
        match a.as_str() {
            "--config" => {
                val();
            }
            "--ingress" => {
                cfg.ingress = val().parse().unwrap_or_else(|_| fail("--ingress ip:port"))
            }
            "--feed" => cfg.feed = val().parse().unwrap_or_else(|_| fail("--feed ip:port")),
            "--recovery" => {
                cfg.recovery = val().parse().unwrap_or_else(|_| fail("--recovery ip:port"))
            }
            "--interface" => {
                cfg.interface = val().parse().unwrap_or_else(|_| fail("--interface ip"))
            }
            "--stream-id" => {
                cfg.stream_id = val().parse().unwrap_or_else(|_| fail("--stream-id u64"))
            }
            "--spin" => cfg.idle = Idle::Spin,
            "--cores" => {
                for (i, c) in val().split(',').enumerate().take(4) {
                    cfg.cores[i] = Some(c.parse().unwrap_or_else(|_| fail("--cores a,b,c,d")));
                }
            }
            "--mold-session" => {
                mold_session = Some(
                    krusty_mold::Session::new(val())
                        .unwrap_or_else(|| fail("--mold-session: 1-10 printable ASCII")),
                )
            }
            "--mold-feed" => {
                mold_feed = Some(
                    val()
                        .parse()
                        .unwrap_or_else(|_| fail("--mold-feed ip:port")),
                )
            }
            "--mold-rerequest" => {
                mold_rerequest = val()
                    .parse()
                    .unwrap_or_else(|_| fail("--mold-rerequest ip:port"))
            }
            other => fail(format!("unknown argument {other}")),
        }
    }
    cfg.mold = match (mold_session, mold_feed) {
        (Some(session), Some(feed)) => Some(MoldConfig {
            session,
            feed,
            rerequest: mold_rerequest,
        }),
        (None, None) => None,
        _ => fail("the MoldUDP64 adapter needs both a session and a feed"),
    };
    cfg.validate().unwrap_or_else(|e| fail(e));
    if cfg.metrics_path.is_none() {
        cfg.metrics_path = Some(format!("/dev/shm/krusty-{}.stats", cfg.stream_id).into());
    }

    let feed = cfg.feed;
    let stream_id = cfg.stream_id;
    let node = krusty_node::start(cfg).unwrap_or_else(|e| fail(format!("start: {e}")));
    println!(
        "krusty-node: ingress {}, feed {}, recovery {}, stream {}",
        node.ingress_addr(),
        feed,
        node.recovery_addr(),
        stream_id
    );
    let (tx, rx) = std::sync::mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = tx.send(());
    })
    .expect("signal handler");
    loop {
        if rx.recv_timeout(Duration::from_secs(5)).is_ok() {
            break;
        }
        let s = &node.stats;
        println!(
            "submits {} published {} frames {} busy {} edge-rejects {} recover {} mold {} (lost {})",
            s.submits.load(Ordering::Relaxed),
            s.published.load(Ordering::Relaxed),
            s.frames.load(Ordering::Relaxed),
            s.busy.load(Ordering::Relaxed),
            s.rejected_at_edge.load(Ordering::Relaxed),
            s.recover_requests.load(Ordering::Relaxed),
            s.mold_published.load(Ordering::Relaxed),
            s.mold_lost.load(Ordering::Relaxed)
        );
    }
    println!("stopping: sending END_OF_SESSION");
    node.stop();
}
