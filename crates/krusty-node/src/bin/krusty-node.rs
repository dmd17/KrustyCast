//! KrustyCast node (single node, in-memory).
//!
//!   krusty-node --ingress 0.0.0.0:7000 --feed 239.1.1.1:7001 --interface 10.0.0.5 \
//!               --recovery 0.0.0.0:7002 [--spin] [--cores 2,3,4,5]
//!               [--mold-session NAME --mold-feed 239.1.1.2:7004 [--mold-rerequest 0.0.0.0:7003]]
use krusty_node::{Idle, MoldConfig, NodeConfig};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::Ordering;
use std::time::Duration;

fn main() {
    let mut ingress: SocketAddrV4 = "0.0.0.0:7000".parse().unwrap();
    let mut feed: SocketAddrV4 = "127.0.0.1:7001".parse().unwrap();
    let mut recovery: SocketAddrV4 = "0.0.0.0:7002".parse().unwrap();
    let mut interface = Ipv4Addr::UNSPECIFIED;
    let mut spin = false;
    let mut cores = [None; 4];
    let mut stream_id = 1u64;
    let mut mold_session: Option<krusty_mold::Session> = None;
    let mut mold_feed: Option<SocketAddrV4> = None;
    let mut mold_rerequest: SocketAddrV4 = "0.0.0.0:7003".parse().unwrap();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().unwrap_or_else(|| panic!("{a} needs a value"));
        match a.as_str() {
            "--ingress" => ingress = val().parse().expect("--ingress ip:port"),
            "--feed" => feed = val().parse().expect("--feed ip:port"),
            "--recovery" => recovery = val().parse().expect("--recovery ip:port"),
            "--interface" => interface = val().parse().expect("--interface ip"),
            "--stream-id" => stream_id = val().parse().expect("--stream-id u64"),
            "--spin" => spin = true,
            "--mold-session" => {
                mold_session = Some(
                    krusty_mold::Session::new(&val())
                        .expect("--mold-session: 1-10 printable ASCII"),
                )
            }
            "--mold-feed" => mold_feed = Some(val().parse().expect("--mold-feed ip:port")),
            "--mold-rerequest" => mold_rerequest = val().parse().expect("--mold-rerequest ip:port"),
            "--cores" => {
                for (i, c) in val().split(',').enumerate().take(4) {
                    cores[i] = Some(c.parse().expect("--cores a,b,c,d"));
                }
            }
            other => panic!("unknown argument {other}"),
        }
    }
    if feed.ip().is_multicast() && interface.is_unspecified() {
        // docs/BASELINE.md finding 1: the default route may be Wi-Fi.
        eprintln!(
            "error: a multicast feed needs --interface (the default route may be a slow link)"
        );
        std::process::exit(2);
    }
    let mut cfg = NodeConfig::new(ingress, feed, recovery);
    cfg.interface = interface;
    cfg.stream_id = stream_id;
    cfg.cores = cores;
    if spin {
        cfg.idle = Idle::Spin;
    }
    match (mold_session, mold_feed) {
        (Some(session), Some(feed)) => {
            cfg.mold = Some(MoldConfig {
                session,
                feed,
                rerequest: mold_rerequest,
            });
        }
        (None, None) => {}
        _ => {
            eprintln!("error: the MoldUDP64 adapter needs both --mold-session and --mold-feed");
            std::process::exit(2);
        }
    }
    let node = krusty_node::start(cfg).expect("start node");
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
            "submits {} published {} frames {} busy {} edge-rejects {} recover {}",
            s.submits.load(Ordering::Relaxed),
            s.published.load(Ordering::Relaxed),
            s.frames.load(Ordering::Relaxed),
            s.busy.load(Ordering::Relaxed),
            s.rejected_at_edge.load(Ordering::Relaxed),
            s.recover_requests.load(Ordering::Relaxed)
        );
    }
    println!("stopping: sending END_OF_SESSION");
    node.stop();
}
