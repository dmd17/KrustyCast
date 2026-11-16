//! The MoldUDP64 adapter publishes the same sequence with the same seqnos,
//! answers standard re-requests, and ends the session.
use krusty_node::{MoldConfig, NodeConfig};
use krusty_wire::feedback::{Feedback, Outcome};
use krusty_wire::ingress::{Append, Submit};
use std::net::{SocketAddr, SocketAddrV4, UdpSocket};
use std::time::{Duration, Instant};

fn v4(a: SocketAddr) -> SocketAddrV4 {
    match a {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    }
}

#[test]
fn mold_feed_carries_the_same_sequence_and_serves_rerequests() {
    let native = UdpSocket::bind("127.0.0.1:0").unwrap();
    let mold = UdpSocket::bind("127.0.0.1:0").unwrap();
    mold.set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    // The test reads this socket only after sending everything.
    socket2::SockRef::from(&mold)
        .set_recv_buffer_size(8 << 20)
        .unwrap();
    let session = krusty_mold::Session::new("KC1").unwrap();
    let mut cfg = NodeConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        v4(native.local_addr().unwrap()),
        "127.0.0.1:0".parse().unwrap(),
    );
    cfg.mold = Some(MoldConfig {
        session,
        feed: v4(mold.local_addr().unwrap()),
        rerequest: "127.0.0.1:0".parse().unwrap(),
    });
    cfg.heartbeat_us = 5_000;
    let node = krusty_node::start(cfg).unwrap();

    // One client, stop-and-wait, 300 messages.
    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let mut buf = [0u8; 2048];
    for seq in 1..=300u64 {
        let payload = format!("msg-{seq}");
        let s = Submit {
            client_id: 9,
            incarnation: 1,
            client_seqno: seq,
            append: Append::Unconditional,
            payload: payload.as_bytes(),
        };
        let n = s.encode(&mut buf).unwrap();
        loop {
            client.send_to(&buf[..n], node.ingress_addr()).unwrap();
            let mut fb = [0u8; 64];
            if let Ok(k) = client.recv(&mut fb) {
                let f = Feedback::decode(&fb[..k]).unwrap();
                assert_eq!(f.outcome, Outcome::Accepted { global_seqno: seq });
                break;
            }
        }
    }

    // The Mold feed: every message, in order, with the global seqno.
    let mut got: Vec<(u64, Vec<u8>)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while got.len() < 300 && Instant::now() < deadline {
        let Ok(n) = mold.recv(&mut buf) else { continue };
        assert!(n >= 20, "short MoldUDP64 packet");
        let p = moldudp::Packet::new(&buf[..n]);
        assert_eq!(p.session_ident_raw(), &session.0);
        if matches!(p.packet_kind(), moldudp::PacketKind::Standard) {
            for (i, m) in p.iter().enumerate() {
                got.push((p.seq_num() + i as u64, m.data().to_vec()));
            }
        }
    }
    assert_eq!(got.len(), 300);
    for (i, (seqno, m)) in got.iter().enumerate() {
        assert_eq!(*seqno, i as u64 + 1);
        assert_eq!(m, format!("msg-{seqno}").as_bytes());
    }

    // A standard re-request for 10..15 from another socket.
    // Laid out as the MoldUDP64 spec says: session, seqno, count.
    let mut req = [0u8; 20];
    req[..10].copy_from_slice(&session.0);
    req[10..18].copy_from_slice(&10u64.to_be_bytes());
    req[18..20].copy_from_slice(&5u16.to_be_bytes());
    let asker = UdpSocket::bind("127.0.0.1:0").unwrap();
    asker
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    asker
        .send_to(&req, node.mold_rerequest_addr().unwrap())
        .unwrap();
    let n = asker.recv(&mut buf).unwrap();
    let p = moldudp::Packet::new(&buf[..n]);
    assert_eq!(p.seq_num(), 10);
    let ms: Vec<&[u8]> = p.iter().map(|m| m.data()).collect();
    assert_eq!(ms.len(), 5);
    assert_eq!(ms[0], b"msg-10");

    // Stopping ends the Mold session at the next seqno.
    node.stop();
    let mut ended = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while !ended && Instant::now() < deadline {
        if let Ok(n) = mold.recv(&mut buf)
            && n >= 20
            && matches!(
                moldudp::Packet::new(&buf[..n]).packet_kind(),
                moldudp::PacketKind::EndOfSession
            )
        {
            assert_eq!(moldudp::Packet::new(&buf[..n]).seq_num(), 301);
            ended = true;
        }
    }
    assert!(ended, "no end of session");
}
