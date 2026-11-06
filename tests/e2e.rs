//! End-to-end tests over real UDP sockets (loopback unicast + local multicast).
//! Each test uses its own multicast group/port so they can run in parallel.

use crossbeam_channel::{unbounded, Receiver};
use krustycast::client::{ClientConfig, KrustyClient};
use krustycast::net::bind_receiver;
use krustycast::protocol::{MoldKind, MoldPacket, SequencedMessage, Session};
use krustycast::sequencer::{self, SequencerConfig, SequencerHandle};
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

fn v4(a: SocketAddr) -> SocketAddrV4 {
    match a {
        SocketAddr::V4(a) => SocketAddrV4::new(Ipv4Addr::LOCALHOST, a.port()),
        SocketAddr::V6(_) => unreachable!(),
    }
}

fn start_sequencer(session: &str, output: &str) -> SequencerHandle {
    sequencer::start(SequencerConfig {
        input_addr: "127.0.0.1:0".parse().unwrap(),
        output_addr: output.parse().unwrap(),
        rerequest_addr: "127.0.0.1:0".parse().unwrap(),
        heartbeat_interval: Duration::from_millis(200),
        end_of_session_linger: Duration::from_millis(200),
        ..SequencerConfig::new(Session::new(session).unwrap())
    })
    .unwrap()
}

fn client_config(id: u32, seq: &SequencerHandle, output: &str) -> ClientConfig {
    ClientConfig {
        to_sequencer_addr: v4(seq.input_local_addr()),
        from_sequencer_addr: output.parse().unwrap(),
        rerequest_addr: Some(v4(seq.rerequest_local_addr())),
        ..ClientConfig::new(id)
    }
}

/// A started client whose callback forwards payloads to the returned channel.
fn collecting_client(cfg: ClientConfig) -> (KrustyClient, Receiver<String>) {
    let client = KrustyClient::new(cfg).unwrap();
    let (tx, rx) = unbounded();
    client.register_callback(move |p| {
        let _ = tx.send(String::from_utf8(p).unwrap());
    });
    client.start_listener().unwrap();
    (client, rx)
}

fn collect(rx: &Receiver<String>, n: usize, timeout: Duration) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    let mut out = Vec::new();
    while out.len() < n {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(s) => out.push(s),
            Err(_) => break,
        }
    }
    out
}

fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    f()
}

#[test]
fn concurrent_senders_are_delivered_exactly_once() {
    const OUT: &str = "239.77.0.1:17101";
    let seq = start_sequencer("CONTEND", OUT);
    let (_c3, rx) = collecting_client(client_config(3, &seq, OUT));
    let c1 = KrustyClient::new(client_config(1, &seq, OUT)).unwrap();
    let c2 = KrustyClient::new(client_config(2, &seq, OUT)).unwrap();
    c1.start_listener().unwrap();
    c2.start_listener().unwrap();
    thread::sleep(Duration::from_millis(100));

    // Both senders race for the same target slots.
    thread::scope(|s| {
        for (id, c) in [(1u32, &c1), (2, &c2)] {
            s.spawn(move || {
                for i in 0..50 {
                    c.send_message(3, format!("{id}-{i}").as_bytes());
                }
            });
        }
    });

    let got = collect(&rx, 100, Duration::from_secs(5));
    assert_eq!(got.len(), 100, "got {:?}", got);
    let unique: HashSet<_> = got.iter().collect();
    assert_eq!(unique.len(), 100, "duplicates delivered");
    assert!(wait_until(Duration::from_secs(2), || c1.pending() == 0 && c2.pending() == 0));
    assert!(collect(&rx, 1, Duration::from_millis(300)).is_empty(), "extra deliveries");
}

#[test]
fn late_joiner_replays_session_from_history() {
    const OUT: &str = "239.77.0.2:17102";
    let seq = start_sequencer("REPLAY", OUT);
    let sender = KrustyClient::new(client_config(1, &seq, OUT)).unwrap();
    sender.start_listener().unwrap();
    for i in 0..20 {
        sender.send_message(9, format!("m{i}").as_bytes());
    }
    assert!(wait_until(Duration::from_secs(2), || sender.pending() == 0));

    // Joins after everything was published; recovers it all via re-requests,
    // triggered by the next heartbeat.
    let (late, rx) = collecting_client(ClientConfig { start_seqno: Some(1), ..client_config(9, &seq, OUT) });
    let got = collect(&rx, 20, Duration::from_secs(3));
    let expected: Vec<String> = (0..20).map(|i| format!("m{i}")).collect();
    assert_eq!(got, expected);
    assert_eq!(late.lost(), 0);
}

#[test]
fn rerequest_server_answers_and_ignores_bad_requests() {
    const OUT: &str = "239.77.0.3:17103";
    let seq = start_sequencer("RREQ", OUT);
    let sender = KrustyClient::new(client_config(1, &seq, OUT)).unwrap();
    sender.start_listener().unwrap();
    for i in 0..5 {
        sender.send_message(2, format!("p{i}").as_bytes());
    }
    assert!(wait_until(Duration::from_secs(2), || sender.pending() == 0));

    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    let ask = |session: &str, seqno, count| {
        let req = krustycast::protocol::RequestPacket { session: Session::new(session).unwrap(), seqno, count };
        sock.send_to(&req.encode(), seq.rerequest_local_addr()).unwrap();
        let mut buf = [0u8; 2048];
        sock.recv(&mut buf).ok().map(|n| MoldPacket::decode(&buf[..n]).unwrap())
    };

    let p = ask("RREQ", 2, 3).expect("no retransmission");
    assert_eq!(p.seqno, 2);
    let MoldKind::Messages(msgs) = p.kind else { panic!("not a message packet") };
    let payloads: Vec<_> = msgs.iter().map(|m| SequencedMessage::decode(m).unwrap().payload).collect();
    assert_eq!(payloads, [b"p1".to_vec(), b"p2".to_vec(), b"p3".to_vec()]);

    assert!(ask("WRONG", 1, 1).is_none(), "answered a foreign session");
    assert!(ask("RREQ", 100, 1).is_none(), "answered beyond the end");
}

/// The `moldudp` crate's client, pointed at our sequencer, recovers missed
/// messages from our re-request server.
#[test]
fn interop_moldudp_client_against_our_sequencer() {
    const OUT: &str = "239.77.0.4:17104";
    let seq = start_sequencer("INTEROP1", OUT);
    let sender = KrustyClient::new(client_config(1, &seq, OUT)).unwrap();
    sender.start_listener().unwrap();
    for i in 0..10 {
        sender.send_message(2, format!("x{i}").as_bytes());
    }
    assert!(wait_until(Duration::from_secs(2), || sender.pending() == 0));

    // Their client joins late, expecting seqno 1: the first live packet
    // (a heartbeat) reveals the gap and it re-requests 1..=10 from us.
    let theirs = moldudp::MoldUDP64::builder()
        .multicast_addr(OUT.parse().unwrap())
        .interface_addr(Ipv4Addr::UNSPECIFIED)
        .rerequest_server_addrs(vec![seq.rerequest_local_addr()])
        .expected_session_ident("INTEROP1  ".to_string())
        .expected_seq_num(1)
        .build();
    let downstream = bind_receiver(OUT.parse().unwrap(), Ipv4Addr::UNSPECIFIED).unwrap();
    let rereq = UdpSocket::bind("127.0.0.1:0").unwrap();
    let (data_rx, _req_tx) = theirs
        .start_with_sockets(downstream, rereq, &[seq.rerequest_local_addr()])
        .unwrap();

    let mut recovered = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(3);
    while recovered.len() < 10 && Instant::now() < deadline {
        let Ok(dgram) = data_rx.recv_timeout(Duration::from_millis(100)) else { continue };
        let pkt = moldudp::Packet::new(dgram.bytes());
        for (i, m) in pkt.iter().enumerate() {
            let s = pkt.seq_num() + i as u64;
            if s as usize == recovered.len() + 1 {
                recovered.push(SequencedMessage::decode(m.data()).unwrap().payload);
            }
        }
    }
    let expected: Vec<Vec<u8>> = (0..10).map(|i| format!("x{i}").into_bytes()).collect();
    assert_eq!(recovered, expected);
}

/// Our client, fed by the `moldudp` crate's server with a dropped packet,
/// recovers it from that server's re-request endpoint and delivers in order.
#[test]
fn interop_our_client_against_moldudp_server() {
    const OUT: &str = "239.77.0.5:17105";
    let rereq = UdpSocket::bind("127.0.0.1:0").unwrap();
    let rereq_addr = v4(rereq.local_addr().unwrap());
    let server = moldudp::MoldUDP64Server::builder()
        .multicast_addr(OUT.parse().unwrap())
        .rerequest_bind_addr(rereq.local_addr().unwrap())
        .session("INTEROP2".to_string())
        .heartbeat_interval(Duration::from_millis(200))
        .build();
    let downstream = UdpSocket::bind("0.0.0.0:0").unwrap();
    downstream.set_multicast_ttl_v4(1).unwrap();
    let h = server.start_with_sockets(downstream, rereq).unwrap();

    let (client, rx) = collecting_client(ClientConfig {
        to_sequencer_addr: "127.0.0.1:9".parse().unwrap(), // not used: this client only listens
        from_sequencer_addr: OUT.parse().unwrap(),
        rerequest_addr: Some(rereq_addr),
        ..ClientConfig::new(7)
    });
    thread::sleep(Duration::from_millis(100));

    let msg = |p: &str| SequencedMessage { sender_id: 1, target_id: 7, target_seq: 0, payload: p.into() }.encode();
    h.send(vec![msg("a")]); // seq 1
    h.send_dropped(vec![msg("b"), msg("c")]); // seq 2-3, never multicast
    h.send(vec![msg("d")]); // seq 4: reveals the gap
    h.send_dropped(vec![msg("e")]); // seq 5: only the next heartbeat reveals it

    let got = collect(&rx, 5, Duration::from_secs(3));
    assert_eq!(got, ["a", "b", "c", "d", "e"]);
    assert_eq!(client.lost(), 0);
    h.shutdown();
}
