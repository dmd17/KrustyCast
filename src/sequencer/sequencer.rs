use crossbeam_channel::unbounded;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::thread;

// Define the message structure.
#[derive(Debug)]
struct Message {
    target: u32,
    sequence: u64,
    payload: Vec<u8>,
    src_addr: SocketAddr,
}

/// Parses an incoming byte slice into a Message.
/// Expected format: 4 bytes (target) + 8 bytes (sequence) + payload.
fn parse_message(data: &[u8], src_addr: SocketAddr) -> Option<Message> {
    if data.len() < 12 {
        return None;
    }
    use std::convert::TryInto;
    let target = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let sequence = u64::from_be_bytes(data[4..12].try_into().unwrap());
    let payload = data[12..].to_vec();
    Some(Message {
        target,
        sequence,
        payload,
        src_addr,
    })
}

/// Builds a NAK (Negative Acknowledgment) message.
/// Format: 1 byte marker (0xFF) + 4 bytes (target) + 8 bytes (expected sequence).
fn build_nak_message(target: u32, expected_sequence: u64) -> Vec<u8> {
    let mut msg = Vec::with_capacity(1 + 4 + 8);
    msg.push(0xFF); // Marker for NAK
    msg.extend(&target.to_be_bytes());
    msg.extend(&expected_sequence.to_be_bytes());
    msg
}

fn main() -> std::io::Result<()> {
    // Define multicast addresses.
    const RECV_ADDR: &str = "239.0.0.1:5000";
    const SEND_ADDR: &str = "239.0.0.2:6000";

    // Create UDP socket for receiving.
    let recv_socket = UdpSocket::bind("0.0.0.0:5000")?;
    // Join the multicast group (using an appropriate interface, here "0.0.0.0").
    let multicast_v4: Ipv4Addr = "239.0.0.1".parse().unwrap();
    let interface_v4: Ipv4Addr = "0.0.0.0".parse().unwrap();
    recv_socket.join_multicast_v4(&multicast_v4, &interface_v4)?;

    // Create UDP socket for sending.
    let send_socket = UdpSocket::bind("0.0.0.0:6000")?;
    send_socket.set_multicast_ttl_v4(1)?;

    // Create a lock-free channel for inter-thread message passing.
    let (tx, rx) = unbounded::<Message>();

    // Receiver thread: listens for incoming UDP messages.
    let recv_socket_clone = recv_socket.try_clone()?;
    let receiver_handle = thread::spawn(move || {
        let mut buf = [0u8; 1500];
        loop {
            match recv_socket_clone.recv_from(&mut buf) {
                Ok((n, src_addr)) => {
                    if let Some(message) = parse_message(&buf[..n], src_addr) {
                        println!("Received: {:?}", message);
                        if let Err(e) = tx.send(message) {
                            eprintln!("Channel send error: {}", e);
                        }
                    } else {
                        eprintln!("Failed to parse message from {}", src_addr);
                    }
                }
                Err(e) => {
                    eprintln!("UDP receive error: {}", e);
                }
            }
        }
    });

    // Resender thread: validates sequence numbers and forwards messages.
    let send_socket_clone = send_socket.try_clone()?;
    let resender_handle = thread::spawn(move || {
        // Mapping: target ID → last sent sequence number.
        let mut target_map: HashMap<u32, u64> = HashMap::new();

        loop {
            match rx.recv() {
                Ok(msg) => {
                    let last_seq = target_map.get(&msg.target).copied().unwrap_or(0);
                    if msg.sequence == last_seq + 1 {
                        // Correct sequence: update map and forward the message.
                        target_map.insert(msg.target, msg.sequence);
                        let mut forwarded_msg = Vec::with_capacity(12 + msg.payload.len());
                        forwarded_msg.extend(&msg.target.to_be_bytes());
                        forwarded_msg.extend(&msg.sequence.to_be_bytes());
                        forwarded_msg.extend(&msg.payload);
                        match send_socket_clone.send_to(&forwarded_msg, SEND_ADDR) {
                            Ok(sent) => {
                                println!(
                                    "Forwarded to {}: {} bytes (target: {}, sequence: {})",
                                    SEND_ADDR, sent, msg.target, msg.sequence
                                );
                            }
                            Err(e) => {
                                eprintln!("UDP send error: {}", e);
                            }
                        }
                    } else if msg.sequence <= last_seq {
                        // Duplicate or stale message; drop it.
                        println!(
                            "Dropping stale message for target {}: received sequence {} (last: {})",
                            msg.target, msg.sequence, last_seq
                        );
                    } else if msg.sequence > last_seq + 1 {
                        // Gap detected: drop and send a NAK.
                        println!(
                            "Gap detected for target {}: expected {}, received {}",
                            msg.target,
                            last_seq + 1,
                            msg.sequence
                        );
                        let nak = build_nak_message(msg.target, last_seq + 1);
                        match send_socket_clone.send_to(&nak, msg.src_addr) {
                            Ok(sent) => {
                                println!("Sent NAK to {}: {} bytes", msg.src_addr, sent);
                            }
                            Err(e) => {
                                eprintln!("Failed to send NAK: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Channel receive error: {}", e);
                }
            }
        }
    });

    // Wait on the threads (this example runs indefinitely).
    receiver_handle.join().unwrap();
    resender_handle.join().unwrap();

    Ok(())
}

