use crossbeam_channel::{select, tick, unbounded};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::thread;
use clap::Parser;

const BUFFER_SIZE: usize = 1000; // Store the last 1000 messages
const MOLDUDP64_HEADER_SIZE: usize = 20;
const MOLDUDP64_MESSAGE_HEADER_SIZE: usize = 22;

/// Command-line argument parser
#[derive(Parser, Debug)]
#[command(name = "KrustyCast Sequencer")]
#[command(about = "The KrustyCast multicast sequencer.")]
struct Args {
    /// Unique sender ID (u32)
    #[arg(long)]
    epoch: u32,

    /// Sequencer Input multicast address (e.g., "127.0.0.1:5000")
    #[arg(long, default_value="239.0.0.1:5000")]
    to_sequencer_addr: String,

    /// Sequencer Output multicast address (e.g., "239.0.0.2:6000")
    #[arg(long, default_value="239.0.0.2:6000")]
    from_sequencer_addr: String
}

// Define the message structure.
#[derive(Debug)]
struct MessageFromClient {
    sender_id: u32,
    sender_seqno: u64,
    target: u32,
    sequence: u64,
    payload: Vec<u8>
}

/// Parses an incoming byte slice into a Message.
/// Expected format: 4 bytes (sender), 8 bytes (sender_seqno), 4 bytes (target) + 8 bytes (target_seqno) + payload.
fn parse_message(data: &[u8], src_addr: SocketAddr) -> Option<MessageFromClient> {
    if data.len() < 24 {
        return None;
    }
    use std::convert::TryInto;
    let sender_id = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let sender_seqno = u64::from_be_bytes(data[4..12].try_into().unwrap());
    let target = u32::from_be_bytes(data[12..16].try_into().unwrap());
    let sequence = u64::from_be_bytes(data[16..24].try_into().unwrap());
    let payload = data[24..].to_vec();

    Some(MessageFromClient {
        sender_id,
        sender_seqno,
        target,
        sequence,
        payload
    })
}

/// Builds a NAK (Negative Acknowledgment) message.
/// Format: 1 byte marker (0xFF) + 4 bytes (target) + 8 bytes (expected sequence).
fn build_nak_message(expected_sequence: u64) -> Vec<u8> {
    let mut msg = Vec::with_capacity(8);
    //msg.push(0xFF); // Marker for NAK
    //msg.extend(&target.to_be_bytes());
    msg.extend(&expected_sequence.to_be_bytes());
    msg
}

fn main() -> std::io::Result<()> {
    let args = Args::parse(); // Parse command-line arguments

    let epoch: u64 = args.epoch;

    // Create UDP socket for receiving.
    let recv_socket = UdpSocket::bind("0.0.0.0:5000")?;
    // Join the recv multicast group (using an appropriate interface, here "0.0.0.0").
    let multicast_v4: Ipv4Addr = args.to_sequencer_addr.parse().unwrap();
    let interface_v4: Ipv4Addr = "0.0.0.0".parse().unwrap();
    recv_socket.join_multicast_v4(&multicast_v4, &interface_v4)?;

    // Create UDP socket for sending.
    let send_socket = UdpSocket::bind("0.0.0.0:6000")?;
    send_socket.set_multicast_ttl_v4(1)?;

    let from_sequencer_multicast_v4: Ipv4Addr = args.from_sequencer_addr.parse().unwrap();

    // Create a lock-free channel for inter-thread message passing.
    let (tx, rx) = unbounded::<Message>();
    let ticker = tick(Duration::from_secs(1));

    let mut message_buffer: VecDeque<Message> = VecDeque::with_capacity(BUFFER_SIZE);
    let mut last_heartbeat = Instant::now();
    let mut sequence_number = 1;

    // Receiver thread: listens for incoming UDP messages.
    let recv_socket_clone = recv_socket.try_clone()?;
    let receiver_handle = thread::spawn(move || {
        // Mapping: sender ID → last rcv sequence number.
        let mut sender_map: HashMap<u32, u64> = HashMap::new();

        //let mut buf = [0u8; 1500];
        let mut buf = Vec::with_capacity(1500 + msg.payload.len());
        loop {
            match recv_socket_clone.recv_from(&mut buf) {
                Ok((n, src_addr)) => {
                    if let Some(message) = parse_message(&buf[..n], src_addr) {
                        println!("Received: {:?}", message);

                        let last_seq = target_map.get(&message.sender_id).copied().unwrap_or(0);
                        //Check for gaps in per sender seqno
                        if (message.sender_seqno == last_seq + 1) {
                            // enqueue for resender thread
                            if let Err(e) = tx.send(message) {
                                eprintln!("Channel send error: {}", e);
                            }
                        } else if message.sender_seqno > last_seq + 1 {
                            // Gap detected: drop and send a NAK.
                            println!(
                                "Gap detected for sender {}: expected {}, received {}",
                                message.sender_id,
                                last_seq + 1,
                                message.sender_seqno
                            );
                            let nak = build_nak_message(last_seq + 1);
                            match send_socket_clone.send_to(&nak, src_addr) {
                                Ok(sent) => {
                                    println!("Sent NAK to {}: {} bytes", msg.src_addr, sent);
                                }
                                Err(e) => {
                                    eprintln!("Failed to send NAK: {}", e);
                                }
                            }
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

    // Resender thread: puts global sequence number on outgoing messages.
    let send_socket_clone = send_socket.try_clone()?;
    let resender_handle = thread::spawn(move || -> ! {
        // Mapping: target ID → last sent sequence number.
        let mut target_map: HashMap<u32, u64> = HashMap::new();
        let mut global_seqno: u64 = 0;
        let msg_count: u16 = 1;

        loop {
            select! {
                recv(rx) -> msg => {
                    if let Ok(msg) = msg {
                        let last_seq_to_target = target_map.get(&msg.target).copied().unwrap_or(0);

                        //do optimistic concurrency control per target
                        if msg.sequence == last_seq_to_target + 1 {
                            // Correct sequence: update map and forward the message.
                            target_map.insert(msg.target, msg.sequence);

                            // build the MoldUDP64 header
                            let mut forwarded_msg = Vec::with_capacity(20 + msg.payload.len());
                            forwarded_msg.extend(&epoch.to_be_bytes());
                            forwarded_msg.extend(&global_seqno.to_be_bytes());
                            forwarded_msg.extend(&msg_count.to_be_bytes());

                            // add the message size
                            let msg_size: u16 = 4 + 8 + msg.payload.len();
                            forwarded_msg.extend(&msg_size.to_be_bytes());

                            forwarded_msg.extend(&msg.sender_id.to_be_bytes());
                            forwarded_msg.extend(&msg.target.to_be_bytes());
                            forwarded_msg.extend(&msg.sequence.to_be_bytes());
                            forwarded_msg.extend(&msg.payload);

                            match send_socket_clone.send_to(&forwarded_msg, from_sequencer_multicast_v4) {
                                Ok(sent) => {
                                    println!(
                                        "Forwarded to {}: {} bytes (target: {}, sequence: {})",
                                        from_sequencer_multicast_v4, sent, msg.target, msg.sequence
                                    );
                                }
                                Err(e) => {
                                    eprintln!("UDP send error: {}", e);
                                }
                            }

                            last_heartbeat = Instant::now();
                        } else if msg.sequence <= last_seq {
                            // Stale message for target; drop it.
                            println!(
                                "Dropping stale message for target {}: received sequence {} (last: {})",
                                msg.target, msg.sequence, last_seq
                            );
                        } 
                    }
                }

                recv(ticker) -> _ => {
                    if last_heartbeat.elapsed() >= Duration::from_secs(1) {
                        // build the heartbeat header
                        let mut heartbeat_packet = Vec::with_capacity(20);
                        forwarded_msg.extend(&epoch.to_be_bytes());
                        forwarded_msg.extend(&global_seqno.to_be_bytes());
                        forwarded_msg.extend(0.to_be_bytes());

                        match send_socket.send_to(&heartbeat_packet, from_sequencer_multicast_v4) {
                            Ok(sent) => {
                                println!("Sent heartbeat with sequence: {})", global_seqno);
                            }
                            Err(e) => {
                                eprintln!("UDP send error: {}", e);
                            }
                        }
                    }
                }
            }
        }
    });

    // Wait on the threads (this example runs indefinitely).
    receiver_handle.join().unwrap();
    resender_handle.join().unwrap();

    Ok(())
}
