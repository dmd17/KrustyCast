use dashmap::DashMap;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::thread;

/// A type alias for clarity.
pub type TargetId = u32;
pub type SequenceNumber = u64;

/// ClientSender encapsulates the sending logic and sequence tracking.
pub struct ClientSender {
    /// A thread-safe map tracking the last known sequence number for each target.
    pub global_sequence_map: Arc<DashMap<TargetId, SequenceNumber>>,
    /// The address of the sequencer server.
    pub sequencer_addr: String,
    /// UDP socket used for sending messages.
    pub socket: UdpSocket,
}

impl ClientSender {
    /// Creates a new ClientSender.
    ///
    /// `sequencer_addr` should be in the form "ip:port".
    pub fn new(sequencer_addr: &str) -> Self {
        let socket = UdpSocket::bind("0.0.0.0:0").expect("Could not bind to a local socket");
        ClientSender {
            global_sequence_map: Arc::new(DashMap::new()),
            sequencer_addr: sequencer_addr.to_string(),
            socket,
        }
    }

    /// Sends a message for a given target.
    ///
    /// This function automatically increments the sequence number based on the
    /// last known global sequence for that target.
    pub fn send_message(&self, target: TargetId, payload: &[u8]) -> std::io::Result<()> {
        // Look up the last known sequence number; default to 0 if not found.
        let next_seq = self
            .global_sequence_map
            .get(&target)
            .map(|s| *s + 1)
            .unwrap_or(1);

        // Build the message: 4 bytes target + 8 bytes sequence + payload.
        let mut message = Vec::with_capacity(12 + payload.len());
        message.extend(&target.to_be_bytes());
        message.extend(&next_seq.to_be_bytes());
        message.extend(payload);

        // Send the message to the sequencer server.
        self.socket.send_to(&message, &self.sequencer_addr)?;

        // Update the local global sequence map.
        self.global_sequence_map.insert(target, next_seq);
        println!(
            "Sent message for target {} with sequence {}",
            target, next_seq
        );
        Ok(())
    }
}


/// Starts a listener that joins the multicast group and updates the global sequence map.
///
/// - `multicast_addr`: The multicast group address in the form "239.0.0.2:6000".
/// - `global_sequence_map`: An Arc-wrapped DashMap to store the last known sequence number per target.
pub fn start_listener(
    multicast_addr: &str,
    global_sequence_map: Arc<DashMap<u32, u64>>,
) -> std::io::Result<()> {
    // Bind to a local address. Use "0.0.0.0" to listen on all interfaces.
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    // Parse the multicast address.
    let multicast: SocketAddr = multicast_addr.parse().expect("Invalid multicast address");
    // For IPv4, join the multicast group.
    if let SocketAddr::V4(addr_v4) = multicast {
        socket.join_multicast_v4(addr_v4.ip(), &Ipv4Addr::UNSPECIFIED)?;
    } else {
        unimplemented!("IPv6 multicast is not implemented in this example");
    }

    // Spawn a thread to continuously listen for messages.
    thread::spawn(move || {
        let mut buf = [0u8; 1500];
        loop {
            match socket.recv_from(&mut buf) {
                Ok((n, _src)) => {
                    if n >= 12 {
                        // Parse the target and sequence number.
                        let target = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
                        let seq = u64::from_be_bytes([
                            buf[4], buf[5], buf[6], buf[7],
                            buf[8], buf[9], buf[10], buf[11],
                        ]);
                        // Update the sequence map if this is a new sequence.
                        global_sequence_map.insert(target, seq);
                        println!(
                            "Listener: Updated target {} with global sequence {}",
                            target, seq
                        );
                    } else {
                        eprintln!("Listener: Received a packet that is too short");
                    }
                }
                Err(e) => {
                    eprintln!("Listener error: {}", e);
                    // Optionally handle or break the loop on critical errors.
                }
            }
        }
    });

    Ok(())
}

