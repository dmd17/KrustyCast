use dashmap::DashMap;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;

/// A type alias for clarity.
pub type TargetId = u32;
pub type SequenceNumber = u64;

 // Define the message structure.
 #[derive(Debug, Clone)]
 struct OutstandingMessage {
     target_seq: u64,
     payload: Vec<u8>
 }

#[derive(Debug, Clone)]
 struct Target {
    last_seq: SequenceNumber,
    outstanding: DashMap<SequenceNumber, OutstandingMessage>
 }

/// ClientSender encapsulates the sending logic and sequence tracking.
pub struct KrustyClient {
    my_id: u32,
    /// My sequence number
    my_seqno: u64,
    /// The address of the sequencer server.
    to_sequencer_addr: String,
    /// The address of the multicast group to which the sequencer sends.
    from_sequencer_addr: String,
    /// UDP socket used for sending messages.
    to_sequencer_socket: UdpSocket,
    /// UDP socket used for receiving messages.
    from_sequencer_socket: UdpSocket,
    // Buffers sent messages for potential resending to sequencer.
    sent_message_buffer: Arc<Mutex<HashMap<u64, Vec<u8>>>>,

    // track last known seqno and outstanding messages by Target
    target_map: Arc<DashMap<TargetId, Target>>,

    callback: Arc<Mutex<Option<Box<dyn Fn(Vec<u8>) + Send>>>>
}

impl KrustyClient {
    /// Creates a new KrustyClient.
    ///
    /// `sequencer_addr` should be in the form "ip:port".
    pub fn new(my_id: u32, to_sequencer_addr: &str, from_sequencer_addr: &str) -> Self {
        let to_sequencer_socket = UdpSocket::bind("0.0.0.0:0").expect("Could not bind to a local socket");
        let from_sequencer_socket = UdpSocket::bind("0.0.0.0:6000").expect("Could not bind to a local socket");

        KrustyClient {
            my_id,
            my_seqno: 0,
            to_sequencer_addr: to_sequencer_addr.to_string(),
            from_sequencer_addr: from_sequencer_addr.to_string(),
            to_sequencer_socket,
            from_sequencer_socket,
            sent_message_buffer: Arc::new(Mutex::new(HashMap::new())),
            target_map: Arc::new(DashMap::new()),
            callback: Arc::new(Mutex::new(None))
        }
    }

    /// Registers a single callback to receive messages intended for this target.
    pub fn register_callback<F>(&self, callback: F)
    where
        F: Fn(Vec<u8>) + Send + 'static,
    {
        let mut cb = self.callback.lock().unwrap();
        *cb = Some(Box::new(callback));
    }

    /// Sends a message for a given target.
    ///
    /// This function automatically increments the sequence number based on the
    /// last known global sequence for that target.
    pub fn send_message(&mut self, target_id: TargetId, payload: &[u8]) {
        let mut msg_option: Option<OutstandingMessage> = None;
        
        {
            let mut target = self.target_map.entry(target_id).or_insert(Target { last_seq: 0, outstanding: DashMap::new() });

            target.last_seq += 1;

            let msg = OutstandingMessage {
                target_seq: target.last_seq,
                payload: payload.to_vec() // there is a copy here
            };

            //TODO: fix this unnecessary clone
            target.outstanding.insert(msg.target_seq, msg.clone());

            msg_option = Some(msg);
        }

        // Send the message to the sequencer server.
        self.send_or_resend_msg(target_id, &msg_option.unwrap());
    }

    fn send_or_resend_msg(&mut self, target_id: TargetId, msg: &OutstandingMessage) {

        // Build the message: 4 bytes my id, 8 bytes my seqno, 4 bytes target + 8 bytes sequence + payload.
        let mut message = Vec::with_capacity(24 + msg.payload.len());
        message.extend(&self.my_id.to_be_bytes());
        message.extend(&self.my_seqno.to_be_bytes());

        message.extend(&target_id.to_be_bytes());
        message.extend(&msg.target_seq.to_be_bytes());
        message.extend(msg.payload.clone());

        // buffer the message in case resend is needed
        //TODO: remove the unnecessary clone
        self.sent_message_buffer.lock().unwrap().insert(self.my_seqno, message.clone());

        // Send the message to the sequencer server.
        self.to_sequencer_socket.send_to(&message, &self.to_sequencer_addr);

        // increment my sender seqno
        self.my_seqno += 1;

        println!(
            "Sent message for target {} with sequence {}",
            target_id, msg.target_seq
        );
    }

    /// Starts a listener that joins the multicast group and updates the global sequence map.
    ///
    /// - `multicast_addr`: The multicast group address in the form "239.0.0.2:6000".
    /// - `global_sequence_map`: An Arc-wrapped DashMap to store the last known sequence number per target.
    pub fn start_listener(&self) -> std::io::Result<()> {
        // Parse the multicast address.
        let multicast: SocketAddr = self.from_sequencer_addr.parse().expect("Invalid multicast address");
        // For IPv4, join the multicast group.
        if let SocketAddr::V4(addr_v4) = multicast {
            self.from_sequencer_socket.join_multicast_v4(addr_v4.ip(), &Ipv4Addr::UNSPECIFIED)?;
        } else {
            unimplemented!("IPv6 multicast is not implemented in this example");
        }

        // Spawn a thread to continuously listen for messages.
        thread::spawn(move || {
            let mut buf = [0u8; 9000];
            let mut global_seqno: u64 = 0;

            loop {
                match self.from_sequencer_socket.recv_from(&mut buf) {
                    Ok((n, _src)) => {
                        if n >= 20 {
                            let filled_buf = &mut buf[..n];
                            //let v = filled_buf.to_vec();

                            use std::convert::TryInto;
                            // Parse the target and sequence number.
                            let epoch = u32::from_be_bytes(filled_buf[0..3].try_into().unwrap());
                            let seq_no = u64::from_be_bytes(filled_buf[4..11].try_into().unwrap());
                            let msg_count: u16 = u16::from_be_bytes(filled_buf[12..13].try_into().unwrap());

                            //check if the global sequence number is the one expected
                            if seq_no == global_seqno {
                                global_seqno += 1;
                                
                                let mut offset = 14;

                                // parse the messages
                                for i in 0..msg_count {
                                    let msg_len: u16 = u16::from_be_bytes(filled_buf[offset..offset+1].try_into().unwrap());

                                    let sender_id: u32 = u32::from_be_bytes(filled_buf[offset+2..offset+5].try_into().unwrap());

                                    let target_id: u32 = u32::from_be_bytes(filled_buf[offset+6..offset+11].try_into().unwrap());

                                    let target_seq = u64::from_be_bytes(filled_buf[offset+12..offset+20].try_into().unwrap());

                                    offset += 2;
                                    offset += msg_len as usize;

                                    let payload_len = msg_len - 12;

                                    let mut target = self.target_map.entry(target_id).or_insert(Target { last_seq: target_seq, outstanding: DashMap::new() });

                                    // update the last known seq to target
                                    target.last_seq = target_seq;

                                    if target_id == self.my_id {
                                        let payload = &filled_buf[offset+14..offset+payload_len as usize];
                                        
                                        // Execute registered callback if available
                                        if let Some(cb) = self.callback.lock().unwrap().as_ref() {
                                            //TODO: to_vec makes a copy, so eliminate that
                                            cb(payload.to_vec());
                                        }
                                    } else {                                       
                                        // find the message
                                        if let Some(msg) = target.outstanding.get(&target_seq) {
                                            // check if my messages won the race to a target
                                            if sender_id == self.my_id {
                                                // it's the one i sent out, so clean it up
                                                target.outstanding.remove(&target_seq);
                                            } else {
                                                // another client beat me to it, so i need to retry
                                                //TODO: invoke application logic?

                                                // update the target_seq
                                                target.last_seq += 1;
                                                msg.target_seq = target.last_seq;

                                                self.send_or_resend_msg(target_id, &msg);
                                            }
                                        } else {
                                            println!("Couldn't find outstanding message to target {} with target_seq {}",
                                                target_id, target_seq
                                            );
                                        }
                                    }
                                }
                            } else {
                                println!(
                                    "Client detected msg out of global sequence - expected: {}, got: {}).",
                                    global_seqno, seq_no
                                );

                                //TODO: send a Request Packet to the sequencer (or later to a re-request server)
                                let mut msg = Vec::with_capacity(20);
                                msg.extend(&epoch.to_be_bytes());
                                msg.extend(&seq_no.to_be_bytes());
                                msg.extend(1u16.to_be_bytes());

                                //TODO: send it
                            }

                        } else {
                            println!("Listener: Received a packet that is too short");
                        }
                    }
                    Err(e) => {
                        eprintln!("Listener error: {}", e);
                        // Optionally handle or break the loop on critical errors.
                    }
                }
            }
        });

        // Spawn a thread to continuously listen for NAK messages from the sequencer, on the same socket that was used to send to the sequencer.
        thread::spawn(move || {
            let mut buf = [0u8; 100];
            loop {
                match self.to_sequencer_socket.recv_from(&mut buf) {
                    Ok((n, _src)) => {
                        if n >= 8 {
                            let expected_seq = u64::from_be_bytes(buf[0..7].try_into().unwrap());

                            println!("Received NAK: Sequencer expects seqno {}", expected_seq);

                            // Resend the missing message to the sequencer
                            if let Some(buffered_msg) = self.sent_message_buffer.lock().unwrap().get(&expected_seq) {
                                println!("Resending message with seqno {}", expected_seq);
                                
                                // Send the message to the sequencer server.
                                self.to_sequencer_socket.send_to(&buffered_msg, &self.to_sequencer_addr);
                            }
                        } else {
                            eprintln!("NAK Listener: Received a packet that is too short");
                        }
                    }
                    Err(e) => {
                        eprintln!("NAK Listener error: {}", e);
                        // Optionally handle or break the loop on critical errors.
                    }
                }
            }
        });

        Ok(())
    }

}
