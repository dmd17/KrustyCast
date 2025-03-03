use dashmap::DashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

 // Define the message structure.
 #[derive(Debug, Clone)]
 struct OutstandingMessage {
     target_seq: u64,
     payload: Vec<u8>
 }

#[derive(Debug, Clone)]
 struct Target {
    last_seq: Arc<AtomicU64>,
    outstanding: DashMap<u64, OutstandingMessage>
 }

/// ClientSender encapsulates the sending logic and sequence tracking.
pub struct KrustyClient {
    my_id: u32,
    /// My sequence number
    my_seqno: Arc<AtomicU64>,
    /// The address of the sequencer server.
    to_sequencer_addr: SocketAddrV4,
    /// The address of the multicast group to which the sequencer sends.
    from_sequencer_addr: SocketAddrV4,
    /// UDP socket used for sending messages.
    to_sequencer_socket: Arc<UdpSocket>,

    // Buffers sent messages for potential resending to sequencer.
    sent_message_buffer: Arc<DashMap<u64, Vec<u8>>>,

    // track last known seqno and outstanding messages by Target
    target_map: Arc<DashMap<u32, Target>>,

    callback: Arc<Mutex<Option<Box<dyn Fn(Vec<u8>) + Send>>>>
}

impl KrustyClient {
    /// Creates a new KrustyClient.
    ///
    /// `sequencer_addr` should be in the form "ip:port".
    pub fn new(my_id: u32, to_sequencer_addr: &str, from_sequencer_addr: &str) -> Self {
        KrustyClient {
            my_id: my_id,
            my_seqno: Arc::new(AtomicU64::new(0)),
            to_sequencer_addr: to_sequencer_addr.parse().unwrap(),
            from_sequencer_addr: from_sequencer_addr.parse().unwrap(),
            to_sequencer_socket: Arc::new(UdpSocket::bind("0.0.0.0:0").expect("Could not bind to a local socket")),
            sent_message_buffer: Arc::new(DashMap::new()),
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
    pub fn send_message(&self, target_id: u32, payload: &[u8]) {
        let target = self.target_map.entry(target_id).or_insert(Target { last_seq: Arc::new(AtomicU64::new(0)), outstanding: DashMap::new() });

        target.last_seq.fetch_add(1, Ordering::Relaxed);

        let msg = OutstandingMessage {
            target_seq: target.last_seq.load(Ordering::Relaxed),
            payload: payload.to_vec() // there is a copy here
        };

        //TODO: fix this unnecessary clone
        target.outstanding.insert(msg.target_seq, msg.clone());

        // Send the message to the sequencer server.
        send_or_resend_msg(
            self.my_id,
            &Arc::clone(&self.my_seqno),
            target_id,
            &msg,
            &Arc::clone(&self.sent_message_buffer),
            &Arc::clone(&self.to_sequencer_socket),
            &self.to_sequencer_addr
            );
    }

    /// Starts a listener that joins the multicast group and updates the global sequence map.
    ///
    /// - `multicast_addr`: The multicast group address in the form "239.0.0.2:6000".
    /// - `global_sequence_map`: An Arc-wrapped DashMap to store the last known sequence number per target.
    pub fn start_listener(&self) -> std::io::Result<()> {
        // alias the needed structures so that the thread doesn't need to borrow self
        let my_id = self.my_id;
        let from_sequencer_socket = UdpSocket::bind("0.0.0.0:6000").expect("Could not bind to a local socket");
        let target_map = Arc::clone(&self.target_map);
        let cb_clone = Arc::clone(&self.callback);

        let my_seqno_clone = Arc::clone(&self.my_seqno);

        let send_msg_buf_clone = Arc::clone(&self.sent_message_buffer);
        let to_seq_sock_clone = Arc::clone(&self.to_sequencer_socket);
        let to_seq_addr_clone = self.to_sequencer_addr.clone();

        // For IPv4, join the multicast group.
        from_sequencer_socket.join_multicast_v4(self.from_sequencer_addr.ip(), &Ipv4Addr::UNSPECIFIED)?;
        
        // Spawn a thread to continuously listen for messages.
        thread::spawn(move || {
            let mut buf = [0u8; 9000];
            let mut global_seqno: u64 = 0;

            loop {
                match from_sequencer_socket.recv_from(&mut buf) {
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

                                    let target = target_map.entry(target_id).or_insert(Target { last_seq: Arc::new(AtomicU64::new(0)), outstanding: DashMap::new() });

                                    // update the last known seq to target
                                    target.last_seq.store(target_seq, Ordering::Relaxed);

                                    if target_id == my_id {
                                        let payload = &filled_buf[offset+14..offset+payload_len as usize];
                                        
                                        // Execute registered callback if available
                                        if let Some(cb) = cb_clone.lock().unwrap().as_ref() {
                                            //TODO: to_vec makes a copy, so eliminate that
                                            cb(payload.to_vec());
                                        }
                                    } else {                                       
                                        // find the message
                                        if let Some(mut msg) = target.outstanding.get_mut(&target_seq) {
                                            // check if my messages won the race to a target
                                            if sender_id == my_id {
                                                // it's the one i sent out, so clean it up
                                                target.outstanding.remove(&target_seq);
                                            } else {
                                                // another client beat me to it, so i need to retry
                                                //TODO: invoke application logic?

                                                // update the target_seq
                                                target.last_seq.fetch_add(1, Ordering::Relaxed);
                                                msg.target_seq = target.last_seq.load(Ordering::Relaxed);

                                                // Send the message to the sequencer server.
                                                send_or_resend_msg(
                                                    my_id,
                                                    &my_seqno_clone,
                                                    target_id,
                                                    &msg,
                                                    &send_msg_buf_clone,
                                                    &to_seq_sock_clone,
                                                    &to_seq_addr_clone
                                                    );

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

        let to_seq_socket_clone = Arc::clone(&self.to_sequencer_socket);
        let msg_buf_clone = Arc::clone(&self.sent_message_buffer);
        let to_sequencer_addr_clone = self.to_sequencer_addr.clone();

        // Spawn a thread to continuously listen for NAK messages from the sequencer, on the same socket that was used to send to the sequencer.
        thread::spawn(move || {
            let mut buf = [0u8; 100];
            loop {
                match to_seq_socket_clone.recv_from(&mut buf) {
                    Ok((n, _src)) => {
                        if n >= 8 {
                            let expected_seq = u64::from_be_bytes(buf[0..7].try_into().unwrap());

                            println!("Received NAK: Sequencer expects seqno {}", expected_seq);

                            // Resend the missing message to the sequencer
                            if let Some(buffered_msg) = msg_buf_clone.get(&expected_seq) {
                                println!("Resending message with seqno {}", expected_seq);
                                
                                // Send the message to the sequencer server.
                                to_seq_socket_clone.send_to(&buffered_msg, &to_sequencer_addr_clone);
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

    pub fn stop() {

    }

}

fn send_or_resend_msg(my_id: u32, my_seqno: &Arc<AtomicU64>, target_id: u32, msg: &OutstandingMessage,
    sent_message_buffer: &Arc<DashMap<u64, Vec<u8>>>,
    to_sequencer_socket: &Arc<UdpSocket>, to_sequencer_addr: &SocketAddrV4) {

    let my_seqno_snapshot = my_seqno.load(Ordering::Relaxed);

    // Build the message: 4 bytes my id, 8 bytes my seqno, 4 bytes target + 8 bytes sequence + payload.
    let mut message = Vec::with_capacity(24 + msg.payload.len());
    message.extend(my_id.to_be_bytes());
    message.extend(&my_seqno_snapshot.to_be_bytes());
    message.extend(&target_id.to_be_bytes());
    message.extend(&msg.target_seq.to_be_bytes());
    message.extend(msg.payload.clone());

    // buffer the message in case resend is needed
    //TODO: remove the unnecessary clone
    sent_message_buffer.insert(my_seqno_snapshot, message.clone());

    // Send the message to the sequencer server.
    to_sequencer_socket.send_to(&message, to_sequencer_addr);

    // increment my sender seqno
    my_seqno.fetch_add(1, Ordering::Relaxed);

    println!(
        "Sent message for target {} with sequence {}",
        target_id, msg.target_seq
    );
}