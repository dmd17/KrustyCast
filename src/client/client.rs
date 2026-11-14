use crate::net::{RECV_BUFFER_SIZE, bind_receiver, bind_sender};
use crate::protocol::{ClientMessage, Feedback, MoldPacket, SequencedMessage, Session};
use crate::stream::{Outcome, StreamTracker, TrackerConfig};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// How many sent messages are kept for resending after a NAK.
const SENT_BUFFER_LIMIT: usize = 65536;
/// Socket read timeout, i.e. how quickly the listener threads notice `stop()`.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How often the stream processor wakes up to retry re-requests.
const RETRY_TICK: Duration = Duration::from_millis(20);
/// How often the feedback listener checks for messages to resend.
const RESEND_TICK: Duration = Duration::from_millis(20);

type Callback = Box<dyn Fn(Vec<u8>) + Send>;

/// Client settings. `ClientConfig::new` fills in defaults matching the sequencer's.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub my_id: u32,
    /// Where messages to the sequencer go (unicast or multicast).
    pub to_sequencer_addr: SocketAddrV4,
    /// The sequencer's MoldUDP64 multicast stream.
    pub from_sequencer_addr: SocketAddrV4,
    /// The sequencer's re-request server. None = don't recover gaps, skip them.
    pub rerequest_addr: Option<SocketAddrV4>,
    /// Local interface for multicast (unspecified = OS default).
    pub interface: Ipv4Addr,
    /// Only follow this session. None = lock onto the first one seen.
    pub session: Option<Session>,
    /// Replay the session from this seqno via re-requests. None = start at the
    /// first live packet (messages sent before the client joined are not delivered).
    pub start_seqno: Option<u64>,
    /// Identifies this client process to the sequencer. None = random, so a
    /// restarted client is never confused with its previous run.
    pub incarnation: Option<u64>,
    /// Resend a message if the sequencer hasn't answered it (on the stream or
    /// with feedback) within this long. Doubles per attempt, up to 16×.
    pub resend_after: Duration,
    /// Give up on a message after this many resends.
    pub max_resends: u32,
    pub verbose: bool,
}

impl ClientConfig {
    pub fn new(my_id: u32) -> Self {
        ClientConfig {
            my_id,
            to_sequencer_addr: "239.0.0.1:5000".parse().unwrap(),
            from_sequencer_addr: "239.0.0.2:6000".parse().unwrap(),
            rerequest_addr: Some("127.0.0.1:5001".parse().unwrap()),
            interface: Ipv4Addr::UNSPECIFIED,
            session: None,
            start_seqno: None,
            incarnation: None,
            resend_after: Duration::from_millis(200),
            max_resends: 10,
            verbose: false,
        }
    }
}

/// A message to a target that has been sent but not yet seen on the sequenced stream.
#[derive(Debug)]
struct Outstanding {
    sender_seqno: u64,
    payload: Vec<u8>,
    sent_at: Instant,
    resends: u32,
    /// Global seqno, once the sequencer has confirmed it was sequenced.
    accepted_as: Option<u64>,
}

#[derive(Debug, Default)]
struct TargetState {
    /// Highest target seq seen on the stream or claimed by us.
    last_seq: u64,
    /// Our in-flight messages to this target, by the target seq they claimed.
    outstanding: HashMap<u64, Outstanding>,
}

#[derive(Debug)]
struct SenderState {
    next_seqno: u64,
    /// Encoded messages by sender seqno, kept for resending after a NAK.
    sent: BTreeMap<u64, Vec<u8>>,
}

struct Inner {
    cfg: ClientConfig,
    incarnation: u64,
    /// Sends to the sequencer and receives its NAK/REJECT feedback.
    socket: UdpSocket,
    // Lock order: `targets` before `sender`.
    targets: Mutex<HashMap<u32, TargetState>>,
    sender: Mutex<SenderState>,
    callback: Mutex<Option<Callback>>,
    running: AtomicBool,
    delivered: AtomicU64,
    lost: AtomicU64,
    failed: AtomicU64,
}

/// A KrustyCast client: sends messages to targets through the sequencer and
/// delivers sequenced messages addressed to `my_id` to a callback, in
/// sequence order, recovering lost packets from the re-request server.
pub struct KrustyClient {
    inner: Arc<Inner>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl KrustyClient {
    pub fn new(cfg: ClientConfig) -> io::Result<Self> {
        let socket = bind_sender(cfg.to_sequencer_addr, cfg.interface)?;
        socket.set_read_timeout(Some(RESEND_TICK))?;
        Ok(KrustyClient {
            inner: Arc::new(Inner {
                incarnation: cfg.incarnation.unwrap_or_else(new_incarnation),
                cfg,
                socket,
                targets: Mutex::new(HashMap::new()),
                sender: Mutex::new(SenderState {
                    next_seqno: 1,
                    sent: BTreeMap::new(),
                }),
                callback: Mutex::new(None),
                running: AtomicBool::new(false),
                delivered: AtomicU64::new(0),
                lost: AtomicU64::new(0),
                failed: AtomicU64::new(0),
            }),
            threads: Mutex::new(Vec::new()),
        })
    }

    /// Registers the callback that receives payloads of messages addressed to this client.
    /// It runs on the stream processing thread.
    pub fn register_callback<F>(&self, callback: F)
    where
        F: Fn(Vec<u8>) + Send + 'static,
    {
        *self.inner.callback.lock().unwrap() = Some(Box::new(callback));
    }

    /// Sends a message to `target_id`, claiming the next target seq this client knows of.
    /// If another sender wins that slot, the sequencer rejects it and the client
    /// retries automatically with a fresh seq.
    pub fn send_message(&self, target_id: u32, payload: &[u8]) {
        let mut targets = self.inner.targets.lock().unwrap();
        let target = targets.entry(target_id).or_default();
        self.inner
            .claim_and_send(target_id, target, payload.to_vec());
    }

    /// Number of messages sent but not yet seen on the sequenced stream.
    pub fn pending(&self) -> usize {
        self.inner
            .targets
            .lock()
            .unwrap()
            .values()
            .map(|t| t.outstanding.len())
            .sum()
    }

    /// Messages from the stream processed so far (all targets).
    pub fn delivered(&self) -> u64 {
        self.inner.delivered.load(Ordering::SeqCst)
    }

    /// Stream messages given up on after recovery failed.
    pub fn lost(&self) -> u64 {
        self.inner.lost.load(Ordering::SeqCst)
    }

    /// Messages this client sent that were abandoned after `max_resends`
    /// resends went unanswered.
    pub fn failed(&self) -> u64 {
        self.inner.failed.load(Ordering::SeqCst)
    }

    /// This client process's incarnation ID.
    pub fn incarnation(&self) -> u64 {
        self.inner.incarnation
    }

    /// Joins the sequenced stream and starts the listener threads.
    pub fn start_listener(&self) -> io::Result<()> {
        if self.inner.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // already running
        }
        let sockets = (|| -> io::Result<_> {
            let stream =
                bind_receiver(self.inner.cfg.from_sequencer_addr, self.inner.cfg.interface)?;
            stream.set_read_timeout(Some(POLL_INTERVAL))?;
            let rereq = match self.inner.cfg.rerequest_addr {
                Some(addr) => {
                    let s = bind_sender(addr, self.inner.cfg.interface)?;
                    s.set_read_timeout(Some(POLL_INTERVAL))?;
                    Some(Arc::new(s))
                }
                None => None,
            };
            Ok((stream, rereq))
        })();
        let (stream, rereq) = match sockets {
            Ok(s) => s,
            Err(e) => {
                self.inner.running.store(false, Ordering::SeqCst);
                return Err(e);
            }
        };

        let (pkt_tx, pkt_rx) = unbounded::<MoldPacket>();
        let mut threads = self.threads.lock().unwrap();
        {
            let inner = Arc::clone(&self.inner);
            let tx = pkt_tx.clone();
            threads.push(thread::spawn(move || {
                inner.packet_recv_loop(stream, tx, "stream")
            }));
        }
        if let Some(rereq) = &rereq {
            let inner = Arc::clone(&self.inner);
            let socket = rereq.try_clone()?;
            let tx = pkt_tx.clone();
            threads.push(thread::spawn(move || {
                inner.packet_recv_loop(socket, tx, "retransmission")
            }));
        }
        drop(pkt_tx);
        {
            let inner = Arc::clone(&self.inner);
            threads.push(thread::spawn(move || inner.process_loop(pkt_rx, rereq)));
        }
        {
            let inner = Arc::clone(&self.inner);
            threads.push(thread::spawn(move || inner.feedback_loop()));
        }
        Ok(())
    }

    /// Stops the listener threads and waits for them to exit.
    pub fn stop(&self) {
        self.inner.running.store(false, Ordering::SeqCst);
        for handle in self.threads.lock().unwrap().drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for KrustyClient {
    fn drop(&mut self) {
        self.stop();
    }
}

fn new_incarnation() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    // RandomState is seeded randomly per process.
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    h.write_u32(std::process::id());
    h.finish()
}

fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

impl Inner {
    /// Claims the next target seq, records the message as outstanding and sends it.
    /// Caller holds the `targets` lock (passed in as `target`).
    fn claim_and_send(&self, target_id: u32, target: &mut TargetState, payload: Vec<u8>) {
        target.last_seq += 1;
        let target_seq = target.last_seq;

        let mut sender = self.sender.lock().unwrap();
        let sender_seqno = sender.next_seqno;
        sender.next_seqno += 1;
        let msg = ClientMessage {
            sender_id: self.cfg.my_id,
            incarnation: self.incarnation,
            sender_seqno,
            target_id,
            target_seq,
            payload,
        };
        let bytes = msg.encode();
        // Send while holding the lock so sender seqnos hit the wire in order.
        if let Err(e) = self.socket.send_to(&bytes, self.cfg.to_sequencer_addr) {
            // Not fatal: the next message will open a gap and the sequencer will NAK for this one.
            eprintln!("Send to sequencer failed: {}", e);
        }
        sender.sent.insert(sender_seqno, bytes);
        while sender.sent.len() > SENT_BUFFER_LIMIT {
            sender.sent.pop_first();
        }
        drop(sender);

        target.outstanding.insert(
            target_seq,
            Outstanding {
                sender_seqno,
                payload: msg.payload,
                sent_at: Instant::now(),
                resends: 0,
                accepted_as: None,
            },
        );
    }

    /// Receives MoldUDP64 packets on `socket` (the multicast stream, or
    /// retransmissions) and hands them to the processor.
    fn packet_recv_loop(&self, socket: UdpSocket, tx: Sender<MoldPacket>, what: &str) {
        let mut buf = vec![0u8; RECV_BUFFER_SIZE];
        while self.running.load(Ordering::SeqCst) {
            let n = match socket.recv_from(&mut buf) {
                Ok((n, _)) => n,
                Err(e) if is_timeout(&e) => continue,
                Err(e) => {
                    eprintln!("{} receive error: {}", what, e);
                    continue;
                }
            };
            match MoldPacket::decode(&buf[..n]) {
                Some(p) => {
                    if tx.send(p).is_err() {
                        return;
                    }
                }
                None => eprintln!("Malformed {} byte {} packet", n, what),
            }
        }
    }

    /// Orders packets, re-requests gaps and delivers messages.
    fn process_loop(&self, rx: Receiver<MoldPacket>, rereq: Option<Arc<UdpSocket>>) {
        let mut tracker = StreamTracker::new(TrackerConfig {
            session: self.cfg.session,
            start_seqno: self.cfg.start_seqno,
            recover: rereq.is_some(),
            ..TrackerConfig::default()
        });
        let mut deliver = |_seqno: u64, data: Vec<u8>| self.deliver(data);

        while self.running.load(Ordering::SeqCst) {
            let lost_before = tracker.lost();
            match rx.recv_timeout(RETRY_TICK) {
                Ok(p) => match tracker.on_packet(p, &mut deliver) {
                    Outcome::NewSession { session, start } => {
                        println!(
                            "[client {}] following session {} from #{}",
                            self.cfg.my_id, session, start
                        )
                    }
                    Outcome::EndOfSession => println!("[client {}] end of session", self.cfg.my_id),
                    Outcome::ForeignSession(_) | Outcome::Invalid | Outcome::Accepted => {}
                },
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
            let request = tracker.poll_request(Instant::now(), &mut deliver);
            if let (Some(req), Some(socket), Some(addr)) =
                (request, &rereq, self.cfg.rerequest_addr)
            {
                if self.cfg.verbose {
                    println!(
                        "[client {}] re-requesting #{} (+{})",
                        self.cfg.my_id, req.seqno, req.count
                    );
                }
                if let Err(e) = socket.send_to(&req.encode(), addr) {
                    eprintln!("Re-request send failed: {}", e);
                }
            }
            let lost = tracker.lost();
            if lost != lost_before {
                eprintln!(
                    "[client {}] gave up on {} messages",
                    self.cfg.my_id,
                    lost - lost_before
                );
                self.lost.store(lost, Ordering::SeqCst);
                // Our own messages that were sequenced into the skipped range
                // will never be seen; stop waiting for them.
                if let Some(next) = tracker.next_seqno() {
                    for t in self.targets.lock().unwrap().values_mut() {
                        t.outstanding
                            .retain(|_, o| o.accepted_as.is_none_or(|g| g >= next));
                    }
                }
            }
        }
    }

    /// Handles one in-order message from the stream.
    fn deliver(&self, data: Vec<u8>) {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        let Some(m) = SequencedMessage::decode(&data) else {
            eprintln!("Malformed {} byte sequenced message", data.len());
            return;
        };
        {
            let mut targets = self.targets.lock().unwrap();
            let target = targets.entry(m.target_id).or_default();
            target.last_seq = target.last_seq.max(m.target_seq);
            if m.sender_id == self.cfg.my_id {
                target.outstanding.remove(&m.target_seq);
            }
        }
        if m.target_id == self.cfg.my_id
            && let Some(cb) = self.callback.lock().unwrap().as_ref()
        {
            cb(m.payload);
        }
    }

    /// Receives NAK, REJECT and ACCEPTED feedback from the sequencer, and
    /// resends messages it hasn't answered.
    fn feedback_loop(&self) {
        let mut buf = [0u8; 64];
        let mut last_scan = Instant::now();
        while self.running.load(Ordering::SeqCst) {
            if last_scan.elapsed() >= RESEND_TICK {
                self.resend_unanswered();
                last_scan = Instant::now();
            }
            let n = match self.socket.recv_from(&mut buf) {
                Ok((n, _)) => n,
                Err(e) if is_timeout(&e) => continue,
                Err(e) => {
                    eprintln!("Feedback listener error: {}", e);
                    continue;
                }
            };
            match Feedback::decode(&buf[..n]) {
                Some(Feedback::Nak { expected }) => self.resend_from(expected),
                Some(Feedback::Reject {
                    target_id,
                    rejected_target_seq,
                    current_target_seq,
                    sender_seqno,
                }) => {
                    let mut targets = self.targets.lock().unwrap();
                    let target = targets.entry(target_id).or_default();
                    match target.outstanding.get(&rejected_target_seq) {
                        Some(o) if o.sender_seqno == sender_seqno => {}
                        _ => continue, // stale or unknown reject
                    }
                    let o = target.outstanding.remove(&rejected_target_seq).unwrap();
                    target.last_seq = target.last_seq.max(current_target_seq);
                    if self.cfg.verbose {
                        println!(
                            "[client {}] lost slot {} on target {} (target at {}); retrying as {}",
                            self.cfg.my_id,
                            rejected_target_seq,
                            target_id,
                            current_target_seq,
                            target.last_seq + 1
                        );
                    }
                    self.claim_and_send(target_id, target, o.payload);
                }
                Some(Feedback::Accepted {
                    sender_seqno,
                    global_seqno,
                }) => {
                    // A resent message had already been sequenced; the stream
                    // (or a re-request) will deliver it. Stop resending.
                    let mut targets = self.targets.lock().unwrap();
                    if let Some(o) = targets
                        .values_mut()
                        .flat_map(|t| t.outstanding.values_mut())
                        .find(|o| o.sender_seqno == sender_seqno)
                    {
                        o.accepted_as = Some(global_seqno);
                    }
                }
                None => eprintln!("Feedback listener: malformed {} byte packet", n),
            }
        }
    }

    /// Resends, unchanged, every outstanding message the sequencer hasn't
    /// answered within the (backed-off) resend interval. This recovers a lost
    /// message (including the last one, which no later message would expose
    /// with a NAK) and a lost REJECT; the sequencer answers resends of
    /// already-decided messages with their original outcome.
    fn resend_unanswered(&self) {
        let now = Instant::now();
        let mut targets = self.targets.lock().unwrap();
        for (&target_id, target) in targets.iter_mut() {
            let mut abandoned = Vec::new();
            for (&target_seq, o) in target.outstanding.iter_mut() {
                let backoff = self.cfg.resend_after * (1 << o.resends.min(4));
                if o.accepted_as.is_some() || now.duration_since(o.sent_at) < backoff {
                    continue;
                }
                if o.resends >= self.cfg.max_resends {
                    abandoned.push(target_seq);
                    continue;
                }
                o.resends += 1;
                o.sent_at = now;
                if self.cfg.verbose {
                    println!(
                        "[client {}] no answer for seqno {}; resending (attempt {})",
                        self.cfg.my_id, o.sender_seqno, o.resends
                    );
                }
                let bytes = ClientMessage {
                    sender_id: self.cfg.my_id,
                    incarnation: self.incarnation,
                    sender_seqno: o.sender_seqno,
                    target_id,
                    target_seq,
                    payload: o.payload.clone(),
                }
                .encode();
                if let Err(e) = self.socket.send_to(&bytes, self.cfg.to_sequencer_addr) {
                    eprintln!("Resend failed: {}", e);
                }
            }
            for target_seq in abandoned {
                target.outstanding.remove(&target_seq);
                self.failed.fetch_add(1, Ordering::SeqCst);
                eprintln!(
                    "[client {}] giving up on message to target {} after {} resends",
                    self.cfg.my_id, target_id, self.cfg.max_resends
                );
            }
        }
    }

    /// Resends every buffered message from sender seqno `expected` onward.
    fn resend_from(&self, expected: u64) {
        let sender = self.sender.lock().unwrap();
        if sender
            .sent
            .first_key_value()
            .is_none_or(|(&first, _)| first > expected)
        {
            eprintln!(
                "NAK for seqno {} which is no longer buffered; cannot recover",
                expected
            );
            return;
        }
        for (seqno, bytes) in sender.sent.range(expected..) {
            if self.cfg.verbose {
                println!(
                    "[client {}] resending seqno {} after NAK",
                    self.cfg.my_id, seqno
                );
            }
            if let Err(e) = self.socket.send_to(bytes, self.cfg.to_sequencer_addr) {
                eprintln!("Resend failed: {}", e);
            }
        }
    }
}
