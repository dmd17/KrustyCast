//! The sequencer: accepts client messages, enforces per-sender ordering and
//! per-target optimistic concurrency, and publishes the result as a
//! MoldUDP64 stream with a re-request server. Optionally journals every
//! decision so a restarted sequencer resumes the session where it left off.

use crate::journal::{FsyncPolicy, Journal, Record, SenderKey};
use crate::net::{bind_receiver, bind_sender, RECV_BUFFER_SIZE};
use crate::protocol::{
    ClientMessage, Feedback, MoldPacket, PacketBuilder, RequestPacket, SequencedMessage, Session,
};
use crossbeam_channel::{bounded, select, tick, unbounded, Receiver, Sender};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Most client messages handled per batch (bounds the latency a batch adds).
const MAX_BATCH: usize = 1024;
/// Outcomes remembered per sender, for answering resends of old messages.
const OUTCOME_HISTORY: usize = 4096;

#[derive(Debug, Clone)]
pub struct SequencerConfig {
    pub session: Session,
    /// Where clients send: a multicast group to join, or a local unicast address to bind.
    pub input_addr: SocketAddrV4,
    /// Multicast group the MoldUDP64 stream is published to.
    pub output_addr: SocketAddrV4,
    /// Local address the re-request server binds.
    pub rerequest_addr: SocketAddrV4,
    /// Local interface for multicast (unspecified = OS default).
    pub interface: Ipv4Addr,
    /// How many recent messages are kept for retransmission.
    pub history_limit: usize,
    /// Datagram size limit for batching and retransmissions.
    pub max_packet_size: usize,
    pub heartbeat_interval: Duration,
    /// How long end-of-session is announced (and re-requests served) at shutdown.
    pub end_of_session_linger: Duration,
    /// Journal file. None = state is in memory only and lost if the process dies.
    pub journal: Option<PathBuf>,
    pub fsync: FsyncPolicy,
    /// Log every sequenced and rejected message.
    pub verbose: bool,
}

impl SequencerConfig {
    pub fn new(session: Session) -> Self {
        SequencerConfig {
            session,
            input_addr: "239.0.0.1:5000".parse().unwrap(),
            output_addr: "239.0.0.2:6000".parse().unwrap(),
            rerequest_addr: "0.0.0.0:5001".parse().unwrap(),
            interface: Ipv4Addr::UNSPECIFIED,
            history_limit: 1_000_000,
            max_packet_size: 1400,
            heartbeat_interval: Duration::from_secs(1),
            end_of_session_linger: Duration::from_secs(2),
            journal: None,
            fsync: FsyncPolicy::EveryBatch,
            verbose: false,
        }
    }
}

/// Recently sequenced messages (MoldUDP64 message data), for retransmission.
struct History {
    first_seqno: u64,
    msgs: VecDeque<Vec<u8>>,
    limit: usize,
}

impl History {
    fn push(&mut self, data: Vec<u8>) {
        self.msgs.push_back(data);
        if self.msgs.len() > self.limit {
            self.msgs.pop_front();
            self.first_seqno += 1;
        }
    }

    /// Builds a response packet for `req`, or None if nothing in range is held.
    fn response(&self, req: &RequestPacket, max_packet_size: usize) -> Option<Vec<u8>> {
        let start = req.seqno.checked_sub(self.first_seqno)? as usize;
        if start >= self.msgs.len() || req.count == 0 {
            return None;
        }
        let mut b = PacketBuilder::new(req.session, req.seqno, max_packet_size);
        for data in self.msgs.range(start..).take(req.count as usize) {
            if !b.try_push(data) {
                break; // the client re-requests the rest
            }
        }
        Some(b.finish())
    }
}

/// Per-sender state: the last accepted sender seqno and recent outcomes.
#[derive(Debug)]
struct SenderRecord {
    last: u64,
    /// Sender seqno of `outcomes[0]`.
    first_outcome: u64,
    outcomes: VecDeque<Feedback>,
}

impl SenderRecord {
    fn new() -> Self {
        SenderRecord { last: 0, first_outcome: 1, outcomes: VecDeque::new() }
    }

    /// Records the outcome of sender seqno `last + 1`.
    fn record(&mut self, outcome: Feedback) {
        self.last += 1;
        self.outcomes.push_back(outcome);
        if self.outcomes.len() > OUTCOME_HISTORY {
            self.outcomes.pop_front();
            self.first_outcome += 1;
        }
    }

    fn outcome(&self, sender_seqno: u64) -> Option<&Feedback> {
        self.outcomes.get(sender_seqno.checked_sub(self.first_outcome)? as usize)
    }
}

/// Packets and feedback to send once a batch is durable.
#[derive(Default)]
struct Outgoing {
    packets: Vec<Vec<u8>>,
    feedback: Vec<(Vec<u8>, SocketAddr)>,
}

/// All sequencing state. Owned by the sequencing thread.
struct Core {
    session: Session,
    max_packet_size: usize,
    verbose: bool,
    senders: HashMap<SenderKey, SenderRecord>,
    /// target ID → last forwarded target seq.
    targets: HashMap<u32, u64>,
    next_seqno: u64,
    history: Arc<Mutex<History>>,
    journal: Option<Journal>,
}

impl Core {
    /// Rebuilds state from journal records.
    fn replay(&mut self, records: Vec<Record>) -> io::Result<()> {
        let mut history = self.history.lock().unwrap();
        for rec in records {
            match rec {
                Record::Accepted { key, sender_seqno, data } => {
                    let m = SequencedMessage::decode(&data)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad message in journal"))?;
                    self.targets.insert(m.target_id, m.target_seq);
                    let sender = self.senders.entry(key).or_insert_with(SenderRecord::new);
                    debug_assert_eq!(sender.last + 1, sender_seqno);
                    sender.record(Feedback::Accepted { sender_seqno, global_seqno: self.next_seqno });
                    history.push(data);
                    self.next_seqno += 1;
                }
                Record::Rejected { key, sender_seqno, target_id, rejected_target_seq, current_target_seq } => {
                    let sender = self.senders.entry(key).or_insert_with(SenderRecord::new);
                    sender.record(Feedback::Reject { target_id, rejected_target_seq, current_target_seq, sender_seqno });
                }
                Record::EndOfSession => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("session {} has already ended; start a new session", self.session),
                    ));
                }
            }
        }
        Ok(())
    }

    fn journal(&mut self, rec: &Record) -> io::Result<()> {
        match &mut self.journal {
            Some(j) => j.append(rec),
            None => Ok(()),
        }
    }

    /// Processes a batch of client messages. Nothing in the returned
    /// `Outgoing` may be sent before this returns (the journal is committed).
    fn process_batch(&mut self, msgs: Vec<(ClientMessage, SocketAddr)>) -> io::Result<Outgoing> {
        let mut out = Outgoing::default();
        let mut accepted = Vec::new();
        let mut batch = PacketBuilder::new(self.session, self.next_seqno, self.max_packet_size);

        for (msg, src) in msgs {
            let key = SenderKey { sender_id: msg.sender_id, incarnation: msg.incarnation };
            let sender = self.senders.entry(key).or_insert_with(SenderRecord::new);

            if msg.sender_seqno <= sender.last {
                // A resend of something already decided: repeat the outcome.
                if let Some(o) = sender.outcome(msg.sender_seqno) {
                    out.feedback.push((o.encode(), src));
                }
                continue;
            }
            if msg.sender_seqno > sender.last + 1 {
                eprintln!(
                    "Gap from sender {}: expected {}, got {}; sending NAK",
                    msg.sender_id,
                    sender.last + 1,
                    msg.sender_seqno
                );
                out.feedback.push((Feedback::Nak { expected: sender.last + 1 }.encode(), src));
                continue;
            }

            let last_target_seq = self.targets.entry(msg.target_id).or_insert(0);
            if msg.target_seq == *last_target_seq + 1 {
                *last_target_seq = msg.target_seq;
                let global_seqno = self.next_seqno;
                if self.verbose {
                    println!(
                        "Sequenced #{}: sender {} → target {} (target seq {})",
                        global_seqno, msg.sender_id, msg.target_id, msg.target_seq
                    );
                }
                sender.record(Feedback::Accepted { sender_seqno: msg.sender_seqno, global_seqno });
                let data = SequencedMessage {
                    sender_id: msg.sender_id,
                    target_id: msg.target_id,
                    target_seq: msg.target_seq,
                    payload: msg.payload,
                }
                .encode();
                self.journal(&Record::Accepted { key, sender_seqno: msg.sender_seqno, data: data.clone() })?;
                if !batch.try_push(&data) {
                    let full = std::mem::replace(
                        &mut batch,
                        PacketBuilder::new(self.session, global_seqno, self.max_packet_size),
                    );
                    out.packets.push(full.finish());
                    assert!(batch.try_push(&data));
                }
                accepted.push(data);
                self.next_seqno += 1;
            } else {
                // Lost the race (stale) or claimed a slot too far ahead: tell the sender.
                let current = *last_target_seq;
                if self.verbose {
                    println!(
                        "Rejecting sender {} for target {}: claimed {}, target is at {}",
                        msg.sender_id, msg.target_id, msg.target_seq, current
                    );
                }
                let reject = Feedback::Reject {
                    target_id: msg.target_id,
                    rejected_target_seq: msg.target_seq,
                    current_target_seq: current,
                    sender_seqno: msg.sender_seqno,
                };
                out.feedback.push((reject.encode(), src));
                sender.record(reject);
                self.journal(&Record::Rejected {
                    key,
                    sender_seqno: msg.sender_seqno,
                    target_id: msg.target_id,
                    rejected_target_seq: msg.target_seq,
                    current_target_seq: current,
                })?;
            }
        }
        if !batch.is_empty() {
            out.packets.push(batch.finish());
        }

        if let Some(j) = &mut self.journal {
            j.commit()?;
        }
        // Only now can re-requests see these messages.
        let mut history = self.history.lock().unwrap();
        for data in accepted {
            history.push(data);
        }
        Ok(out)
    }
}

enum Stop {
    /// End the session: journal it, announce end-of-session, linger.
    Graceful,
    /// Stop as if the process died (no end-of-session); the session can be resumed.
    Abort,
}

/// A running sequencer. Dropping it shuts it down gracefully.
pub struct SequencerHandle {
    input_local_addr: SocketAddr,
    rerequest_local_addr: SocketAddr,
    next_seqno_at_start: u64,
    shutdown_tx: Option<Sender<Stop>>,
    running: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl SequencerHandle {
    /// The bound input address (useful when the configured port was 0).
    pub fn input_local_addr(&self) -> SocketAddr {
        self.input_local_addr
    }

    /// The bound re-request server address (useful when the configured port was 0).
    pub fn rerequest_local_addr(&self) -> SocketAddr {
        self.rerequest_local_addr
    }

    /// The seqno the first message sequenced by this run gets (1 unless resumed from a journal).
    pub fn next_seqno_at_start(&self) -> u64 {
        self.next_seqno_at_start
    }

    /// True if the sequencer stopped on its own (e.g. a journal write failed).
    pub fn has_failed(&self) -> bool {
        self.threads.first().is_none_or(|t| t.is_finished())
    }

    /// Ends the session: stops accepting messages, announces end-of-session for
    /// the linger period while still serving re-requests, then stops.
    pub fn shutdown(mut self) {
        self.stop(Stop::Graceful);
    }

    /// Stops immediately without ending the session, as a crash would. With a
    /// journal, a new sequencer on the same session resumes from here.
    pub fn abort(mut self) {
        self.stop(Stop::Abort);
    }

    fn stop(&mut self, how: Stop) {
        let Some(tx) = self.shutdown_tx.take() else { return };
        let _ = tx.send(how);
        // The sequencing thread (first) may linger sending end-of-session; the
        // others keep serving until it is done.
        let mut threads = self.threads.drain(..);
        if let Some(seq) = threads.next() {
            let _ = seq.join();
        }
        self.running.store(false, Ordering::SeqCst);
        for t in threads {
            let _ = t.join();
        }
    }
}

impl Drop for SequencerHandle {
    fn drop(&mut self) {
        self.stop(Stop::Graceful);
    }
}

pub fn start(cfg: SequencerConfig) -> io::Result<SequencerHandle> {
    let history = Arc::new(Mutex::new(History {
        first_seqno: 1,
        msgs: VecDeque::new(),
        limit: cfg.history_limit.max(1),
    }));
    let mut core = Core {
        session: cfg.session,
        max_packet_size: cfg.max_packet_size,
        verbose: cfg.verbose,
        senders: HashMap::new(),
        targets: HashMap::new(),
        next_seqno: 1,
        history: Arc::clone(&history),
        journal: None,
    };
    if let Some(path) = &cfg.journal {
        let (journal, recovered) = Journal::open(path, cfg.session, cfg.fsync)?;
        if recovered.truncated_bytes > 0 {
            eprintln!("Journal: discarded {} bytes of torn/corrupt tail", recovered.truncated_bytes);
        }
        let n = recovered.records.len();
        core.replay(recovered.records)?;
        if n > 0 {
            println!("Journal: resumed session {} at seqno {} ({} records)", cfg.session, core.next_seqno, n);
        }
        core.journal = Some(journal);
    }
    let next_seqno_at_start = core.next_seqno;

    let recv_socket = bind_receiver(cfg.input_addr, cfg.interface)?;
    recv_socket.set_read_timeout(Some(POLL_INTERVAL))?;
    let send_socket = bind_sender(cfg.output_addr, cfg.interface)?;
    let rereq_socket = UdpSocket::bind(cfg.rerequest_addr)?;
    rereq_socket.set_read_timeout(Some(POLL_INTERVAL))?;

    let input_local_addr = recv_socket.local_addr()?;
    let rerequest_local_addr = rereq_socket.local_addr()?;
    let running = Arc::new(AtomicBool::new(true));
    let (msg_tx, msg_rx) = unbounded::<(ClientMessage, SocketAddr)>();
    let (shutdown_tx, shutdown_rx) = bounded::<Stop>(1);

    let sequencing = {
        let cfg = cfg.clone();
        thread::spawn(move || sequencing_loop(cfg, core, send_socket, msg_rx, shutdown_rx))
    };
    let receiver = {
        let running = Arc::clone(&running);
        thread::spawn(move || receive_loop(recv_socket, msg_tx, running))
    };
    let rerequest = {
        let running = Arc::clone(&running);
        let (session, max, verbose) = (cfg.session, cfg.max_packet_size, cfg.verbose);
        thread::spawn(move || rerequest_loop(rereq_socket, session, history, max, verbose, running))
    };

    Ok(SequencerHandle {
        input_local_addr,
        rerequest_local_addr,
        next_seqno_at_start,
        shutdown_tx: Some(shutdown_tx),
        running,
        // The sequencing thread must be first; see `stop` and `has_failed`.
        threads: vec![sequencing, receiver, rerequest],
    })
}

fn is_timeout(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
}

/// Parses client messages and hands them to the sequencing thread.
fn receive_loop(socket: UdpSocket, tx: Sender<(ClientMessage, SocketAddr)>, running: Arc<AtomicBool>) {
    let mut buf = vec![0u8; RECV_BUFFER_SIZE];
    while running.load(Ordering::SeqCst) {
        let (n, src) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(e) if is_timeout(&e) => continue,
            Err(e) => {
                eprintln!("UDP receive error: {}", e);
                continue;
            }
        };
        match ClientMessage::decode(&buf[..n]) {
            Some(msg) => {
                if tx.send((msg, src)).is_err() {
                    return; // sequencing thread has stopped
                }
            }
            None => eprintln!("Failed to parse {} byte message from {}", n, src),
        }
    }
}

/// Batches client messages through `Core`, then publishes.
fn sequencing_loop(
    cfg: SequencerConfig,
    mut core: Core,
    socket: UdpSocket,
    rx: Receiver<(ClientMessage, SocketAddr)>,
    shutdown_rx: Receiver<Stop>,
) {
    let session = cfg.session;
    let out = cfg.output_addr;
    let mut last_sent = Instant::now();
    let ticker = tick(cfg.heartbeat_interval);

    let send_to = |bytes: &[u8], dest: SocketAddr, what: &str| {
        if let Err(e) = socket.send_to(bytes, dest) {
            // Downstream messages are in history, so receivers can still recover them.
            eprintln!("{} send error to {}: {}", what, dest, e);
        }
    };

    println!("Session {} started", session);
    let stop = loop {
        select! {
            recv(rx) -> first => {
                let Ok(first) = first else { break Stop::Abort };
                let mut items = vec![first];
                while items.len() < MAX_BATCH {
                    match rx.try_recv() {
                        Ok(item) => items.push(item),
                        Err(_) => break,
                    }
                }
                let outgoing = match core.process_batch(items) {
                    Ok(o) => o,
                    Err(e) => {
                        // Can't make decisions durable: stop rather than publish them.
                        eprintln!("Journal write failed, stopping without ending the session: {}", e);
                        return;
                    }
                };
                for p in &outgoing.packets {
                    send_to(p, out.into(), "Downstream");
                }
                for (fb, src) in &outgoing.feedback {
                    send_to(fb, *src, "Feedback");
                }
                if !outgoing.packets.is_empty() {
                    last_sent = Instant::now();
                }
            }

            recv(ticker) -> _ => {
                if last_sent.elapsed() >= cfg.heartbeat_interval {
                    send_to(&MoldPacket::heartbeat(session, core.next_seqno), out.into(), "Heartbeat");
                    last_sent = Instant::now();
                }
            }

            recv(shutdown_rx) -> how => break how.unwrap_or(Stop::Graceful),
        }
    };
    if let Stop::Abort = stop {
        return;
    }

    // End of session: journal it, then announce it in place of heartbeats for
    // the linger period while the re-request server keeps answering.
    if let Some(j) = &mut core.journal {
        if let Err(e) = j.append(&Record::EndOfSession).and_then(|_| j.commit()) {
            eprintln!("Could not journal end of session: {}", e);
        }
    }
    println!("Session {} ended at seqno {}", session, core.next_seqno);
    let eos = MoldPacket::end_of_session(session, core.next_seqno);
    let deadline = Instant::now() + cfg.end_of_session_linger;
    let interval = cfg.heartbeat_interval.min(Duration::from_millis(250));
    loop {
        send_to(&eos, out.into(), "End-of-session");
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        thread::sleep(interval.min(deadline - now));
    }
}

/// Answers MoldUDP64 request packets from history.
fn rerequest_loop(
    socket: UdpSocket,
    session: Session,
    history: Arc<Mutex<History>>,
    max_packet_size: usize,
    verbose: bool,
    running: Arc<AtomicBool>,
) {
    let mut buf = [0u8; 64];
    while running.load(Ordering::SeqCst) {
        let (n, src) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(e) if is_timeout(&e) => continue,
            Err(e) => {
                eprintln!("Re-request receive error: {}", e);
                continue;
            }
        };
        let Some(req) = RequestPacket::decode(&buf[..n]) else {
            eprintln!("Malformed {} byte re-request from {}", n, src);
            continue;
        };
        if req.session != session {
            eprintln!("Re-request from {} for unknown session {}", src, req.session);
            continue;
        }
        let response = history.lock().unwrap().response(&req, max_packet_size);
        match response {
            Some(pkt) => {
                if verbose {
                    println!("Retransmitting from #{} (asked for {}) to {}", req.seqno, req.count, src);
                }
                if let Err(e) = socket.send_to(&pkt, src) {
                    eprintln!("Retransmission to {} failed: {}", src, e);
                }
            }
            None => eprintln!("Re-request from {} for #{} (+{}) not in history", src, req.seqno, req.count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::MoldKind;

    #[test]
    fn history_eviction_and_responses() {
        let session = Session::new("H").unwrap();
        let mut h = History { first_seqno: 1, msgs: VecDeque::new(), limit: 3 };
        for i in 1..=5u8 {
            h.push(vec![i; 4]);
        }
        // Holds seqnos 3, 4, 5.
        assert_eq!(h.first_seqno, 3);
        let req = |seqno, count| RequestPacket { session, seqno, count };
        assert_eq!(h.response(&req(2, 1), 1400), None); // evicted
        assert_eq!(h.response(&req(6, 1), 1400), None); // not yet sequenced
        assert_eq!(h.response(&req(3, 0), 1400), None);

        let p = MoldPacket::decode(&h.response(&req(4, 10), 1400).unwrap()).unwrap();
        assert_eq!(p.seqno, 4);
        assert_eq!(p.kind, MoldKind::Messages(vec![vec![4; 4], vec![5; 4]]));

        // Size-limited: 20 header + one 6-byte block.
        let p = MoldPacket::decode(&h.response(&req(3, 3), 26).unwrap()).unwrap();
        assert_eq!(p.message_count(), 1);
    }

    fn core() -> Core {
        Core {
            session: Session::new("CORE").unwrap(),
            max_packet_size: 1400,
            verbose: false,
            senders: HashMap::new(),
            targets: HashMap::new(),
            next_seqno: 1,
            history: Arc::new(Mutex::new(History { first_seqno: 1, msgs: VecDeque::new(), limit: 100 })),
            journal: None,
        }
    }

    fn msg(sender_id: u32, incarnation: u64, sender_seqno: u64, target_seq: u64) -> (ClientMessage, SocketAddr) {
        let m = ClientMessage { sender_id, incarnation, sender_seqno, target_id: 9, target_seq, payload: vec![] };
        (m, "127.0.0.1:1".parse().unwrap())
    }

    fn feedback(out: &Outgoing) -> Vec<Feedback> {
        out.feedback.iter().map(|(b, _)| Feedback::decode(b).unwrap()).collect()
    }

    #[test]
    fn duplicates_get_their_original_outcome() {
        let mut c = core();
        let out = c.process_batch(vec![msg(1, 7, 1, 1), msg(1, 7, 2, 1)]).unwrap();
        assert_eq!(out.packets.len(), 1);
        let reject = Feedback::Reject { target_id: 9, rejected_target_seq: 1, current_target_seq: 1, sender_seqno: 2 };
        assert_eq!(feedback(&out), [reject.clone()]);

        // Resends of both: nothing new is sequenced, outcomes are repeated.
        let out = c.process_batch(vec![msg(1, 7, 1, 1), msg(1, 7, 2, 1)]).unwrap();
        assert!(out.packets.is_empty());
        assert_eq!(feedback(&out), [Feedback::Accepted { sender_seqno: 1, global_seqno: 1 }, reject]);
        assert_eq!(c.next_seqno, 2);
    }

    #[test]
    fn new_incarnation_starts_fresh_and_gaps_are_naked() {
        let mut c = core();
        c.process_batch(vec![msg(1, 7, 1, 1), msg(1, 7, 2, 2)]).unwrap();
        // Same sender ID, restarted: seqno 1 again is a new message, not a duplicate.
        let out = c.process_batch(vec![msg(1, 8, 1, 3)]).unwrap();
        assert_eq!(out.packets.len(), 1);
        assert_eq!(c.next_seqno, 4);
        // A gap for that incarnation is NAKed.
        let out = c.process_batch(vec![msg(1, 8, 3, 4)]).unwrap();
        assert_eq!(feedback(&out), [Feedback::Nak { expected: 2 }]);
    }

    #[test]
    fn replay_rebuilds_state() {
        let mut a = core();
        let records: Vec<Record> = {
            // Run some traffic and capture what would be journaled.
            let key = SenderKey { sender_id: 1, incarnation: 7 };
            a.process_batch(vec![msg(1, 7, 1, 1), msg(1, 7, 2, 1), msg(1, 7, 3, 2)]).unwrap();
            vec![
                Record::Accepted {
                    key,
                    sender_seqno: 1,
                    data: SequencedMessage { sender_id: 1, target_id: 9, target_seq: 1, payload: vec![] }.encode(),
                },
                Record::Rejected { key, sender_seqno: 2, target_id: 9, rejected_target_seq: 1, current_target_seq: 1 },
                Record::Accepted {
                    key,
                    sender_seqno: 3,
                    data: SequencedMessage { sender_id: 1, target_id: 9, target_seq: 2, payload: vec![] }.encode(),
                },
            ]
        };
        let mut b = core();
        b.replay(records).unwrap();
        assert_eq!(b.next_seqno, a.next_seqno);
        assert_eq!(b.targets, a.targets);
        assert_eq!(b.history.lock().unwrap().msgs, a.history.lock().unwrap().msgs);
        // A resend after "restart" gets the original answer.
        let out = b.process_batch(vec![msg(1, 7, 3, 2)]).unwrap();
        assert_eq!(feedback(&out), [Feedback::Accepted { sender_seqno: 3, global_seqno: 2 }]);

        let mut c = core();
        assert!(c.replay(vec![Record::EndOfSession]).is_err());
    }
}
