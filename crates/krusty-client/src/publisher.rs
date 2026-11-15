//! The publisher: a `krusty-session` session over UDP. Every submitted
//! message ends in exactly one event (gap K3).

use krusty_session::{
    Config as SessionConfig, Event as SessionEvent, FailReason, Session, SubmitError,
};
use krusty_wire::feedback::Feedback;
use krusty_wire::ingress::{Append, Submit};
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct PublisherConfig {
    pub client_id: u32,
    /// Nodes' ingress addresses, in node id order (`nodes[k]` is node
    /// k + 1). After NOT_LEADER the publisher goes to the leader it names
    /// and resends there; NOT_LEADERs naming the node it already uses
    /// answer earlier transmissions and are ignored. A NOT_LEADER that
    /// names no leader moves it to the next node, at most once per
    /// `retry_interval_ns`. After `failover_ns` without any answer while
    /// messages are outstanding, it tries the next node.
    pub nodes: Vec<SocketAddrV4>,
    pub failover_ns: u64,
    /// Most unanswered messages (≤ the cluster's outcome ring).
    pub window: usize,
    pub retry_interval_ns: u64,
    pub max_attempts: u32,
    /// Largest payload.
    pub max_payload: usize,
}

impl PublisherConfig {
    pub fn new(client_id: u32, nodes: Vec<SocketAddrV4>) -> PublisherConfig {
        PublisherConfig {
            client_id,
            nodes,
            failover_ns: 60_000_000,
            window: 64,
            retry_interval_ns: 20_000_000,
            max_attempts: 50,
            max_payload: 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Accepted {
        client_seqno: u64,
        global_seqno: u64,
    },
    /// A conditional append lost: `key` is at `current_version`.
    Rejected {
        client_seqno: u64,
        key: u64,
        current_version: u64,
    },
    /// Unknown fate: maybe sequenced, maybe not.
    Failed {
        client_seqno: u64,
        reason: FailReason,
    },
    /// The session restarted with a new incarnation (after SESSION_EXPIRED);
    /// client seqnos start again at 1.
    Restarted { incarnation: u64 },
}

pub struct Publisher {
    cfg: PublisherConfig,
    sock: UdpSocket,
    session: Session,
    node: usize,
    /// When feedback last arrived (or the node last changed).
    heard_at: u64,
    /// When a NOT_LEADER without a leader last moved us on.
    rotated_at: u64,
    /// Payload copies for resends: `window` slots of `max_payload` bytes.
    payloads: Vec<u8>,
    lens: Vec<usize>,
    submitted: u64,
    start: Instant,
    tx: Vec<krusty_session::Transmit>,
    ev: Vec<SessionEvent>,
    buf: Vec<u8>,
}

/// A fresh incarnation: the wall clock in ns (ADR-12: incarnations increase).
fn incarnation_now(at_least: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);
    now.max(at_least).max(1)
}

impl Publisher {
    pub fn new(cfg: PublisherConfig) -> io::Result<Publisher> {
        assert!(!cfg.nodes.is_empty());
        let sock = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))?;
        let _ = socket2::SockRef::from(&sock).set_recv_buffer_size(8 << 20);
        sock.set_nonblocking(true)?;
        let session = Session::new(
            SessionConfig {
                client_id: cfg.client_id,
                window: cfg.window,
                retry_interval_ns: cfg.retry_interval_ns,
                max_attempts: cfg.max_attempts,
            },
            incarnation_now(1),
        );
        Ok(Publisher {
            payloads: vec![0; cfg.window * cfg.max_payload],
            lens: vec![0; cfg.window],
            submitted: 0,
            start: Instant::now(),
            tx: Vec::with_capacity(cfg.window),
            ev: Vec::with_capacity(cfg.window),
            buf: vec![0; 65_536],
            node: 0,
            heard_at: 0,
            rotated_at: 0,
            cfg,
            sock,
            session,
        })
    }

    pub fn incarnation(&self) -> u64 {
        self.session.incarnation()
    }

    /// Messages submitted and not finished.
    pub fn in_flight(&self) -> usize {
        self.session.in_flight()
    }

    /// Queues a message; it goes out at the next `poll`. Returns its client
    /// seqno. `WindowFull`: wait for outcomes (call `poll`).
    pub fn submit(&mut self, append: Append, payload: &[u8]) -> Result<u64, SubmitError> {
        assert!(payload.len() <= self.cfg.max_payload, "payload too large");
        // At most `window` messages are unfinished, so slot k % window is free.
        let slot = (self.submitted % self.cfg.window as u64) as usize;
        let seqno = self.session.submit(append, slot as u64)?;
        let at = slot * self.cfg.max_payload;
        self.payloads[at..at + payload.len()].copy_from_slice(payload);
        self.lens[slot] = payload.len();
        self.submitted += 1;
        Ok(seqno)
    }

    /// Sends what is due, reads feedback, and reports finished messages.
    pub fn poll(&mut self, mut f: impl FnMut(Event)) -> io::Result<()> {
        let now = self.start.elapsed().as_nanos() as u64;
        if let Some(min) = self.session.must_restart() {
            self.ev.clear();
            self.session.restart(incarnation_now(min), &mut self.ev);
            self.report(&mut f);
            f(Event::Restarted {
                incarnation: self.session.incarnation(),
            });
        }
        self.tx.clear();
        self.ev.clear();
        self.session.poll(now, &mut self.tx, &mut self.ev);
        let to = self.cfg.nodes[self.node % self.cfg.nodes.len()];
        for t in &self.tx {
            let slot = t.handle as usize;
            let at = slot * self.cfg.max_payload;
            let s = Submit {
                client_id: self.cfg.client_id,
                incarnation: t.incarnation,
                client_seqno: t.client_seqno,
                append: t.append,
                payload: &self.payloads[at..at + self.lens[slot]],
            };
            let n = s.encode(&mut self.buf).unwrap();
            match self.sock.send_to(&self.buf[..n], to) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {} // resent later
                Err(e) => return Err(e),
            }
        }
        loop {
            let n = match self.sock.recv(&mut self.buf) {
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            };
            if let Ok(fb) = Feedback::decode(&self.buf[..n]) {
                self.heard_at = now;
                if let krusty_wire::feedback::Outcome::NotLeader { leader } = fb.outcome {
                    // A window of messages to a follower comes back as a
                    // burst of NOT_LEADERs: switch (and resend) once.
                    let k = self.cfg.nodes.len();
                    let current = self.node % k;
                    let next = if (1..=k as u64).contains(&leader) {
                        (leader - 1) as usize
                    } else if now.saturating_sub(self.rotated_at) >= self.cfg.retry_interval_ns {
                        self.rotated_at = now;
                        current + 1
                    } else {
                        current
                    };
                    if next % k == current {
                        continue; // the ordinary retries go on
                    }
                    self.node = next;
                }
                self.session.on_feedback(&fb, now, &mut self.ev);
            }
        }
        // Silence while messages are outstanding: the node may be gone.
        if self.session.in_flight() == 0 {
            self.heard_at = now;
        } else if self.cfg.nodes.len() > 1
            && self.session.in_flight() > 0
            && now.saturating_sub(self.heard_at) > self.cfg.failover_ns
        {
            self.node += 1;
            self.heard_at = now;
        }
        self.report(&mut f);
        Ok(())
    }

    fn report(&mut self, f: &mut impl FnMut(Event)) {
        for e in self.ev.drain(..) {
            f(match e {
                SessionEvent::Accepted {
                    client_seqno,
                    global_seqno,
                    ..
                } => Event::Accepted {
                    client_seqno,
                    global_seqno,
                },
                SessionEvent::Rejected {
                    client_seqno,
                    key,
                    current_version,
                    ..
                } => Event::Rejected {
                    client_seqno,
                    key,
                    current_version,
                },
                SessionEvent::Failed {
                    client_seqno,
                    reason,
                    ..
                } => Event::Failed {
                    client_seqno,
                    reason,
                },
            });
        }
    }
}
