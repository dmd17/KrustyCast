//! The consumer: in-order delivery from the native stream, gap recovery
//! with delayed, randomized requests spread across nodes (ARCHITECTURE
//! §8.5), and replay from any position.

use crate::Rng;
use krusty_stream::{
    Config as TrackerConfig, Delivered, Frame as TFrame, GapPolicy, Kind, Tracker, WINDOW,
};
use krusty_wire::stream::{self, Frame, MessageMeta};
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    /// The feed (multicast group or unicast address) to listen on.
    pub feed: SocketAddrV4,
    /// Interface for a multicast feed.
    pub interface: Ipv4Addr,
    /// Nodes' recovery addresses; requests rotate across them.
    pub recovery: Vec<SocketAddrV4>,
    /// Follow only this stream (None: the first one seen).
    pub stream_id: Option<u64>,
    /// Start at this seqno (replay from a position); None: the first frame seen.
    pub start_seqno: Option<u64>,
    /// A newly detected gap waits up to this long (random) before RECOVER,
    /// so many consumers missing the same data do not all ask at once.
    pub max_recover_delay_ns: u64,
    pub retry_interval_ns: u64,
    /// Stall on a gap no node fills, or report it lost after some number of
    /// requests (a policy choice: [`GapPolicy`]).
    pub gap_policy: GapPolicy,
    /// Largest payload to buffer out of order.
    pub max_payload: usize,
    /// Seed for the recovery jitter.
    pub seed: u64,
}

impl ConsumerConfig {
    pub fn new(feed: SocketAddrV4, recovery: Vec<SocketAddrV4>) -> ConsumerConfig {
        ConsumerConfig {
            feed,
            interface: Ipv4Addr::UNSPECIFIED,
            recovery,
            stream_id: None,
            start_seqno: None,
            max_recover_delay_ns: 200_000,
            retry_interval_ns: 20_000_000,
            gap_policy: GapPolicy::GiveUpAfter(50),
            max_payload: 1408,
            seed: 0x5EED,
        }
    }
}

/// One message, in order. `payload` borrows the receive buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivery<'a> {
    pub seqno: u64,
    /// Assigned by the leader and replicated: the same on every consumer.
    pub timestamp_ns: u64,
    pub client_id: u32,
    /// `(key, version reached)` for a conditional append.
    pub conditional: Option<(u64, u64)>,
    pub payload: &'a [u8],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event<'a> {
    Message(Delivery<'a>),
    /// `count` messages before the next delivery are gone for good (aged out
    /// of retention, or recovery gave up).
    Lost {
        count: u64,
    },
    /// The stream ended and everything before the end was handled.
    EndOfStream,
}

struct Slot {
    seqno: u64,
    meta: MessageMeta,
    len: usize,
}

pub struct Consumer {
    cfg: ConsumerConfig,
    feed: UdpSocket,
    /// Sends RECOVER and receives the unicast answers.
    req: UdpSocket,
    tracker: Tracker,
    slots: Vec<Option<Slot>>,
    bytes: Vec<u8>,
    rng: Rng,
    /// A first RECOVER waiting for its random delay: (request, due).
    delayed: Option<(krusty_stream::Recover, u64)>,
    next_node: usize,
    lost_reported: u64,
    ended: bool,
    start: Instant,
    buf: Vec<u8>,
    out: Vec<Delivered>,
}

fn bind_feed(addr: SocketAddrV4, interface: Ipv4Addr) -> io::Result<UdpSocket> {
    let s = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    let _ = s.set_recv_buffer_size(8 << 20);
    if addr.ip().is_multicast() {
        s.set_reuse_address(true)?;
        s.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, addr.port()).into())?;
        s.join_multicast_v4(addr.ip(), &interface)?;
    } else {
        s.bind(&addr.into())?;
    }
    let s: UdpSocket = s.into();
    s.set_nonblocking(true)?;
    Ok(s)
}

impl Consumer {
    pub fn new(cfg: ConsumerConfig) -> io::Result<Consumer> {
        assert!(!cfg.recovery.is_empty(), "at least one recovery address");
        let feed = bind_feed(cfg.feed, cfg.interface)?;
        let req = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))?;
        let _ = socket2::SockRef::from(&req).set_recv_buffer_size(8 << 20);
        req.set_nonblocking(true)?;
        let tracker = Tracker::new(TrackerConfig {
            pinned: cfg.stream_id,
            start_seqno: cfg.start_seqno,
            recover: true,
            retry_interval_ns: cfg.retry_interval_ns,
            gap_policy: cfg.gap_policy,
        });
        Ok(Consumer {
            slots: (0..WINDOW).map(|_| None).collect(),
            bytes: vec![0; WINDOW * cfg.max_payload],
            rng: Rng(cfg.seed),
            delayed: None,
            next_node: (cfg.seed as usize) % cfg.recovery.len(),
            lost_reported: 0,
            ended: false,
            start: Instant::now(),
            buf: vec![0; 65_536],
            out: Vec::with_capacity(4 * WINDOW),
            cfg,
            feed,
            req,
            tracker,
        })
    }

    /// The next seqno to deliver (the consumer's position), once known.
    pub fn position(&self) -> Option<u64> {
        self.tracker.next_seqno()
    }

    /// Messages reported lost so far.
    pub fn lost(&self) -> u64 {
        self.tracker.lost()
    }

    pub fn is_complete(&self) -> bool {
        self.tracker.is_complete()
    }

    /// Receives whatever is queued, recovers gaps, and calls `f` for each
    /// event in order. Never blocks. Returns how many datagrams it handled.
    pub fn poll(&mut self, mut f: impl FnMut(Event<'_>)) -> io::Result<usize> {
        let mut handled = 0;
        // Moved out while frames borrow it (no allocation).
        let mut buf = std::mem::take(&mut self.buf);
        for which in 0..2 {
            loop {
                let sock = if which == 0 { &self.feed } else { &self.req };
                let n = match sock.recv(&mut buf) {
                    Ok(n) => n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        self.buf = buf;
                        return Err(e);
                    }
                };
                handled += 1;
                self.on_datagram(&buf[..n], &mut f);
            }
        }
        self.buf = buf;
        self.recover(&mut f)?;
        if self.tracker.is_complete() && !self.ended {
            self.ended = true;
            f(Event::EndOfStream);
        }
        Ok(handled)
    }

    fn now_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    fn on_datagram(&mut self, bytes: &[u8], f: &mut impl FnMut(Event<'_>)) {
        let Ok(frame) = Frame::decode(bytes) else {
            return;
        };
        let tframe = match frame {
            Frame::Data(d) => {
                // Buffer what the tracker will hold back (ahead of `next`),
                // and only for a stream it follows: after a session roll a
                // late frame of the old stream must not fill a slot of the new.
                let follows = self.tracker.follows(d.stream_id);
                let switching = follows && self.tracker.stream().is_some_and(|s| s != d.stream_id);
                if switching {
                    self.slots.iter_mut().for_each(|s| *s = None);
                }
                let next = if switching {
                    1
                } else {
                    self.tracker.next_seqno().unwrap_or(0)
                };
                for m in d.messages() {
                    if follows
                        && m.seqno > next
                        && m.seqno - next < WINDOW as u64
                        && m.payload.len() <= self.cfg.max_payload
                    {
                        let i = (m.seqno % WINDOW as u64) as usize;
                        let at = i * self.cfg.max_payload;
                        self.bytes[at..at + m.payload.len()].copy_from_slice(m.payload);
                        self.slots[i] = Some(Slot {
                            seqno: m.seqno,
                            meta: m.meta,
                            len: m.payload.len(),
                        });
                    }
                }
                // A retransmission covering a delayed request makes it moot.
                if let Some((r, _)) = self.delayed
                    && d.retransmit
                    && d.seqno <= r.from
                    && d.next_seqno() > r.from
                {
                    self.delayed = None;
                }
                TFrame {
                    stream_id: d.stream_id,
                    seqno: d.seqno,
                    kind: Kind::Data {
                        count: d.count as u64,
                        first_handle: d.seqno,
                    },
                }
            }
            Frame::Heartbeat {
                stream_id,
                next_seqno,
            } => TFrame {
                stream_id,
                seqno: next_seqno,
                kind: Kind::Heartbeat,
            },
            Frame::EndOfSession {
                stream_id,
                next_seqno,
            } => TFrame {
                stream_id,
                seqno: next_seqno,
                kind: Kind::EndOfStream,
            },
            Frame::Unavailable {
                stream_id,
                from,
                earliest_retained,
            } => TFrame {
                stream_id,
                seqno: from,
                kind: Kind::Unavailable { earliest_retained },
            },
            Frame::Recover { .. } => return,
        };
        self.out.clear();
        let before = self.tracker.stream();
        self.tracker.on_frame(&tframe, &mut self.out);
        // A new stream after END_OF_SESSION (a session roll): the old one
        // ended, even if this poll never saw it complete.
        let switched = before.is_some() && self.tracker.stream() != before;
        self.emit(Some(frame), switched, f);
    }

    /// Reports new losses, then the deliveries in `out` (from `frame` when
    /// they were in it, else from the out-of-order store).
    fn emit(&mut self, frame: Option<Frame<'_>>, switched: bool, f: &mut impl FnMut(Event<'_>)) {
        let lost = self.tracker.lost();
        if lost > self.lost_reported {
            f(Event::Lost {
                count: lost - self.lost_reported,
            });
            self.lost_reported = lost;
        }
        if switched {
            if !self.ended {
                f(Event::EndOfStream);
            }
            self.ended = false;
        }
        let mut in_frame = match frame {
            Some(Frame::Data(d)) => Some(d.messages().peekable()),
            _ => None,
        };
        for d in &self.out {
            let mut from_frame = None;
            if let Some(it) = in_frame.as_mut() {
                while let Some(m) = it.peek() {
                    if m.seqno < d.seqno {
                        it.next();
                    } else {
                        break;
                    }
                }
                if let Some(m) = it.peek()
                    && m.seqno == d.seqno
                {
                    from_frame = Some(*m);
                }
            }
            match from_frame {
                Some(m) => f(Event::Message(Delivery {
                    seqno: m.seqno,
                    timestamp_ns: m.meta.timestamp_ns,
                    client_id: m.meta.client_id,
                    conditional: m.meta.conditional,
                    payload: m.payload,
                })),
                None => {
                    let i = (d.seqno % WINDOW as u64) as usize;
                    if let Some(s) = self.slots[i].take()
                        && s.seqno == d.seqno
                    {
                        let at = i * self.cfg.max_payload;
                        f(Event::Message(Delivery {
                            seqno: s.seqno,
                            timestamp_ns: s.meta.timestamp_ns,
                            client_id: s.meta.client_id,
                            conditional: s.meta.conditional,
                            payload: &self.bytes[at..at + s.len],
                        }));
                    }
                }
            }
        }
    }

    fn recover(&mut self, f: &mut impl FnMut(Event<'_>)) -> io::Result<()> {
        let now = self.now_ns();
        self.out.clear();
        let req = self.tracker.poll_recover(now, &mut self.out);
        if !self.out.is_empty() {
            self.emit(None, false, f); // gave up on a gap: what followed is delivered
        }
        match (req, self.delayed) {
            // A new gap: wait a random delay first (suppression, §8.5).
            (Some(r), None) if self.cfg.max_recover_delay_ns > 0 => {
                let delay = self.rng.next() % self.cfg.max_recover_delay_ns;
                self.delayed = Some((r, now + delay));
            }
            (Some(r), _) => self.send(r)?,
            (None, Some((r, due))) if now >= due => {
                self.delayed = None;
                if self.tracker.next_seqno() == Some(r.from) {
                    self.send(r)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn send(&mut self, r: krusty_stream::Recover) -> io::Result<()> {
        let n = stream::encode_recover(&mut self.buf, r.stream_id, r.from, r.count).unwrap();
        let to = self.cfg.recovery[self.next_node % self.cfg.recovery.len()];
        self.next_node += 1; // spread requests (and retries) across nodes
        match self.req.send_to(&self.buf[..n], to) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(e) => Err(e),
        }
    }
}
