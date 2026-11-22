//! KrustyCast for the rig: the client session (`krusty-session`) on the
//! rig's thread, and an I/O thread standing where Aeron's media driver
//! stands. The session's SUBMITs go to the I/O thread through a byte ring;
//! it packs them back to back into datagrams of up to `mtu` bytes
//! (krusty_wire::batch, as the publisher does) and hands each datagram it
//! receives (one or more outcomes) back through a second ring. Nothing
//! allocates per message.
use crate::{CHECKSUM, Transceiver, now_ns, pin};
use krusty_net::{Packed, RecvBatch, SendBatch};
use krusty_ring::byte_ring::{Consumer, Producer, byte_ring};
use krusty_session::{Config, Event, Session, Transmit};
use krusty_wire::feedback::Outcome;
use krusty_wire::ingress::{Append, Submit};
use std::net::{SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Instant;

pub struct Krusty {
    session: Session,
    to_io: Producer,
    from_io: Consumer,
    target: Arc<AtomicUsize>,
    nodes: usize,
    stop: Arc<AtomicBool>,
    io: Option<JoinHandle<()>>,
    /// Scheduled send time of each message, by handle.
    ts: Vec<u64>,
    next_handle: u64,
    tx: Vec<Transmit>,
    ev: Vec<Event>,
    payload: Vec<u8>,
    epoch: Instant,
    rotated_at: u64,
    retry_ns: u64,
    pub failed: u64,
}

impl Krusty {
    pub fn start(
        nodes: Vec<SocketAddrV4>,
        payload: usize,
        mtu: usize,
        window: usize,
        retry_ms: u64,
        io_cpu: Option<usize>,
        epoch: Instant,
    ) -> Krusty {
        assert!(
            !nodes.is_empty(),
            "--nodes: the ingress addresses, in node id order"
        );
        let (to_io, io_in) = byte_ring(1 << 22);
        let (io_out, from_io) = byte_ring(1 << 22);
        let target = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
        sock.set_nonblocking(true).unwrap();
        let (t, s, n) = (target.clone(), stop.clone(), nodes.clone());
        let io = std::thread::Builder::new()
            .name("krusty-io".into())
            .spawn(move || {
                pin(io_cpu);
                io_loop(sock, n, t, s, io_in, io_out, mtu)
            })
            .unwrap();
        let incarnation = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Krusty {
            session: Session::new(
                Config {
                    client_id: 1,
                    window,
                    retry_interval_ns: retry_ms * 1_000_000,
                    max_attempts: 1_000_000,
                },
                incarnation,
            ),
            to_io,
            from_io,
            target,
            nodes: nodes.len(),
            stop,
            io: Some(io),
            ts: vec![0; 4 * window],
            next_handle: 0,
            tx: Vec::with_capacity(window),
            ev: Vec::with_capacity(window),
            payload: vec![0u8; payload],
            epoch,
            rotated_at: 0,
            retry_ns: retry_ms * 1_000_000,
            failed: 0,
        }
    }

    /// Hands what the session wants sent to the I/O thread.
    fn transmit(&mut self, got: &mut dyn FnMut(u64)) {
        let now = now_ns(self.epoch);
        self.tx.clear();
        self.session.poll(now, &mut self.tx, &mut self.ev);
        let n = self.payload.len();
        for t in &self.tx {
            self.payload[0..8].copy_from_slice(
                &self.ts[(t.handle % self.ts.len() as u64) as usize].to_le_bytes(),
            );
            self.payload[n - 8..].copy_from_slice(&CHECKSUM.to_le_bytes());
            let s = Submit {
                client_id: 1,
                incarnation: t.incarnation,
                client_seqno: t.client_seqno,
                append: t.append,
                payload: &self.payload,
            };
            // A full ring drops it; the session resends.
            self.to_io.try_write(1, s.encoded_len(), |b| {
                s.encode(b).unwrap();
            });
        }
        self.to_io.publish();
        self.report(got);
    }

    fn report(&mut self, got: &mut dyn FnMut(u64)) {
        for e in self.ev.drain(..) {
            match e {
                Event::Accepted { handle, .. } => {
                    got(self.ts[(handle % self.ts.len() as u64) as usize])
                }
                _ => self.failed += 1,
            }
        }
    }
}

impl Transceiver for Krusty {
    fn send(&mut self, n: usize, ts: u64) -> usize {
        let mut k = 0;
        while k < n {
            let h = self.next_handle;
            if self.session.submit(Append::Unconditional, h).is_err() {
                break; // window full: the rig retries
            }
            let len = self.ts.len() as u64;
            self.ts[(h % len) as usize] = ts;
            self.next_handle += 1;
            k += 1;
        }
        if k > 0 {
            self.transmit(&mut |_| {});
        }
        k
    }

    fn receive(&mut self, got: &mut dyn FnMut(u64)) {
        let now = now_ns(self.epoch);
        let (session, ev, target, nodes) =
            (&mut self.session, &mut self.ev, &self.target, self.nodes);
        let (rotated_at, retry_ns) = (&mut self.rotated_at, self.retry_ns);
        self.from_io.read(64, |_, d| {
            for f in krusty_wire::batch::feedbacks(d).flatten() {
                if let Outcome::NotLeader { leader } = f.outcome {
                    // As the publisher: follow the named leader once.
                    let current = target.load(Ordering::Relaxed);
                    let next = if (1..=nodes as u64).contains(&leader) {
                        (leader - 1) as usize
                    } else if now.saturating_sub(*rotated_at) >= retry_ns {
                        *rotated_at = now;
                        (current + 1) % nodes
                    } else {
                        current
                    };
                    if next == current {
                        continue;
                    }
                    target.store(next, Ordering::Relaxed);
                }
                session.on_feedback(&f, now, ev);
            }
        });
        self.report(got);
        // Resends that came due (timer, NAK, NOT_LEADER).
        self.transmit(got);
    }
}

impl Drop for Krusty {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.io.take() {
            let _ = h.join();
        }
    }
}

fn io_loop(
    sock: UdpSocket,
    nodes: Vec<SocketAddrV4>,
    target: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    mut input: Consumer,
    mut output: Producer,
    mtu: usize,
) {
    let mut sb = SendBatch::new(64, mtu);
    let mut rb = RecvBatch::new(64, 65_536);
    while !stop.load(Ordering::Acquire) {
        let to = nodes[target.load(Ordering::Relaxed)];
        input.read(1024, |_, rec| {
            let len = rec.len();
            let fill = |b: &mut [u8]| b.copy_from_slice(rec);
            let p = if len <= mtu {
                sb.push_packed(to, len, fill)
            } else {
                Packed::Full
            };
            if p == Packed::Full {
                let _ = sb.flush(&sock);
                sb.push_packed(to, len, |b| b.copy_from_slice(rec));
            }
        });
        if !sb.is_empty() {
            let _ = sb.flush(&sock);
        }
        let n = rb.recv(&sock).unwrap_or(0);
        for i in 0..n {
            let (d, _) = rb.get(i);
            // A full ring drops the datagram; the session resends.
            output.try_write(2, d.len(), |b| b.copy_from_slice(d));
        }
        if n > 0 {
            output.publish();
        }
    }
}
