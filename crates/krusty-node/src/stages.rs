//! The four stages and their wiring.

use crate::{Idle, NodeConfig};
use krusty_core::Core;
use krusty_net::{RecvBatch, SendBatch};
use krusty_ring::byte_ring::{Consumer, Producer, byte_ring};
use krusty_ring::history::History;
use krusty_wire::feedback::{Feedback, Outcome};
use krusty_wire::ingress::Submit;
use krusty_wire::stream::{self, DataFrameWriter, Frame, MessageMeta};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const REC_SUBMIT: u32 = 1;
const REC_FEED: u32 = 1;
const REC_TO: u32 = 2;
/// Address prefix in ring records: ip (4) + port (2) + padding (2).
const ADDR_LEN: usize = 8;
/// History entry header: timestamp, client, flags, key, version.
const HIST_HEADER: usize = 32;
/// Most messages one RECOVER is answered with.
const MAX_RECOVER_ANSWER: u64 = 1024;

/// Counters, readable while the node runs.
#[derive(Default, Debug)]
pub struct Stats {
    pub submits: AtomicU64,
    pub rejected_at_edge: AtomicU64,
    pub busy: AtomicU64,
    pub published: AtomicU64,
    pub frames: AtomicU64,
    pub feedback: AtomicU64,
    pub recover_requests: AtomicU64,
}

pub struct Node {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    ingress_addr: SocketAddrV4,
    recovery_addr: SocketAddrV4,
    pub stats: Arc<Stats>,
}

impl Node {
    pub fn ingress_addr(&self) -> SocketAddrV4 {
        self.ingress_addr
    }

    pub fn recovery_addr(&self) -> SocketAddrV4 {
        self.recovery_addr
    }

    /// Ends the stream (END_OF_SESSION) and stops all stages.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Release);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn v4(a: SocketAddr) -> SocketAddrV4 {
    match a {
        SocketAddr::V4(a) => a,
        SocketAddr::V6(_) => panic!("IPv4 only"),
    }
}

fn put_addr(b: &mut [u8], a: SocketAddrV4) {
    b[..4].copy_from_slice(&u32::from(*a.ip()).to_le_bytes());
    b[4..6].copy_from_slice(&a.port().to_le_bytes());
    b[6..8].fill(0);
}

fn get_addr(b: &[u8]) -> SocketAddrV4 {
    let ip = u32::from_le_bytes(b[..4].try_into().unwrap());
    SocketAddrV4::new(
        Ipv4Addr::from(ip),
        u16::from_le_bytes(b[4..6].try_into().unwrap()),
    )
}

fn pin(core: Option<usize>) {
    if let Some(core) = core {
        // SAFETY: cpu_set_t is plain data; sched_setaffinity only reads it.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(core, &mut set);
            if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) != 0 {
                eprintln!("warning: could not pin a stage to core {core}");
            }
        }
    }
}

/// Waits after an empty poll: spin, or spin a little and then sleep.
fn idle(how: Idle, empty_polls: &mut u32) {
    *empty_polls += 1;
    match how {
        Idle::Spin => std::hint::spin_loop(),
        Idle::Sleep { sleep_us } => {
            if *empty_polls < 1000 {
                std::hint::spin_loop();
            } else {
                std::thread::sleep(Duration::from_micros(sleep_us));
            }
        }
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn bind(addr: SocketAddrV4, interface: Ipv4Addr) -> io::Result<UdpSocket> {
    let s = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    let _ = s.set_recv_buffer_size(8 << 20);
    let _ = s.set_send_buffer_size(8 << 20);
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

fn sender(feed: SocketAddrV4, interface: Ipv4Addr) -> io::Result<UdpSocket> {
    let s = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    let _ = s.set_send_buffer_size(8 << 20);
    if feed.ip().is_multicast() {
        s.set_multicast_ttl_v4(1)?;
        s.set_multicast_loop_v4(true)?;
        if !interface.is_unspecified() {
            s.set_multicast_if_v4(&interface)?;
        }
    }
    s.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into())?;
    Ok(s.into())
}

pub fn start(cfg: NodeConfig) -> io::Result<Node> {
    assert!(
        stream::HEADER_LEN + DataFrameWriter::block_len(cfg.max_payload) <= cfg.mtu,
        "max_payload must fit one message per frame"
    );
    let ingress_sock = bind(cfg.ingress, cfg.interface)?;
    let recovery_sock = bind(cfg.recovery, cfg.interface)?;
    let egress_sock = sender(cfg.feed, cfg.interface)?;
    let ingress_addr = v4(ingress_sock.local_addr()?);
    let recovery_addr = v4(recovery_sock.local_addr()?);

    let (a_tx, a_rx) = byte_ring(cfg.ring_bytes);
    let (b_tx, b_rx) = byte_ring(cfg.ring_bytes);
    let history = Arc::new(History::new(cfg.history, HIST_HEADER + cfg.max_payload));
    let stop = Arc::new(AtomicBool::new(false));
    let core_done = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Stats::default());

    let mut threads = Vec::new();
    {
        let (cfg, stop, stats) = (cfg.clone(), stop.clone(), stats.clone());
        threads.push(spawn("krusty-ingress", cfg.stage_init, move || {
            ingress(cfg, ingress_sock, a_tx, stop, stats)
        }));
    }
    {
        let (cfg, stop, done, h, stats) = (
            cfg.clone(),
            stop.clone(),
            core_done.clone(),
            history.clone(),
            stats.clone(),
        );
        threads.push(spawn("krusty-core", cfg.stage_init, move || {
            core_stage(cfg, a_rx, b_tx, h, stop, done, stats)
        }));
    }
    {
        let (cfg, done) = (cfg.clone(), core_done.clone());
        threads.push(spawn("krusty-egress", cfg.stage_init, move || {
            egress(cfg, egress_sock, b_rx, done)
        }));
    }
    {
        let (cfg, stop, h, stats) = (cfg.clone(), stop.clone(), history.clone(), stats.clone());
        threads.push(spawn("krusty-recovery", cfg.stage_init, move || {
            recovery(cfg, recovery_sock, h, stop, stats)
        }));
    }
    Ok(Node {
        stop,
        threads,
        ingress_addr,
        recovery_addr,
        stats,
    })
}

fn spawn(name: &str, init: Option<fn()>, f: impl FnOnce() + Send + 'static) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            if let Some(init) = init {
                init();
            }
            f()
        })
        .expect("spawn stage")
}

// ---------------------------------------------------------------------------

fn ingress(
    cfg: NodeConfig,
    sock: UdpSocket,
    mut out: Producer,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
) {
    pin(cfg.cores[0]);
    let mut rb = RecvBatch::new(cfg.batch, 65_536);
    let mut busy = SendBatch::new(cfg.batch, krusty_wire::feedback::LEN);
    let mut empty = 0;
    while !stop.load(Ordering::Acquire) {
        let n = rb.recv(&sock).unwrap_or_default();
        if n == 0 {
            idle(cfg.idle, &mut empty);
            continue;
        }
        empty = 0;
        for i in 0..n {
            let (bytes, src) = rb.get(i);
            let s = match Submit::decode(bytes) {
                Ok(s) if s.payload.len() <= cfg.max_payload => s,
                _ => {
                    stats.rejected_at_edge.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            stats.submits.fetch_add(1, Ordering::Relaxed);
            let wrote = out.try_write(REC_SUBMIT, ADDR_LEN + bytes.len(), |b| {
                put_addr(b, src);
                b[ADDR_LEN..].copy_from_slice(bytes);
            });
            if !wrote {
                // Overloaded (G9): tell the client to back off; nothing is lost.
                stats.busy.fetch_add(1, Ordering::Relaxed);
                let f = Feedback {
                    client_id: s.client_id,
                    incarnation: s.incarnation,
                    client_seqno: s.client_seqno,
                    outcome: Outcome::Busy {
                        retry_after_us: 100,
                    },
                };
                busy.push(src, |b| f.encode(b).unwrap());
            }
        }
        out.publish();
        if !busy.is_empty() {
            let _ = busy.flush(&sock);
        }
    }
}

/// History entry: the message's metadata, then its payload.
fn put_history(h: &History, seqno: u64, meta: &MessageMeta, payload: &[u8], scratch: &mut [u8]) {
    let (cond, key, ver) = match meta.conditional {
        Some((k, v)) => (1u32, k, v),
        None => (0, 0, 0),
    };
    scratch[0..8].copy_from_slice(&meta.timestamp_ns.to_le_bytes());
    scratch[8..12].copy_from_slice(&meta.client_id.to_le_bytes());
    scratch[12..16].copy_from_slice(&cond.to_le_bytes());
    scratch[16..24].copy_from_slice(&key.to_le_bytes());
    scratch[24..32].copy_from_slice(&ver.to_le_bytes());
    scratch[HIST_HEADER..HIST_HEADER + payload.len()].copy_from_slice(payload);
    h.push(seqno, &scratch[..HIST_HEADER + payload.len()]);
}

fn parse_history(b: &[u8]) -> (MessageMeta, &[u8]) {
    let u64_at = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    let cond = u32::from_le_bytes(b[12..16].try_into().unwrap()) != 0;
    let meta = MessageMeta {
        timestamp_ns: u64_at(0),
        client_id: u32::from_le_bytes(b[8..12].try_into().unwrap()),
        conditional: if cond {
            Some((u64_at(16), u64_at(24)))
        } else {
            None
        },
    };
    (meta, &b[HIST_HEADER..])
}

/// Writes a record to ring B, waiting for egress if it is full.
fn write_b(out: &mut Producer, kind: u32, len: usize, mut fill: impl FnMut(&mut [u8])) {
    loop {
        if out.try_write(kind, len, &mut fill) {
            return;
        }
        out.publish();
        std::hint::spin_loop();
    }
}

/// Packs messages `from..to` (from the history) into DATA frames on ring B.
#[allow(clippy::too_many_arguments)]
fn emit_frames(
    cfg: &NodeConfig,
    h: &History,
    from: u64,
    to: u64,
    retransmit: bool,
    frame: &mut [u8],
    entry: &mut [u8],
    mut emit: impl FnMut(&[u8]),
) -> u64 {
    let mut s = from;
    let mut frames = 0;
    while s < to {
        let mut w =
            DataFrameWriter::new(&mut frame[..cfg.mtu], cfg.stream_id, s, retransmit).unwrap();
        while s < to {
            let n = match h.read(s, entry) {
                Ok(n) => n,
                Err(_) => break, // overwritten meanwhile: stop here
            };
            let (meta, payload) = parse_history(&entry[..n]);
            if w.push(&meta, payload).is_err() {
                break; // frame full
            }
            s += 1;
        }
        if w.count() == 0 {
            break;
        }
        let n = w.finish().unwrap();
        emit(&frame[..n]);
        frames += 1;
    }
    frames
}

fn core_stage(
    cfg: NodeConfig,
    mut input: Consumer,
    mut out: Producer,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    stats: Arc<Stats>,
) {
    pin(cfg.cores[1]);
    let mut core = Core::new(cfg.core);
    let mut log_index = 0u64;
    let mut scratch = vec![0u8; HIST_HEADER + cfg.max_payload];
    let mut entry = vec![0u8; HIST_HEADER + cfg.max_payload];
    let mut frame = vec![0u8; cfg.mtu];
    let mut fb = [0u8; krusty_wire::feedback::LEN];
    let heartbeat = Duration::from_micros(cfg.heartbeat_us);
    let mut last_sent = Instant::now();
    let mut empty = 0;
    let mut replies = 0u64;
    loop {
        let first = core.next_seqno();
        let ts = now_ns();
        let n = input.read(cfg.batch, |_, rec| {
            let src = get_addr(rec);
            let Ok(s) = Submit::decode(&rec[ADDR_LEN..]) else {
                return;
            };
            log_index += 1;
            let e = core
                .apply(log_index, ts, &s)
                .expect("entries are applied in order");
            if let Some(p) = e.publish {
                put_history(&history, p.seqno, &p.meta, s.payload, &mut scratch);
            }
            if let Some(f) = e.reply {
                replies += 1;
                write_b(
                    &mut out,
                    REC_TO,
                    ADDR_LEN + krusty_wire::feedback::LEN,
                    |b| {
                        put_addr(b, src);
                        f.encode(&mut fb).unwrap();
                        b[ADDR_LEN..].copy_from_slice(&fb);
                    },
                );
            }
        });
        let last = core.next_seqno();
        if last > first {
            // Publish after the batch is decided (P5: nothing goes out early).
            let frames = emit_frames(
                &cfg,
                &history,
                first,
                last,
                false,
                &mut frame,
                &mut entry,
                |f| write_b(&mut out, REC_FEED, f.len(), |b| b.copy_from_slice(f)),
            );
            stats.published.fetch_add(last - first, Ordering::Relaxed);
            stats.frames.fetch_add(frames, Ordering::Relaxed);
            last_sent = Instant::now();
        }
        if n > 0 {
            stats.feedback.fetch_add(replies, Ordering::Relaxed);
            replies = 0;
            out.publish();
            empty = 0;
            continue;
        }
        if stop.load(Ordering::Acquire) {
            let k = stream::encode_end_of_session(&mut frame, cfg.stream_id, core.next_seqno())
                .unwrap();
            write_b(&mut out, REC_FEED, k, |b| b.copy_from_slice(&frame[..k]));
            out.publish();
            done.store(true, Ordering::Release);
            return;
        }
        if last_sent.elapsed() >= heartbeat {
            let k = stream::encode_heartbeat(&mut frame, cfg.stream_id, core.next_seqno()).unwrap();
            write_b(&mut out, REC_FEED, k, |b| b.copy_from_slice(&frame[..k]));
            out.publish();
            last_sent = Instant::now();
        }
        idle(cfg.idle, &mut empty);
    }
}

fn egress(cfg: NodeConfig, sock: UdpSocket, mut input: Consumer, core_done: Arc<AtomicBool>) {
    pin(cfg.cores[2]);
    let mut sb = SendBatch::new(
        cfg.batch,
        cfg.mtu.max(ADDR_LEN + krusty_wire::feedback::LEN),
    );
    let mut empty = 0;
    loop {
        let finished = core_done.load(Ordering::Acquire);
        let n = input.read(cfg.batch, |kind, rec| {
            if sb.is_full() {
                let _ = sb.flush(&sock);
            }
            match kind {
                REC_FEED => {
                    sb.push(cfg.feed, |b| {
                        b[..rec.len()].copy_from_slice(rec);
                        rec.len()
                    });
                }
                _ => {
                    let dest = get_addr(rec);
                    let body = &rec[ADDR_LEN..];
                    sb.push(dest, |b| {
                        b[..body.len()].copy_from_slice(body);
                        body.len()
                    });
                }
            }
        });
        if !sb.is_empty() {
            let _ = sb.flush(&sock);
        }
        if n == 0 {
            if finished && input.is_empty() {
                return;
            }
            idle(cfg.idle, &mut empty);
        } else {
            empty = 0;
        }
    }
}

fn recovery(
    cfg: NodeConfig,
    sock: UdpSocket,
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    stats: Arc<Stats>,
) {
    pin(cfg.cores[3]);
    let mut rb = RecvBatch::new(16, 256);
    let mut sb = SendBatch::new(64, cfg.mtu);
    let mut frame = vec![0u8; cfg.mtu];
    let mut entry = vec![0u8; HIST_HEADER + cfg.max_payload];
    let mut empty = 0;
    while !stop.load(Ordering::Acquire) {
        let n = rb.recv(&sock).unwrap_or(0);
        if n == 0 {
            // Recovery is off the latency path: never spin here.
            idle(Idle::Sleep { sleep_us: 200 }, &mut empty);
            continue;
        }
        empty = 0;
        for i in 0..n {
            let (bytes, src) = rb.get(i);
            let Ok(Frame::Recover { from, count, .. }) = Frame::decode(bytes) else {
                continue;
            };
            stats.recover_requests.fetch_add(1, Ordering::Relaxed);
            let earliest = history.earliest();
            if from < earliest {
                let k =
                    stream::encode_unavailable(&mut frame, cfg.stream_id, from, earliest).unwrap();
                let f = &frame[..k];
                sb.push(src, |b| {
                    b[..f.len()].copy_from_slice(f);
                    f.len()
                });
            }
            let start = from.max(earliest);
            let end = from
                .saturating_add(count.min(MAX_RECOVER_ANSWER))
                .min(history.committed() + 1);
            emit_frames(
                &cfg,
                &history,
                start,
                end,
                true,
                &mut frame,
                &mut entry,
                |f| {
                    if sb.is_full() {
                        let _ = sb.flush(&sock);
                    }
                    sb.push(src, |b| {
                        b[..f.len()].copy_from_slice(f);
                        f.len()
                    });
                },
            );
        }
        let _ = sb.flush(&sock);
    }
}
