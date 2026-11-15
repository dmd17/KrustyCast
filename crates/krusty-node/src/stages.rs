//! The four stages and their wiring.

use crate::metrics::{Metrics, mono_ns};
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const REC_SUBMIT: u32 = 1;
pub(crate) const REC_FEED: u32 = 1;
pub(crate) const REC_TO: u32 = 2;
/// Ring record header: ip (4) + port (2) + padding (2), then the monotonic
/// time the record was queued (8), for the dwell histograms.
pub(crate) const REC_HDR: usize = 16;
/// History entry header: timestamp, client, flags, key, version.
pub(crate) const HIST_HEADER: usize = 32;
/// Most messages one RECOVER is answered with.
const MAX_RECOVER_ANSWER: u64 = 1024;

pub struct Node {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
    ingress_addr: SocketAddrV4,
    recovery_addr: SocketAddrV4,
    mold_rerequest_addr: Option<SocketAddrV4>,
    pub stats: Arc<Metrics>,
}

impl Node {
    pub fn ingress_addr(&self) -> SocketAddrV4 {
        self.ingress_addr
    }

    pub fn recovery_addr(&self) -> SocketAddrV4 {
        self.recovery_addr
    }

    /// The MoldUDP64 re-request server's bound address, if the adapter runs.
    pub fn mold_rerequest_addr(&self) -> Option<SocketAddrV4> {
        self.mold_rerequest_addr
    }

    /// Ends the stream (END_OF_SESSION) and stops all stages.
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Release);
        let debug = std::env::var_os("KRUSTY_DEBUG_STOP").is_some();
        let t0 = Instant::now();
        for t in self.threads.drain(..) {
            let name = t.thread().name().unwrap_or("?").to_string();
            let _ = t.join();
            if debug {
                eprintln!("stop: {name} joined after {:?}", t0.elapsed());
            }
        }
    }
}

fn v4(a: SocketAddr) -> SocketAddrV4 {
    match a {
        SocketAddr::V4(a) => a,
        SocketAddr::V6(_) => panic!("IPv4 only"),
    }
}

pub(crate) fn put_addr(b: &mut [u8], a: SocketAddrV4) {
    b[..4].copy_from_slice(&u32::from(*a.ip()).to_le_bytes());
    b[4..6].copy_from_slice(&a.port().to_le_bytes());
    b[6..8].fill(0);
}

pub(crate) fn put_ts(b: &mut [u8], t: u64) {
    b[8..16].copy_from_slice(&t.to_le_bytes());
}

pub(crate) fn get_ts(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[8..16].try_into().unwrap())
}

pub(crate) fn get_addr(b: &[u8]) -> SocketAddrV4 {
    let ip = u32::from_le_bytes(b[..4].try_into().unwrap());
    SocketAddrV4::new(
        Ipv4Addr::from(ip),
        u16::from_le_bytes(b[4..6].try_into().unwrap()),
    )
}

pub(crate) fn pin(core: Option<usize>) {
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
pub(crate) fn idle(how: Idle, empty_polls: &mut u32) {
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

pub(crate) fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

pub(crate) fn bind(addr: SocketAddrV4, interface: Ipv4Addr) -> io::Result<UdpSocket> {
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
    cfg.validate()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let ingress_sock = bind(cfg.ingress, cfg.interface)?;
    let recovery_sock = bind(cfg.recovery, cfg.interface)?;
    let egress_sock = sender(cfg.feed, cfg.interface)?;
    let ingress_addr = v4(ingress_sock.local_addr()?);
    let recovery_addr = v4(recovery_sock.local_addr()?);

    let (a_tx, a_rx) = byte_ring(cfg.ring_bytes);
    let (b_tx, b_rx) = byte_ring(cfg.ring_bytes);
    let stop = Arc::new(AtomicBool::new(false));
    let core_done = Arc::new(AtomicBool::new(false));
    let store_done = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Metrics::create(cfg.metrics_path.as_deref(), cfg.stream_id)?);
    let mut threads = Vec::new();

    // Who feeds ring B, and when egress may finish.
    let mut disk: Option<krusty_log::LogReader> = None;
    let mut cluster_start: Option<crate::cluster::ClusterStart> = None;
    let leader_flag = Arc::new(AtomicBool::new(cfg.cluster.is_none()));
    let (history, core_start, out, egress_done) = match &cfg.storage {
        Some(st) if cfg.cluster.is_some() => {
            let cl = cfg.cluster.as_ref().unwrap();
            let mut r = crate::cluster::restore(&cfg, st, cl)?;
            eprintln!(
                "krusty-node: node {} restored: snapshot at {}, {} entries after it, next seqno {}",
                cl.id,
                r.snapshot_index,
                r.loaded,
                r.start.core.next_seqno()
            );
            disk = Some(r.log.reader());
            let (s_tx, s_rx) = byte_ring(cfg.ring_bytes);
            let (snap_tx, snap_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1);
            let (back_tx, back_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1);
            r.start.snapshots = Some((snap_tx, back_rx, st.snapshot_every.max(1)));
            let (log, positions) = (r.log, r.positions);
            let (c2, done, sdone, st2) = (
                cfg.clone(),
                core_done.clone(),
                store_done.clone(),
                stats.clone(),
            );
            threads.push(spawn("krusty-store", cfg.stage_init, move || {
                crate::storage::store_stage(
                    c2,
                    log,
                    positions,
                    s_rx,
                    byte_ring(64).0,
                    snap_rx,
                    back_tx,
                    done,
                    sdone,
                    st2,
                )
            }));
            cluster_start = Some(r.start);
            (
                r.history,
                // Unused in a cluster (the cluster core stage starts from `cluster_start`).
                CoreStart {
                    core: Core::new(cfg.core),
                    log_index: 0,
                    last_ts: 0,
                    snapshots: None,
                },
                Out {
                    b: b_tx,
                    s: Some(s_tx),
                    through_storage: false,
                },
                core_done.clone(),
            )
        }
        None => (
            History::new(cfg.history, HIST_HEADER + cfg.max_payload),
            CoreStart {
                core: Core::new(cfg.core),
                log_index: 0,
                last_ts: 0,
                snapshots: None,
            },
            Out {
                b: b_tx,
                s: None,
                through_storage: false,
            },
            core_done.clone(),
        ),
        Some(st) => {
            // Restore from snapshot + log before anything runs.
            let r = crate::storage::restore(&cfg, st)?;
            eprintln!(
                "krusty-node: restored to log index {} (snapshot at {}, {} entries replayed), next seqno {}",
                r.log_index,
                r.snapshot_index,
                r.replayed,
                r.core.next_seqno()
            );
            disk = Some(r.log.reader());
            let sync = st.durability == crate::Durability::Sync;
            let (s_tx, s_rx) = byte_ring(cfg.ring_bytes);
            let (snap_tx, snap_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1);
            let (back_tx, back_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1);
            let core_start = CoreStart {
                core: r.core,
                log_index: r.log_index,
                last_ts: r.last_ts,
                snapshots: Some((snap_tx, back_rx, st.snapshot_every.max(1))),
            };
            // Mode (b): only the storage stage feeds ring B (after fsync).
            let (core_b, store_b) = if sync {
                (byte_ring(64).0, b_tx)
            } else {
                (b_tx, byte_ring(64).0)
            };
            let log = r.log;
            let (c2, done, sdone, st2) = (
                cfg.clone(),
                core_done.clone(),
                store_done.clone(),
                stats.clone(),
            );
            threads.push(spawn("krusty-store", cfg.stage_init, move || {
                crate::storage::store_stage(
                    c2,
                    log,
                    Default::default(),
                    s_rx,
                    store_b,
                    snap_rx,
                    back_tx,
                    done,
                    sdone,
                    st2,
                )
            }));
            let egress_done = if sync {
                store_done.clone()
            } else {
                core_done.clone()
            };
            (
                r.history,
                core_start,
                Out {
                    b: core_b,
                    s: Some(s_tx),
                    through_storage: sync,
                },
                egress_done,
            )
        }
    };
    let history = Arc::new(history);

    {
        let (cfg, stop, stats) = (cfg.clone(), stop.clone(), stats.clone());
        threads.push(spawn("krusty-ingress", cfg.stage_init, move || {
            ingress(cfg, ingress_sock, a_tx, stop, stats)
        }));
    }
    if let Some(start) = cluster_start {
        let cl = cfg.cluster.clone().unwrap();
        let me = cl.addr_of(cl.id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "no address for this node")
        })?;
        let repl_sock = bind(me, cfg.interface)?;
        let (r_tx, r_rx) = byte_ring(cfg.ring_bytes);
        let (o_tx, o_rx) = byte_ring(cfg.ring_bytes);
        {
            let (cfg, stop, stats) = (cfg.clone(), stop.clone(), stats.clone());
            threads.push(spawn("krusty-repl", cfg.stage_init, move || {
                crate::cluster::repl_stage(cfg, cl, repl_sock, r_tx, o_rx, stop, stats)
            }));
        }
        let (cfg, stop, done, h, stats, lf) = (
            cfg.clone(),
            stop.clone(),
            core_done.clone(),
            history.clone(),
            stats.clone(),
            leader_flag.clone(),
        );
        threads.push(spawn("krusty-core", cfg.stage_init, move || {
            crate::cluster::cluster_core_stage(
                cfg, a_rx, r_rx, o_tx, out, h, start, lf, stop, done, stats,
            )
        }));
    } else {
        let (cfg, stop, done, h, stats) = (
            cfg.clone(),
            stop.clone(),
            core_done.clone(),
            history.clone(),
            stats.clone(),
        );
        threads.push(spawn("krusty-core", cfg.stage_init, move || {
            core_stage(cfg, a_rx, out, h, core_start, stop, done, stats)
        }));
    }
    {
        let (cfg, stats, done) = (cfg.clone(), stats.clone(), egress_done.clone());
        threads.push(spawn("krusty-egress", cfg.stage_init, move || {
            egress(cfg, egress_sock, b_rx, done, stats)
        }));
    }
    {
        let feed_sock = sender(cfg.feed, cfg.interface)?;
        let (cfg, stop, h, stats, d) = (
            cfg.clone(),
            stop.clone(),
            history.clone(),
            stats.clone(),
            disk.clone(),
        );
        threads.push(spawn("krusty-recovery", cfg.stage_init, move || {
            recovery(cfg, recovery_sock, feed_sock, h, d, stop, stats)
        }));
    }
    let mut mold_rerequest_addr = None;
    if let Some(m) = cfg.mold {
        let rereq = bind(m.rerequest, cfg.interface)?;
        mold_rerequest_addr = Some(v4(rereq.local_addr()?));
        let out = sender(m.feed, cfg.interface)?;
        // Ends after whatever feeds egress has finished (the storage stage in mode b).
        let (cfg, done, h, stats, lf) = (
            cfg.clone(),
            egress_done.clone(),
            history.clone(),
            stats.clone(),
            leader_flag.clone(),
        );
        // Mode (b): Mold may publish only what is durable.
        let durable = match &cfg.storage {
            Some(st) if st.durability == crate::Durability::Sync => disk.clone(),
            _ => None,
        };
        threads.push(spawn("krusty-mold", cfg.stage_init, move || {
            mold(cfg, m, out, rereq, h, durable, lf, done, stats)
        }));
    }
    Ok(Node {
        stop,
        threads,
        ingress_addr,
        recovery_addr,
        mold_rerequest_addr,
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
    stats: Arc<Metrics>,
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
        let received = mono_ns();
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
            let wrote = out.try_write(REC_SUBMIT, REC_HDR + bytes.len(), |b| {
                put_addr(b, src);
                put_ts(b, received);
                b[REC_HDR..].copy_from_slice(bytes);
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
pub(crate) fn put_history(
    h: &History,
    seqno: u64,
    meta: &MessageMeta,
    payload: &[u8],
    scratch: &mut [u8],
) {
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

/// Where the core stage's output goes: egress-bound records straight to
/// ring B, or (durability mode b) through the storage stage, which forwards
/// them after the batch's fsync; log entries to the storage stage.
pub(crate) struct Out {
    b: Producer,
    s: Option<Producer>,
    through_storage: bool,
}

impl Out {
    pub(crate) fn egress(&mut self, kind: u32, len: usize, fill: impl FnMut(&mut [u8])) {
        if self.through_storage {
            write_ring(
                self.s.as_mut().unwrap(),
                crate::storage::S_EGRESS + kind,
                len,
                fill,
            );
        } else {
            write_ring(&mut self.b, kind, len, fill);
        }
    }

    pub(crate) fn store(&mut self, kind: u32, len: usize, fill: impl FnMut(&mut [u8])) {
        if let Some(s) = self.s.as_mut() {
            write_ring(s, kind, len, fill);
        }
    }

    pub(crate) fn store_published(&mut self, seqno: u64, log_index: u64) {
        self.store(crate::storage::S_PUBLISHED, 16, |b| {
            b[..8].copy_from_slice(&seqno.to_le_bytes());
            b[8..].copy_from_slice(&log_index.to_le_bytes());
        });
    }

    pub(crate) fn publish(&mut self) {
        self.b.publish();
        if let Some(s) = self.s.as_mut() {
            s.publish();
        }
    }
}

/// Queues a frame for the feed.
pub(crate) fn feed_record(out: &mut Out, frame: &[u8]) {
    let t = mono_ns();
    out.egress(REC_FEED, REC_HDR + frame.len(), |b| {
        b[..8].fill(0);
        put_ts(b, t);
        b[REC_HDR..].copy_from_slice(frame);
    });
}

/// Writes a record, waiting for the consumer if the ring is full.
pub(crate) fn write_ring(
    out: &mut Producer,
    kind: u32,
    len: usize,
    mut fill: impl FnMut(&mut [u8]),
) {
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
pub(crate) fn emit_frames(
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

/// Snapshot hand-off: buffers to the storage stage, buffers coming back,
/// and the cadence in log entries.
pub(crate) type SnapshotLink = (SyncSender<Vec<u8>>, Receiver<Vec<u8>>, u64);

/// What the core stage starts from (fresh, or restored from storage).
pub(crate) struct CoreStart {
    pub core: Core,
    pub log_index: u64,
    pub last_ts: u64,
    /// Snapshot hand-off to the storage stage: (send, buffers coming back).
    pub snapshots: Option<SnapshotLink>,
}

#[allow(clippy::too_many_arguments)]
fn core_stage(
    cfg: NodeConfig,
    mut input: Consumer,
    mut out: Out,
    history: Arc<History>,
    start: CoreStart,
    stop: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    stats: Arc<Metrics>,
) {
    pin(cfg.cores[1]);
    let CoreStart {
        mut core,
        mut log_index,
        mut last_ts,
        snapshots,
    } = start;
    let mut last_snapshot = log_index;
    let mut snap_buf: Option<Vec<u8>> = Some(Vec::new());
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
        // The leader's clock, made monotonic here so the time logged with an
        // entry is the time the core used (replay reproduces it exactly).
        let ts = now_ns().max(last_ts);
        last_ts = ts;
        let batch_start = mono_ns();
        let n = input.read(cfg.batch, |_, rec| {
            let src = get_addr(rec);
            stats
                .ingress_to_core
                .record(batch_start.saturating_sub(get_ts(rec)));
            let Ok(s) = Submit::decode(&rec[REC_HDR..]) else {
                return;
            };
            log_index += 1;
            let e = core
                .apply(log_index, ts, &s)
                .expect("entries are applied in order");
            let raw = &rec[REC_HDR..];
            out.store(
                crate::storage::S_ENTRY,
                crate::storage::S_ENTRY_HEADER + raw.len(),
                |b| crate::storage::put_entry(b, log_index, 0, ts, raw),
            );
            if let Some(p) = e.publish {
                put_history(&history, p.seqno, &p.meta, s.payload, &mut scratch);
                out.store_published(p.seqno, log_index);
            }
            if let Some(f) = e.reply {
                replies += 1;
                out.egress(REC_TO, REC_HDR + krusty_wire::feedback::LEN, |b| {
                    put_addr(b, src);
                    put_ts(b, batch_start);
                    f.encode(&mut fb).unwrap();
                    b[REC_HDR..].copy_from_slice(&fb);
                });
            }
        });
        let last = core.next_seqno();
        stats.stream_next.store(last, Ordering::Relaxed);
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
                |f| feed_record(&mut out, f),
            );
            stats.published.fetch_add(last - first, Ordering::Relaxed);
            stats.frames.fetch_add(frames, Ordering::Relaxed);
            last_sent = Instant::now();
        }
        if let Some((tx, back, every)) = &snapshots
            && log_index >= last_snapshot + every
        {
            // Hand a snapshot to the storage stage (skipped while it is
            // still writing the previous one).
            if snap_buf.is_none() {
                snap_buf = back.try_recv().ok();
            }
            if let Some(mut buf) = snap_buf.take() {
                buf.clear();
                core.snapshot(&mut buf);
                out.store(crate::storage::S_SNAPSHOT, 8, |b| {
                    b.copy_from_slice(&log_index.to_le_bytes())
                });
                let _ = tx.send(buf);
                last_snapshot = log_index;
            }
        }
        if n > 0 {
            stats.core_batch.record(mono_ns() - batch_start);
            stats.feedback.fetch_add(replies, Ordering::Relaxed);
            replies = 0;
            out.publish();
            empty = 0;
            continue;
        }
        if stop.load(Ordering::Acquire) {
            let k = stream::encode_end_of_session(&mut frame, cfg.stream_id, core.next_seqno())
                .unwrap();
            feed_record(&mut out, &frame[..k]);
            out.publish();
            done.store(true, Ordering::Release);
            return;
        }
        if last_sent.elapsed() >= heartbeat {
            let k = stream::encode_heartbeat(&mut frame, cfg.stream_id, core.next_seqno()).unwrap();
            feed_record(&mut out, &frame[..k]);
            out.publish();
            last_sent = Instant::now();
        }
        idle(cfg.idle, &mut empty);
    }
}

fn egress(
    cfg: NodeConfig,
    sock: UdpSocket,
    mut input: Consumer,
    core_done: Arc<AtomicBool>,
    stats: Arc<Metrics>,
) {
    pin(cfg.cores[2]);
    let mut sb = SendBatch::new(cfg.batch, cfg.mtu.max(krusty_wire::feedback::LEN));
    let mut empty = 0;
    loop {
        let finished = core_done.load(Ordering::Acquire);
        let now = mono_ns();
        let n = input.read(cfg.batch, |kind, rec| {
            if sb.is_full() {
                let _ = sb.flush(&sock);
            }
            stats.core_to_egress.record(now.saturating_sub(get_ts(rec)));
            let body = &rec[REC_HDR..];
            match kind {
                REC_FEED => {
                    sb.push(cfg.feed, |b| {
                        b[..body.len()].copy_from_slice(body);
                        body.len()
                    });
                }
                _ => {
                    let dest = get_addr(rec);
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

/// Message `seqno` from the history, or else from disk, into `entry`.
fn fetch(
    seqno: u64,
    history: &History,
    disk: Option<&krusty_log::LogReader>,
    entry: &mut [u8],
    raw: &mut Vec<u8>,
) -> Option<(MessageMeta, usize, usize)> {
    if let Ok(n) = history.read(seqno, entry) {
        let (meta, _) = parse_history(&entry[..n]);
        return Some((meta, HIST_HEADER, n));
    }
    let stored = disk?.read_published(seqno, raw).ok()??;
    let s = Submit::decode(raw).ok()?;
    let meta = MessageMeta {
        timestamp_ns: stored.timestamp_ns,
        client_id: s.client_id,
        // An accepted conditional append always reaches expected + 1.
        conditional: match s.append {
            krusty_wire::ingress::Append::Conditional {
                key,
                expected_version,
            } => Some((key, expected_version + 1)),
            krusty_wire::ingress::Append::Unconditional => None,
        },
    };
    let len = s.payload.len();
    entry[HIST_HEADER..HIST_HEADER + len].copy_from_slice(s.payload);
    Some((meta, HIST_HEADER, HIST_HEADER + len))
}

fn recovery(
    cfg: NodeConfig,
    sock: UdpSocket,
    feed_sock: UdpSocket,
    history: Arc<History>,
    disk: Option<krusty_log::LogReader>,
    stop: Arc<AtomicBool>,
    stats: Arc<Metrics>,
) {
    pin(cfg.cores[3]);
    let durable_only = matches!(&cfg.storage, Some(st) if st.durability == crate::Durability::Sync);
    let mut rb = RecvBatch::new(16, 256);
    let mut sb = SendBatch::new(64, cfg.mtu);
    let mut frame = vec![0u8; cfg.mtu];
    let mut entry = vec![0u8; HIST_HEADER + cfg.max_payload];
    let mut raw = Vec::with_capacity(2048);
    let mut empty = 0;
    // Recent requests: (from, requester, time); and ranges just put on the
    // feed: (from, until).
    let window = cfg.recover_suppress_us * 1000;
    let mut recent: [(u64, SocketAddrV4, u64); 32] =
        [(0, SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0), 0); 32];
    let mut recent_at = 0;
    let mut on_feed: [(u64, u64); 16] = [(0, 0); 16];
    let mut on_feed_at = 0;
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
            let t0 = mono_ns();
            // Suppression: just retransmitted on the feed? Then this
            // consumer will see it there.
            if window > 0 && on_feed.iter().any(|&(f, until)| f == from && t0 < until) {
                stats.recover_suppressed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            // A second consumer asking for the same range: answer on the feed.
            let shared = window > 0
                && recent
                    .iter()
                    .any(|&(f, who, at)| f == from && who != src && t0.saturating_sub(at) < window);
            recent[recent_at % recent.len()] = (from, src, t0);
            recent_at += 1;
            let dest = if shared {
                on_feed[on_feed_at % on_feed.len()] = (from, t0 + window);
                on_feed_at += 1;
                stats.recover_on_feed.fetch_add(1, Ordering::Relaxed);
                cfg.feed
            } else {
                src
            };
            let out_sock = if shared { &feed_sock } else { &sock };
            // What is retained: on disk if there is storage, else in memory.
            let (earliest, last) = match &disk {
                // Mode (b): only what is durable may leave the node.
                Some(d) if durable_only => (d.earliest_seqno(), d.durable_seqno()),
                Some(d) => (d.earliest_seqno(), history.committed().max(d.last_seqno())),
                None => (history.earliest(), history.committed()),
            };
            if from < earliest {
                let k =
                    stream::encode_unavailable(&mut frame, cfg.stream_id, from, earliest).unwrap();
                let f = &frame[..k];
                sb.push(dest, |b| {
                    b[..f.len()].copy_from_slice(f);
                    f.len()
                });
            }
            let mut s = from.max(earliest);
            let end = from
                .saturating_add(count.min(MAX_RECOVER_ANSWER))
                .min(last + 1);
            let history_from = history.earliest();
            while s < end {
                let mut w =
                    DataFrameWriter::new(&mut frame[..cfg.mtu], cfg.stream_id, s, true).unwrap();
                while s < end {
                    let Some((meta, a, b)) =
                        fetch(s, &history, disk.as_ref(), &mut entry, &mut raw)
                    else {
                        break;
                    };
                    if w.push(&meta, &entry[a..b]).is_err() {
                        break; // frame full
                    }
                    if s < history_from {
                        stats.recovered_from_disk.fetch_add(1, Ordering::Relaxed);
                    }
                    s += 1;
                }
                if w.count() == 0 {
                    break;
                }
                let k = w.finish().unwrap();
                if sb.is_full() {
                    let _ = sb.flush(out_sock);
                }
                let f = &frame[..k];
                sb.push(dest, |b| {
                    b[..f.len()].copy_from_slice(f);
                    f.len()
                });
            }
            // Answers go out per request (feed or requester).
            let _ = sb.flush(out_sock);
            stats.recovery_service.record(mono_ns() - t0);
        }
        let _ = sb.flush(&sock);
    }
}

/// Packs history messages `from..to` into MoldUDP64 packets for `dest`.
/// Returns how many seqnos were no longer in the history.
#[allow(clippy::too_many_arguments)]
fn mold_range(
    m: &crate::MoldConfig,
    history: &History,
    from: u64,
    to: u64,
    dest: SocketAddrV4,
    pkt: &mut [u8],
    entry: &mut [u8],
    sb: &mut SendBatch,
    sock: &UdpSocket,
) -> u64 {
    let mut lost = 0;
    let mut s = from;
    while s < to {
        let mut w = krusty_mold::PacketWriter::new(pkt, m.session, s).expect("mtu ≥ header");
        while s < to {
            match history.read(s, entry) {
                Ok(n) => {
                    if !w.push(&entry[HIST_HEADER..n]) {
                        break;
                    }
                    s += 1;
                }
                Err(_) => break,
            }
        }
        if w.count() == 0 {
            // Not in the history any more: lost for Mold consumers.
            lost += 1;
            s += 1;
            continue;
        }
        let n = w.finish();
        if sb.is_full() {
            let _ = sb.flush(sock);
        }
        let p = &pkt[..n];
        sb.push(dest, |b| {
            b[..p.len()].copy_from_slice(p);
            p.len()
        });
    }
    let _ = sb.flush(sock);
    lost
}

/// The MoldUDP64 adapter: follows the history and republishes it as
/// MoldUDP64 with the same seqnos, serves re-requests, and ends the session
/// after the core has.
#[allow(clippy::too_many_arguments)]
fn mold(
    cfg: NodeConfig,
    m: crate::MoldConfig,
    sock: UdpSocket,
    rereq: UdpSocket,
    history: Arc<History>,
    durable: Option<krusty_log::LogReader>,
    leader: Arc<AtomicBool>,
    core_done: Arc<AtomicBool>,
    stats: Arc<Metrics>,
) {
    let mut next = history.earliest();
    let mut pkt = vec![0u8; cfg.mtu];
    let mut entry = vec![0u8; HIST_HEADER + cfg.max_payload];
    let mut sb = SendBatch::new(64, cfg.mtu);
    let mut rb = RecvBatch::new(16, 64);
    let heartbeat = Duration::from_micros(cfg.heartbeat_us);
    let mut last_sent = Instant::now();
    let mut empty = 0;
    loop {
        let done = core_done.load(Ordering::Acquire);
        let committed = match &durable {
            Some(d) => history.committed().min(d.durable_seqno()),
            None => history.committed(),
        };
        let mut busy = false;
        // In a cluster only the leader publishes (re-requests: any node).
        let leading = leader.load(Ordering::Acquire);
        if next <= committed && !leading {
            next = committed + 1;
        } else if next <= committed {
            let lost = mold_range(
                &m,
                &history,
                next,
                committed + 1,
                m.feed,
                &mut pkt,
                &mut entry,
                &mut sb,
                &sock,
            );
            stats
                .mold_published
                .fetch_add(committed + 1 - next - lost, Ordering::Relaxed);
            stats.mold_lost.fetch_add(lost, Ordering::Relaxed);
            next = committed + 1;
            last_sent = Instant::now();
            busy = true;
        } else if done {
            if cfg.cluster.is_none() {
                let n = krusty_mold::encode_end_of_session(&mut pkt, m.session, next);
                let _ = sock.send_to(&pkt[..n], m.feed);
            }
            return;
        } else if leading && last_sent.elapsed() >= heartbeat {
            let n = krusty_mold::encode_heartbeat(&mut pkt, m.session, next);
            let _ = sock.send_to(&pkt[..n], m.feed);
            last_sent = Instant::now();
        }
        let k = rb.recv(&rereq).unwrap_or_default();
        for i in 0..k {
            let (b, src) = rb.get(i);
            let Some(r) = krusty_mold::Request::decode(b) else {
                continue;
            };
            if r.session != m.session || r.seqno == 0 || r.count == 0 {
                continue;
            }
            stats.mold_rerequests.fetch_add(1, Ordering::Relaxed);
            let end = r
                .seqno
                .saturating_add(r.count.min(1024) as u64)
                .min(history.committed() + 1);
            let start = r.seqno.max(history.earliest());
            mold_range(
                &m, &history, start, end, src, &mut pkt, &mut entry, &mut sb, &rereq,
            );
            busy = true;
        }
        if busy {
            empty = 0;
        } else {
            idle(Idle::Sleep { sleep_us: 100 }, &mut empty);
        }
    }
}
