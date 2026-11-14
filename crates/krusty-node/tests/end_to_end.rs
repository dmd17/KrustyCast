//! A real node over loopback UDP, driven by a krusty-session client and
//! read by a krusty-stream consumer. Checks exactly-once, in-order delivery
//! with the right bytes, and that the node's stage threads allocate nothing
//! once warmed up (NFR-7, gap P3).
use krusty_node::{Idle, NodeConfig};
use krusty_session::{Config as SessionConfig, Event, Session};
use krusty_stream::{Config as TrackerConfig, Kind, Tracker};
use krusty_wire::feedback::Feedback;
use krusty_wire::ingress::{Append, Submit};
use krusty_wire::stream::{self, Frame};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct Counting;

static STAGE_ALLOCS: AtomicU64 = AtomicU64::new(0);
thread_local! {
    static IS_STAGE: Cell<bool> = const { Cell::new(false) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if IS_STAGE.with(|s| s.get()) {
            STAGE_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if IS_STAGE.with(|s| s.get()) {
            STAGE_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static A: Counting = Counting;

fn mark_stage() {
    IS_STAGE.with(|s| s.set(true));
}

fn v4(a: SocketAddr) -> SocketAddrV4 {
    match a {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    }
}

struct Harness {
    client: UdpSocket,
    consumer: UdpSocket,
    session: Session,
    tracker: Tracker,
    ingress: SocketAddrV4,
    recovery: SocketAddrV4,
    events: Vec<Event>,
    delivered: Vec<(u64, Vec<u8>)>,
    stash: HashMap<u64, Vec<u8>>,
    start: Instant,
}

impl Harness {
    /// Submits `count` messages (handles `first..first+count`) and runs until
    /// all are answered and delivered.
    fn run(&mut self, first: u64, count: u64) {
        let mut next = first;
        let mut buf = [0u8; 2048];
        let deadline = Instant::now() + Duration::from_secs(30);
        let want_events = self.events.len() + count as usize;
        while self.events.len() < want_events || self.delivered.len() < self.events.len() {
            assert!(
                Instant::now() < deadline,
                "stuck: {} events, {} delivered",
                self.events.len(),
                self.delivered.len()
            );
            while next < first + count {
                match self.session.submit(Append::Unconditional, next) {
                    Ok(_) => next += 1,
                    Err(_) => break,
                }
            }
            let now = self.start.elapsed().as_nanos() as u64;
            let (mut tx, mut ev) = (Vec::new(), Vec::new());
            self.session.poll(now, &mut tx, &mut ev);
            for t in tx {
                let payload = t.handle.to_le_bytes();
                let s = Submit {
                    client_id: 1,
                    incarnation: t.incarnation,
                    client_seqno: t.client_seqno,
                    append: t.append,
                    payload: &payload,
                };
                let n = s.encode(&mut buf).unwrap();
                self.client.send_to(&buf[..n], self.ingress).unwrap();
            }
            while let Ok(n) = self.client.recv(&mut buf) {
                if let Ok(f) = Feedback::decode(&buf[..n]) {
                    self.session.on_feedback(&f, now, &mut ev);
                }
            }
            self.events.extend(ev);
            let mut out = Vec::new();
            while let Ok(n) = self.consumer.recv(&mut buf) {
                let f = match Frame::decode(&buf[..n]).unwrap() {
                    Frame::Data(d) => {
                        for m in d.messages() {
                            self.stash
                                .entry(m.seqno)
                                .or_insert_with(|| m.payload.to_vec());
                        }
                        krusty_stream::Frame {
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
                    } => krusty_stream::Frame {
                        stream_id,
                        seqno: next_seqno,
                        kind: Kind::Heartbeat,
                    },
                    Frame::EndOfSession {
                        stream_id,
                        next_seqno,
                    } => krusty_stream::Frame {
                        stream_id,
                        seqno: next_seqno,
                        kind: Kind::EndOfStream,
                    },
                    Frame::Unavailable {
                        stream_id,
                        from,
                        earliest_retained,
                    } => krusty_stream::Frame {
                        stream_id,
                        seqno: from,
                        kind: Kind::Unavailable { earliest_retained },
                    },
                    Frame::Recover { .. } => continue,
                };
                self.tracker.on_frame(&f, &mut out);
            }
            if let Some(r) = self.tracker.poll_recover(now, &mut out) {
                let n = stream::encode_recover(&mut buf, r.stream_id, r.from, r.count).unwrap();
                self.consumer.send_to(&buf[..n], self.recovery).unwrap();
            }
            for d in out {
                let p = self.stash.remove(&d.handle).expect("delivered with data");
                self.delivered.push((d.seqno, p));
            }
        }
    }
}

#[test]
fn exactly_once_in_order_and_no_allocation_on_the_hot_path() {
    run(None);
}

/// The same with durable storage in mode (b): the storage stage is on the
/// path, and it must not allocate either.
#[test]
fn with_storage_in_mode_b_still_no_allocation() {
    let dir = std::env::temp_dir().join(format!("krusty-e2e-storage-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    run(Some(krusty_node::StorageConfig {
        log: krusty_log::LogConfig::new(&dir),
        durability: krusty_node::Durability::Sync,
        // Snapshots are rare, write files and may allocate: keep them out of
        // the measured window.
        snapshot_every: 1_000_000,
    }));
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The allocation counter is process-wide: run one node at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn run(storage: Option<krusty_node::StorageConfig>) {
    let _one_at_a_time = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let consumer = UdpSocket::bind("127.0.0.1:0").unwrap();
    consumer.set_nonblocking(true).unwrap();
    let mut cfg = NodeConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        v4(consumer.local_addr().unwrap()),
        "127.0.0.1:0".parse().unwrap(),
    );
    cfg.idle = Idle::Sleep { sleep_us: 20 };
    cfg.heartbeat_us = 10_000;
    cfg.stage_init = Some(mark_stage);
    cfg.storage = storage;
    let node = krusty_node::start(cfg).unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").unwrap();
    client.set_nonblocking(true).unwrap();
    let mut h = Harness {
        client,
        consumer,
        session: Session::new(
            SessionConfig {
                client_id: 1,
                window: 64,
                retry_interval_ns: 20_000_000,
                max_attempts: 100,
            },
            1,
        ),
        tracker: Tracker::new(TrackerConfig {
            pinned: Some(1),
            start_seqno: Some(1),
            recover: true,
            retry_interval_ns: 20_000_000,
            max_retries: 100,
        }),
        ingress: node.ingress_addr(),
        recovery: node.recovery_addr(),
        events: Vec::new(),
        delivered: Vec::new(),
        stash: HashMap::new(),
        start: Instant::now(),
    };

    // Warm up (first-use allocations in std are allowed), then measure.
    h.run(0, 2_000);
    let before = STAGE_ALLOCS.load(Ordering::Relaxed);
    h.run(2_000, 20_000);
    let during = STAGE_ALLOCS.load(Ordering::Relaxed) - before;

    let stats = &node.stats;
    eprintln!(
        "submits {} published {} frames {} feedback {} busy {} recover {}",
        stats.submits.load(Ordering::Relaxed),
        stats.published.load(Ordering::Relaxed),
        stats.frames.load(Ordering::Relaxed),
        stats.feedback.load(Ordering::Relaxed),
        stats.busy.load(Ordering::Relaxed),
        stats.recover_requests.load(Ordering::Relaxed)
    );
    node.stop();

    assert_eq!(
        during, 0,
        "node stages allocated {during} times on the hot path"
    );
    // Exactly once: every message accepted once, at distinct positions 1..=N.
    assert_eq!(h.events.len(), 22_000);
    let mut at = vec![None; 22_001];
    for e in &h.events {
        match *e {
            Event::Accepted {
                handle,
                global_seqno,
                ..
            } => {
                assert!(at[global_seqno as usize].replace(handle).is_none());
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    // In order, every message, with the bytes its ACCEPTED named.
    assert_eq!(h.delivered.len(), 22_000);
    for (i, (seqno, p)) in h.delivered.iter().enumerate() {
        assert_eq!(*seqno, i as u64 + 1);
        assert_eq!(
            Some(u64::from_le_bytes(p[..8].try_into().unwrap())),
            at[*seqno as usize]
        );
    }
}
