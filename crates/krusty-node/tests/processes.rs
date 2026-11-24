//! Real `krusty-node` processes under faults (spec/RAFT-VERIFICATION.md,
//! "real-process fault injection"). The randomized harnesses run Raft and
//! the cluster in one process; here three node processes talk through a
//! proxy this test controls, and the test pauses (SIGSTOP), kills (SIGKILL)
//! and restarts them, cuts and heals links (partitions, one-way cuts), and
//! drops, delays, reorders and duplicates peer datagrams.
//!
//!   cargo test --release -p krusty-node --test processes -- --ignored --nocapture
//!   KRUSTY_PROC_SEED (default: the time), KRUSTY_PROC_SECS (faults, default
//!   20), KRUSTY_PROC_RATE (messages/s per publisher, default 300),
//!   KRUSTY_PROC_DIR (keep configs, logs and traces here; default a
//!   temporary directory, removed on success), KRUSTY_PROC_POWER=0 (no
//!   power losses), KRUSTY_PROC_SCENARIO=crash-pairs (instead of random
//!   faults: pause one node, let the other two commit, kill both at once,
//!   restart them; mode (a) lost committed entries this way until
//!   2026-11-23, when entries still in ring S counted as persisted).
//!
//! A power loss (mode (a)) is a kill that also loses a tail of the log the
//! node had not fsynced, then the `recovering` marker a new boot sets
//! (`lose_power`): the node must recover before it votes (ARCHITECTURE
//! §8.2).
//!
//! Two publishers send all along; a consumer follows the feed. After the
//! faults end and everything heals, the run must finish (liveness: every
//! message answered, the feed complete) and then:
//! - the feed is gap-free from 1 and holds no message twice;
//! - every ACCEPTED message is in the feed at the position it named, with
//!   its own payload, and the feed holds nothing that was not submitted;
//! - every node serves the same stream (RECOVER from each node alone);
//! - each node's trace of its cluster shell (`cluster.trace`) is a behaviour
//!   of spec/tla/Cluster.tla: spec/tla/check-cluster-traces.sh <dir>.
//!
//! Peers find each other through `cluster.members`. Node i's list names its
//! own real address and, for each peer j, the proxy's socket for i → j,
//! which forwards to j's real address. Raft identifies senders by the node
//! id in the message, not the source address, so the proxy is invisible.
//! Clients reach the nodes directly.

use krusty_client::{
    Consumer, ConsumerConfig, ConsumerEvent, GapPolicy, Publisher, PublisherConfig, PublisherEvent,
};
use krusty_wire::ingress::Append;
use krusty_wire::stream::{self, Frame};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::net::{SocketAddrV4, UdpSocket};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[path = "../../krusty-client/tests/common/ports.rs"]
mod ports;
use ports::Ports;

const N: usize = 3;
const STREAM: u64 = 1;

/// SplitMix64.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn ms(&mut self, lo: u64, hi: u64) -> Duration {
        Duration::from_millis(lo + self.below(hi - lo + 1))
    }
}

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// The proxy: one socket per directed peer link.

#[derive(Default)]
struct Link {
    /// Cuts in force (nested: a partition and a one-way cut may overlap).
    cuts: AtomicU32,
}

#[derive(Default)]
struct Net {
    links: [[Link; N]; N],
    /// Applied to every link: loss and duplication (per mille), delay and
    /// jitter (µs; jitter reorders).
    loss: AtomicU32,
    dup: AtomicU32,
    delay_us: AtomicU64,
    jitter_us: AtomicU64,
    stop: AtomicBool,
    forwarded: AtomicU64,
    dropped: AtomicU64,
}

/// A datagram held back by delay: due time, sequence (stable order), link.
struct Held {
    due: Instant,
    seq: u64,
    from: usize,
    to: usize,
    bytes: Vec<u8>,
}
impl PartialEq for Held {
    fn eq(&self, o: &Self) -> bool {
        (self.due, self.seq) == (o.due, o.seq)
    }
}
impl Eq for Held {}
impl PartialOrd for Held {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Held {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        // A min-heap on (due, seq).
        (o.due, o.seq).cmp(&(self.due, self.seq))
    }
}

fn proxy(net: Arc<Net>, socks: Vec<(usize, usize, UdpSocket)>, real: Vec<SocketAddrV4>, seed: u64) {
    let mut rng = Rng(seed);
    let mut held = BinaryHeap::new();
    let mut seq = 0u64;
    let mut buf = vec![0u8; 65_536];
    let index: HashMap<(usize, usize), usize> = socks
        .iter()
        .enumerate()
        .map(|(k, (f, t, _))| ((*f, *t), k))
        .collect();
    while !net.stop.load(Ordering::Relaxed) {
        let mut busy = false;
        for (from, to, s) in &socks {
            while let Ok(n) = s.recv(&mut buf) {
                busy = true;
                let (from, to) = (*from, *to);
                if net.links[from][to].cuts.load(Ordering::Relaxed) > 0
                    || rng.below(1000) < net.loss.load(Ordering::Relaxed) as u64
                {
                    net.dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let copies =
                    1 + (rng.below(1000) < net.dup.load(Ordering::Relaxed) as u64) as usize;
                for _ in 0..copies {
                    let delay = net.delay_us.load(Ordering::Relaxed);
                    let jitter = net.jitter_us.load(Ordering::Relaxed);
                    let d = delay + if jitter > 0 { rng.below(jitter + 1) } else { 0 };
                    if d == 0 {
                        let _ = s.send_to(&buf[..n], real[to]);
                        net.forwarded.fetch_add(1, Ordering::Relaxed);
                    } else {
                        seq += 1;
                        held.push(Held {
                            due: Instant::now() + Duration::from_micros(d),
                            seq,
                            from,
                            to,
                            bytes: buf[..n].to_vec(),
                        });
                    }
                }
            }
        }
        let now = Instant::now();
        while held.peek().is_some_and(|h: &Held| h.due <= now) {
            let h = held.pop().unwrap();
            busy = true;
            // A link cut while the datagram was in flight loses it.
            if net.links[h.from][h.to].cuts.load(Ordering::Relaxed) > 0 {
                net.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let s = &socks[index[&(h.from, h.to)]].2;
            let _ = s.send_to(&h.bytes, real[h.to]);
            net.forwarded.fetch_add(1, Ordering::Relaxed);
        }
        if !busy {
            std::thread::sleep(Duration::from_micros(100));
        }
    }
}

// ---------------------------------------------------------------------------
// The nodes.

struct Proc {
    id: usize,
    config: PathBuf,
    /// Its storage directory.
    dir: PathBuf,
    log: PathBuf,
    child: Option<Child>,
    paused: bool,
    kills: u32,
    power_losses: u32,
}

impl Proc {
    fn start(&mut self) {
        assert!(self.child.is_none());
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_krusty-node"))
            .arg("--config")
            .arg(&self.config)
            .stdin(Stdio::null())
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .spawn()
            .expect("start krusty-node");
        self.child = Some(child);
        self.paused = false;
    }

    fn signal(&self, sig: i32) {
        if let Some(c) = &self.child {
            // SAFETY: kill(2) on our own child's pid.
            unsafe {
                libc::kill(c.id() as i32, sig);
            }
        }
    }

    fn pause(&mut self) {
        self.signal(libc::SIGSTOP);
        self.paused = true;
    }

    fn resume(&mut self) {
        self.signal(libc::SIGCONT);
        self.paused = false;
    }

    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill(); // SIGKILL
            let _ = c.wait();
            self.kills += 1;
        }
        self.paused = false;
    }

    /// The process exited by itself (a crash): fails the run.
    fn check_alive(&mut self) {
        if let Some(c) = &mut self.child
            && let Ok(Some(status)) = c.try_wait()
        {
            panic!(
                "node {} exited on its own ({status}); see {}",
                self.id,
                self.log.display()
            );
        }
    }

    fn stop(&mut self) {
        self.resume();
        if let Some(mut c) = self.child.take() {
            self.signal(libc::SIGTERM);
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if let Ok(Some(_)) = c.try_wait() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The marker a new boot sets (`must_recover`). KRUSTY_PROC_NO_MARKER=1
/// leaves it out: the node restarts as after a plain crash, as the code did
/// before the cautious restart (2026-11-22), to see whether a run catches
/// the safety violation that allows.
fn mark_recovering(dir: &std::path::Path) {
    if std::env::var("KRUSTY_PROC_NO_MARKER").as_deref() != Ok("1") {
        std::fs::File::create(dir.join("recovering")).unwrap();
    }
}

/// What a power loss leaves of a stopped node's storage in mode (a). The
/// hard state (`raft.state`) is always fsynced, and a snapshot is written
/// only after the log up to it was fsynced (storage.rs), so what can be
/// lost is a tail of the log above the latest snapshot: everything from a
/// record on is zeroed, or (torn) from inside it. Returns entries lost.
/// Then the `recovering` marker, as `must_recover` sets it on a new boot.
fn lose_power(dir: &std::path::Path, rng: &mut Rng) -> u64 {
    let mut snap = 0u64;
    let mut segs: Vec<(u64, PathBuf)> = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let num = |pre: &str, suf: &str| {
            name.strip_prefix(pre)
                .and_then(|r| r.strip_suffix(suf))
                .and_then(|n| n.parse::<u64>().ok())
        };
        if let Some(i) = num("snap-", ".snap") {
            snap = snap.max(i);
        }
        // Snapshot slots (krusty-log): magic u64 | log index u64 | ...
        if name == "snapshot.0" || name == "snapshot.1" {
            let b = std::fs::read(e.path()).unwrap_or_default();
            if b.len() >= 16 && b[0..8] == 0x5041_4E53_4F4C_434Bu64.to_le_bytes() {
                snap = snap.max(u64::from_le_bytes(b[8..16].try_into().unwrap()));
            }
        }
        if let Some(i) = num("log-", ".seg") {
            segs.push((i, e.path()));
        }
    }
    let Some((_, seg)) = segs.into_iter().max() else {
        return 0;
    };
    let mut bytes = std::fs::read(&seg).unwrap();
    // Records from the 64-byte file header on: size u32 (header + payload,
    // padded to 8 when stored) | crc u32 | log_index u64 | ...; size 0 ends.
    let mut at = 64usize;
    let mut cuttable = Vec::new();
    while at + 32 <= bytes.len() {
        let size = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        if size == 0 {
            break;
        }
        let index = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap());
        let len = size.div_ceil(8) * 8;
        if index > snap {
            cuttable.push((at, len));
        }
        at += len;
    }
    let end = at;
    if cuttable.is_empty() {
        mark_recovering(dir);
        return 0;
    }
    let pick = rng.below(cuttable.len() as u64) as usize;
    let (from, len) = cuttable[pick];
    let torn = rng.below(2) == 0;
    let cut = if torn {
        from + 4 + rng.below(len as u64 - 4) as usize
    } else {
        from
    };
    bytes[cut..end].iter_mut().for_each(|b| *b = 0);
    std::fs::write(&seg, &bytes).unwrap();
    mark_recovering(dir);
    (cuttable.len() - pick) as u64
}

// ---------------------------------------------------------------------------
// Faults.

#[derive(Debug)]
enum Undo {
    Heal(Vec<(usize, usize)>),
    Resume(usize),
    Restart(usize),
    Calm,
}

/// Starts a random fault; returns how to undo it, or None if the one drawn
/// does not apply now.
fn inject(
    rng: &mut Rng,
    net: &Net,
    procs: &mut [Proc],
    down: &mut [bool],
    power: bool,
) -> Option<Undo> {
    let k = rng.below(N as u64) as usize;
    match rng.below(if power { 7 } else { 6 }) {
        0 => {
            // Isolate node k from its peers, both ways.
            let mut links = Vec::new();
            for j in (0..N).filter(|&j| j != k) {
                links.push((k, j));
                links.push((j, k));
            }
            for &(a, b) in &links {
                net.links[a][b].cuts.fetch_add(1, Ordering::Relaxed);
            }
            Some(Undo::Heal(links))
        }
        1 => {
            // One direction of one link.
            let j = (k + 1 + rng.below(N as u64 - 1) as usize) % N;
            net.links[k][j].cuts.fetch_add(1, Ordering::Relaxed);
            Some(Undo::Heal(vec![(k, j)]))
        }
        2 => {
            net.loss
                .store(50 + rng.below(300) as u32, Ordering::Relaxed);
            net.dup.store(rng.below(100) as u32, Ordering::Relaxed);
            Some(Undo::Calm)
        }
        3 => {
            net.delay_us.store(rng.below(10_000), Ordering::Relaxed);
            net.jitter_us.store(rng.below(20_000), Ordering::Relaxed);
            net.dup.store(rng.below(50) as u32, Ordering::Relaxed);
            Some(Undo::Calm)
        }
        4 if !down[k] => {
            procs[k].pause();
            down[k] = true;
            Some(Undo::Resume(k))
        }
        5 if !down[k] => {
            procs[k].kill();
            down[k] = true;
            Some(Undo::Restart(k))
        }
        // A power loss, while no node is down or still recovering from one
        // (mode (a) tolerates one node at a time: f = 1 of 3).
        6 if !down.iter().any(|&d| d)
            && !procs.iter().any(|p| p.dir.join("recovering").exists()) =>
        {
            procs[k].kill();
            procs[k].kills -= 1;
            procs[k].power_losses += 1;
            let lost = lose_power(&procs[k].dir, rng);
            println!("power loss: node {} lost {lost} log entries", k + 1);
            down[k] = true;
            Some(Undo::Restart(k))
        }
        _ => None,
    }
}

fn undo(u: Undo, net: &Net, procs: &mut [Proc], down: &mut [bool]) {
    match u {
        Undo::Heal(links) => {
            for (a, b) in links {
                net.links[a][b].cuts.fetch_sub(1, Ordering::Relaxed);
            }
        }
        Undo::Resume(k) => {
            procs[k].resume();
            down[k] = false;
        }
        Undo::Restart(k) => {
            procs[k].start();
            down[k] = false;
        }
        Undo::Calm => {
            net.loss.store(0, Ordering::Relaxed);
            net.dup.store(0, Ordering::Relaxed);
            net.delay_us.store(0, Ordering::Relaxed);
            net.jitter_us.store(0, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------
// The stream as one node serves it.

/// Reads messages 1..=last from `node`'s recovery address alone, for at
/// most `wait`: what it served, and why it stopped short, if it did.
fn stream_of(node: SocketAddrV4, last: u64, wait: Duration) -> (Vec<Vec<u8>>, Option<String>) {
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    let mut req = [0u8; 64];
    let mut buf = vec![0u8; 65_536];
    let deadline = Instant::now() + wait;
    while (got.len() as u64) < last {
        if Instant::now() > deadline {
            let why = format!("stuck at {} of {last}", got.len());
            return (got, Some(why));
        }
        let from = got.len() as u64 + 1;
        let count = (last + 1 - from).min(1024);
        let n = stream::encode_recover(&mut req, STREAM, from, count).unwrap();
        sock.send_to(&req[..n], node).unwrap();
        while let Ok(n) = sock.recv(&mut buf) {
            match Frame::decode(&buf[..n]) {
                Ok(Frame::Data(d)) => {
                    for m in d.messages() {
                        if m.seqno == got.len() as u64 + 1 && m.seqno <= last {
                            got.push(m.payload.to_vec());
                        }
                    }
                }
                Ok(Frame::Unavailable { .. }) => {
                    return (got, Some(format!("UNAVAILABLE from {from}")));
                }
                _ => {}
            }
        }
    }
    (got, None)
}

/// Where each node's stream departs from the feed the consumer saw (a
/// safety violation), as far as each node serves it.
fn divergence(recovery: &[SocketAddrV4], feed: &[Vec<u8>]) -> Vec<String> {
    let mut out = Vec::new();
    for (k, r) in recovery.iter().enumerate() {
        let (s, why) = stream_of(*r, feed.len() as u64, Duration::from_secs(3));
        if let Some(i) = s.iter().zip(feed).position(|(a, b)| a != b) {
            out.push(format!(
                "node {} differs from the feed at seqno {}",
                k + 1,
                i + 1
            ));
        } else if (s.len()) < feed.len() {
            // What the feed published, no node may forget: ACCEPTED data lost.
            out.push(format!(
                "node {} agrees with the feed but serves only {} of {} ({})",
                k + 1,
                s.len(),
                feed.len(),
                why.unwrap_or_default()
            ));
        }
    }
    out
}

// ---------------------------------------------------------------------------

/// A message's payload names it: client, incarnation, client seqno.
fn payload(client: u32, inc: u64, seq: u64) -> [u8; 20] {
    let mut p = [0u8; 20];
    p[..4].copy_from_slice(&client.to_le_bytes());
    p[4..12].copy_from_slice(&inc.to_le_bytes());
    p[12..].copy_from_slice(&seq.to_le_bytes());
    p
}

struct Client {
    id: u32,
    p: Publisher,
    inc: u64,
    accepted: Vec<((u64, u64), u64)>,
    failed: u64,
    restarts: u64,
    next_seq: u64,
}

#[test]
#[ignore]
fn processes_under_faults() {
    let seed: u64 = env(
        "KRUSTY_PROC_SEED",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    let secs: u64 = env("KRUSTY_PROC_SECS", 20);
    let rate: u64 = env("KRUSTY_PROC_RATE", 300);
    let power: bool = env("KRUSTY_PROC_POWER", 1u8) == 1;
    // KRUSTY_PROC_SCENARIO=crash-pairs: only the scripted fault (below).
    let crash_pairs = std::env::var("KRUSTY_PROC_SCENARIO").as_deref() == Ok("crash-pairs");
    let (mut pair_step, mut pair_x) = (0u8, 0usize);
    let (dir, keep) = match std::env::var_os("KRUSTY_PROC_DIR") {
        Some(d) => (PathBuf::from(d), true),
        None => (
            std::env::temp_dir().join(format!("krusty-proc-{}-{seed}", std::process::id())),
            false,
        ),
    };
    println!(
        "seed {seed}, {secs} s of faults, {rate} msg/s per publisher, dir {}",
        dir.display()
    );
    let _ = std::fs::create_dir_all(&dir);
    for id in 1..=N {
        for f in [format!("node{id}.trace"), format!("node{id}.log")] {
            let _ = std::fs::remove_file(dir.join(f));
        }
        let _ = std::fs::remove_dir_all(dir.join(format!("node{id}")));
    }

    // Ports: per node ingress, recovery, peer; the feed; a proxy socket per
    // directed link.
    let mut ports = Ports::new(3 * N + 1 + N * (N - 1));
    let ingress: Vec<SocketAddrV4> = (0..N).map(|_| ports.take()).collect();
    let recovery: Vec<SocketAddrV4> = (0..N).map(|_| ports.take()).collect();
    let peer: Vec<SocketAddrV4> = (0..N).map(|_| ports.take()).collect();
    let feed = ports.take();
    let mut link_addr = [[None; N]; N];
    for (i, row) in link_addr.iter_mut().enumerate() {
        for (j, a) in row.iter_mut().enumerate() {
            if i != j {
                *a = Some(ports.take());
            }
        }
    }
    drop(ports);
    let mut socks = Vec::new();
    for (i, row) in link_addr.iter().enumerate() {
        for (j, a) in row.iter().enumerate() {
            if let Some(a) = a {
                let s = UdpSocket::bind(a).unwrap();
                s.set_nonblocking(true).unwrap();
                let _ = socket2::SockRef::from(&s).set_recv_buffer_size(8 << 20);
                socks.push((i, j, s));
            }
        }
    }

    let mut procs: Vec<Proc> = (0..N)
        .map(|k| {
            let id = k + 1;
            let members: Vec<String> = (0..N)
                .map(|j| {
                    let a = if j == k {
                        peer[k]
                    } else {
                        link_addr[k][j].unwrap()
                    };
                    format!("\"{}@{a}\"", j + 1)
                })
                .collect();
            let config = dir.join(format!("node{id}.toml"));
            std::fs::write(
                &config,
                format!(
                    "ingress = \"{}\"\nfeed = \"{feed}\"\ninterface = \"127.0.0.1\"\n\
                     recovery = \"{}\"\nstream_id = {STREAM}\nmetrics = \"{}\"\n\n\
                     [storage]\ndir = \"{}\"\ndurability = \"a\"\nsnapshot_every = 700\n\n\
                     [cluster]\nid = {id}\nmembers = [{}]\ntrace = \"{}\"\n",
                    ingress[k],
                    recovery[k],
                    dir.join(format!("node{id}.stats")).display(),
                    dir.join(format!("node{id}")).display(),
                    members.join(", "),
                    dir.join(format!("node{id}.trace")).display(),
                ),
            )
            .unwrap();
            Proc {
                id,
                config,
                dir: dir.join(format!("node{id}")),
                log: dir.join(format!("node{id}.log")),
                child: None,
                paused: false,
                kills: 0,
                power_losses: 0,
            }
        })
        .collect();

    let net = Arc::new(Net::default());
    let proxy_thread = {
        let net = net.clone();
        let real = peer.clone();
        std::thread::spawn(move || proxy(net, socks, real, seed ^ 0xA5A5))
    };
    for p in &mut procs {
        p.start();
    }

    let mut clients: Vec<Client> = (1..=2)
        .map(|id| {
            let mut cfg = PublisherConfig::new(id, ingress.clone());
            cfg.max_attempts = u32::MAX;
            let p = Publisher::new(cfg).unwrap();
            Client {
                id,
                inc: p.incarnation(),
                p,
                accepted: Vec::new(),
                failed: 0,
                restarts: 0,
                next_seq: 1,
            }
        })
        .collect();
    let mut ccfg = ConsumerConfig::new(feed, recovery.clone());
    ccfg.stream_id = Some(STREAM);
    ccfg.start_seqno = Some(1);
    ccfg.gap_policy = GapPolicy::Wait;
    ccfg.max_payload = 64;
    let mut consumer = Consumer::new(ccfg).unwrap();
    let mut feed_seen: Vec<Vec<u8>> = Vec::new();
    let mut lost = 0u64;

    let mut rng = Rng(seed);
    let mut down = [false; N];
    let mut active: Vec<(Instant, Undo)> = Vec::new();
    let mut injected = 0u32;
    let start = Instant::now();
    let faults_end = start + Duration::from_secs(secs);
    let mut next_fault = start + Duration::from_millis(500);
    let mut sent_by = [0u64; 2];
    let mut healed_at: Option<Instant> = None;

    loop {
        let now = Instant::now();
        for p in &mut procs {
            p.check_alive();
        }
        // Faults.
        if now < faults_end && crash_pairs {
            // Scripted: pause X; once the other two commit without it, kill
            // both at once; restart them and resume X.
            if now >= next_fault {
                match pair_step {
                    0 => {
                        pair_x = rng.below(N as u64) as usize;
                        procs[pair_x].pause();
                        next_fault = now + Duration::from_millis(700);
                    }
                    1 => {
                        for k in (0..N).filter(|&k| k != pair_x) {
                            procs[k].kill();
                        }
                        injected += 1;
                        next_fault = now + Duration::from_millis(100);
                    }
                    _ => {
                        for k in (0..N).filter(|&k| k != pair_x) {
                            procs[k].start();
                        }
                        procs[pair_x].resume();
                        next_fault = now + Duration::from_millis(1500);
                    }
                }
                pair_step = (pair_step + 1) % 3;
            }
        } else if crash_pairs && healed_at.is_none() {
            if pair_step == 2 {
                for k in (0..N).filter(|&k| k != pair_x) {
                    procs[k].start();
                }
            }
            if pair_step != 0 {
                procs[pair_x].resume();
            }
            healed_at = Some(now);
        } else if now < faults_end {
            if now >= next_fault && active.len() < 2 {
                if let Some(u) = inject(&mut rng, &net, &mut procs, &mut down, power) {
                    injected += 1;
                    let d = rng.ms(100, 3000);
                    active.push((now + d, u));
                }
                next_fault = now + rng.ms(50, 800);
            }
            let mut k = 0;
            while k < active.len() {
                if active[k].0 <= now {
                    let (_, u) = active.swap_remove(k);
                    undo(u, &net, &mut procs, &mut down);
                } else {
                    k += 1;
                }
            }
        } else if healed_at.is_none() {
            let ending: Vec<String> = active.iter().map(|(_, u)| format!("{u:?}")).collect();
            for (_, u) in active.drain(..) {
                undo(u, &net, &mut procs, &mut down);
            }
            healed_at = Some(now);
            println!(
                "faults over: {injected} injected; proxy forwarded {}, dropped {}; \
                 healed {ending:?} with {:?} in flight",
                net.forwarded.load(Ordering::Relaxed),
                net.dropped.load(Ordering::Relaxed),
                clients.iter().map(|c| c.p.in_flight()).collect::<Vec<_>>()
            );
        }
        // Load, while the faults last.
        let elapsed = now.duration_since(start).as_secs_f64();
        for (c, sent) in clients.iter_mut().zip(sent_by.iter_mut()) {
            while now < faults_end && (*sent as f64) < elapsed * rate as f64 {
                // Seqnos run 1, 2, ... within an incarnation.
                let m = payload(c.id, c.inc, c.next_seq);
                match c.p.submit(Append::Unconditional, &m) {
                    Ok(seq) => assert_eq!(seq, c.next_seq),
                    Err(_) => break, // window full: later
                }
                c.next_seq += 1;
                *sent += 1;
            }
        }
        for c in &mut clients {
            let (id, inc) = (c.id, &mut c.inc);
            let (acc, failed, restarts, next_seq) = (
                &mut c.accepted,
                &mut c.failed,
                &mut c.restarts,
                &mut c.next_seq,
            );
            c.p.poll(|e| match e {
                PublisherEvent::Accepted {
                    client_seqno,
                    global_seqno,
                } => acc.push(((*inc, client_seqno), global_seqno)),
                PublisherEvent::Failed { .. } => *failed += 1,
                PublisherEvent::Restarted { incarnation } => {
                    *inc = incarnation;
                    *next_seq = 1;
                    *restarts += 1;
                }
                PublisherEvent::Rejected { .. } => panic!("client {id}: unexpected REJECTED"),
            })
            .unwrap();
        }
        consumer
            .poll(|e| match e {
                ConsumerEvent::Message(d) => {
                    assert_eq!(d.seqno, feed_seen.len() as u64 + 1, "feed out of order");
                    feed_seen.push(d.payload.to_vec());
                }
                ConsumerEvent::Lost { count } => lost += count,
                ConsumerEvent::EndOfStream => panic!("unexpected end of stream"),
            })
            .unwrap();
        // Done: every message answered and the feed has everything accepted.
        if let Some(h) = healed_at {
            let answered = clients.iter().all(|c| c.p.in_flight() == 0);
            let top = clients
                .iter()
                .flat_map(|c| c.accepted.iter().map(|a| a.1))
                .max()
                .unwrap_or(0);
            if answered && feed_seen.len() as u64 >= top {
                println!(
                    "settled {:.1} ms after healing",
                    h.elapsed().as_secs_f64() * 1e3
                );
                break;
            }
            if h.elapsed() > Duration::from_secs(30) {
                let inflight: Vec<usize> = clients.iter().map(|c| c.p.in_flight()).collect();
                let diverged = divergence(&recovery, &feed_seen);
                panic!(
                    "liveness: 30 s after healing, in flight {inflight:?}, feed at {} of {top}; \
                     streams: {diverged:?}; logs in {}",
                    feed_seen.len(),
                    dir.display()
                );
            }
        }
        std::thread::sleep(Duration::from_micros(200));
    }
    // Late messages still on their way to the feed (resends of failed ones
    // are not expected: nothing fails with unlimited attempts).
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        consumer
            .poll(|e| {
                if let ConsumerEvent::Message(d) = e {
                    assert_eq!(d.seqno, feed_seen.len() as u64 + 1);
                    feed_seen.push(d.payload.to_vec());
                }
            })
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }

    // Safety.
    assert_eq!(lost, 0, "the consumer reported loss");
    let mut seen = HashSet::new();
    for (k, m) in feed_seen.iter().enumerate() {
        assert!(
            seen.insert(m.clone()),
            "message twice in the feed (at {})",
            k + 1
        );
    }
    let mut accepted = 0;
    for c in &clients {
        assert_eq!(c.failed, 0, "client {}: messages failed", c.id);
        for &((inc, seq), g) in &c.accepted {
            accepted += 1;
            let at = feed_seen
                .get(g as usize - 1)
                .unwrap_or_else(|| panic!("ACCEPTED at {g} beyond the feed"));
            let want = payload(c.id, inc, seq);
            assert_eq!(
                at[..],
                want[..],
                "ACCEPTED {} {inc}/{seq} at {g}: other message there",
                c.id
            );
        }
    }
    let last = feed_seen.len() as u64;
    assert_eq!(
        accepted, last,
        "feed holds messages no one was told were accepted"
    );
    for (k, r) in recovery.iter().enumerate() {
        let (s, why) = stream_of(*r, last, Duration::from_secs(30));
        if let Some(why) = why {
            panic!("node {}: {why}", k + 1);
        }
        assert!(s == feed_seen, "node {} serves a different stream", k + 1);
    }
    net.stop.store(true, Ordering::Relaxed);
    proxy_thread.join().unwrap();
    let kills: Vec<u32> = procs.iter().map(|p| p.kills).collect();
    let powers: Vec<u32> = procs.iter().map(|p| p.power_losses).collect();
    for p in &mut procs {
        p.stop();
    }
    println!(
        "ok: {last} messages, {injected} faults, kills per node {kills:?}, power losses \
         {powers:?}, restarts {:?}; traces: spec/tla/check-cluster-traces.sh {}",
        clients.iter().map(|c| c.restarts).collect::<Vec<_>>(),
        dir.display()
    );
    if !keep {
        let _ = std::fs::remove_dir_all(&dir);
    }
}
