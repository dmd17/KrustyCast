//! Live metrics in shared memory (gap O2): counters and per-stage latency
//! histograms that `krusty-stat` reads from another process while the node
//! runs. Updating one is a relaxed atomic add; nothing is formatted, locked
//! or allocated on the node's side.
//!
//! The file is a `#[repr(C)]` [`Shared`] (fixed layout, versioned by
//! [`MAGIC`]/[`LAYOUT_VERSION`]); readers map it read-only.

use std::fs::OpenOptions;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAGIC: u64 = 0x5354_4154_534B_434B; // "KCKSTATS"
pub const LAYOUT_VERSION: u64 = 12;
pub const BUCKETS: usize = 64;

/// log2 histogram: bucket `b` counts values in `[2^(b-1), 2^b)` ns (bucket 0: 0).
#[repr(C)]
pub struct Histogram {
    pub buckets: [AtomicU64; BUCKETS],
}

impl Histogram {
    pub fn record(&self, ns: u64) {
        let b = (64 - ns.leading_zeros()) as usize;
        self.buckets[b.min(BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> [u64; BUCKETS] {
        std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed))
    }
}

/// Approximate quantile (the upper bound of its bucket), in ns.
pub fn quantile(b: &[u64; BUCKETS], q: f64) -> Option<u64> {
    let total: u64 = b.iter().sum();
    if total == 0 {
        return None;
    }
    let want = (q * total as f64).ceil().max(1.0) as u64;
    let mut seen = 0;
    for (i, n) in b.iter().enumerate() {
        seen += n;
        if seen >= want {
            return Some(if i == 0 { 0 } else { 1u64 << i.min(63) });
        }
    }
    None
}

macro_rules! counters {
    ($($name:ident: $doc:literal),* $(,)?) => {
        /// The shared layout. Field order is part of the format.
        #[repr(C)]
        pub struct Shared {
            pub magic: AtomicU64,
            pub layout_version: AtomicU64,
            pub pid: AtomicU64,
            pub stream_id: AtomicU64,
            pub started_unix_ns: AtomicU64,
            $(#[doc = $doc] pub $name: AtomicU64,)*
            /// Ingress receive → core processing (ring A dwell).
            pub ingress_to_core: Histogram,
            /// One core batch: decide, record, queue.
            pub core_batch: Histogram,
            /// Core queued → egress sent (ring B dwell plus send).
            pub core_to_egress: Histogram,
            /// One RECOVER request, received to answered.
            pub recovery_service: Histogram,
            /// One storage fsync (group commit of a batch).
            pub store_sync: Histogram,
            pub cluster_loop: Histogram,
            /// Cluster: a peer datagram received → taken by the core (ring R).
            pub peer_to_core: Histogram,
            /// Cluster: the core handling one peer message (decode, Raft, carry out).
            pub peer_handling: Histogram,
            /// Leader: an Append sent → the reply that covers it (per follower).
            pub append_rtt: Histogram,
            /// Leader: a SUBMIT proposed → applied (its commit wait).
            pub propose_to_apply: Histogram,
        }

        impl Shared {
            /// (name, value) of every counter, in layout order.
            pub fn counters(&self) -> Vec<(&'static str, u64)> {
                vec![$((stringify!($name), self.$name.load(Ordering::Relaxed)),)*]
            }

            pub fn histograms(&self) -> [(&'static str, &Histogram); 10] {
                [
                    ("ingress_to_core", &self.ingress_to_core),
                    ("core_batch", &self.core_batch),
                    ("core_to_egress", &self.core_to_egress),
                    ("recovery_service", &self.recovery_service),
                    ("store_sync", &self.store_sync),
                    ("cluster_loop", &self.cluster_loop),
                    ("peer_to_core", &self.peer_to_core),
                    ("peer_handling", &self.peer_handling),
                    ("append_rtt", &self.append_rtt),
                    ("propose_to_apply", &self.propose_to_apply),
                ]
            }
        }
    };
}

counters! {
    submits: "Valid SUBMITs received.",
    rejected_at_edge: "Datagrams rejected by ingress (malformed or too large).",
    busy: "SUBMITs answered BUSY because ring A was full.",
    published: "Messages published on the native stream.",
    frames: "DATA frames sent (first transmission).",
    feedback: "Outcomes sent to clients.",
    recover_requests: "RECOVER requests served.",
    mold_published: "Messages published by the MoldUDP64 adapter.",
    mold_lost: "Messages the Mold adapter fell too far behind to publish.",
    mold_rerequests: "MoldUDP64 re-requests served.",
    stored: "Log entries made durable.",
    snapshots: "Core snapshots written.",
    recovered_from_disk: "Messages served to RECOVER from disk.",
    recover_on_feed: "RECOVERs answered once on the feed because several consumers asked.",
    recover_suppressed: "RECOVERs ignored: the range was just retransmitted on the feed.",
    stream_next: "Gauge: the next stream seqno this node's core assigns.",
    raft_role: "Gauge: 0 follower, 1 pre-candidate, 2 candidate, 3 leader.",
    raft_term: "Gauge: current Raft term.",
    raft_commit: "Gauge: Raft commit index.",
    applied: "Committed log entries applied to the core (every node).",
    not_leader: "SUBMITs answered NOT_LEADER.",
    leader_terms: "Terms in which this node became leader.",
    repl_in: "Raft messages received.",
    repl_out: "Raft messages sent.",
    repl_dropped: "Raft messages dropped (ring full or undecodable).",
    repl_unservable: "Appends not sent: entries no longer retained (needs InstallSnapshot).",
    repl_held: "Mode (b): AppendReplies held until the entries they acknowledge were fsynced.",
    node_id: "Gauge: this node's cluster id (0 for a single node).",
    raft_leader: "Gauge: the leader this node knows of (0: none).",
    snapshot_index: "Gauge: log index of the last snapshot handed to storage.",
    clients: "Gauge: client sessions in the core.",
    raft_pre_votes: "Election timeouts: pre-votes this node started.",
    raft_elections: "Elections this node started (pre-vote won, or a transfer).",
    raft_quorum_lost: "Times this node stepped down as leader: a majority went quiet (check-quorum).",
    resends_filtered: "Resends the leader dropped: already in the log, not yet committed (their outcome follows).",
    submits_deferred: "Passes in which the cluster core left submits waiting: its output rings were too full (storage or egress behind).",
    cluster_loop_max_ns: "Gauge: the longest pass of the cluster core loop (ns): how long Raft went without ticks.",
    ingress_datagrams: "Datagrams received on ingress (submits / this: SUBMITs per datagram).",
    feedback_datagrams: "Datagrams of outcomes sent to clients (feedback / this: outcomes per datagram).",
    raft_recoveries: "Times this node started recovering from a possible power loss (mode a): new boot, or still recovering when it stopped.",
    raft_recovering: "Gauge: 1 while this node recovers from a possible power loss (mode a): it does not vote until it holds a leader's log.",
}

/// Counters that are levels, not totals (no rate is computed for them).
pub const GAUGES: &[&str] = &[
    "stream_next",
    "raft_role",
    "raft_recovering",
    "raft_term",
    "raft_commit",
    "node_id",
    "raft_leader",
    "snapshot_index",
    "clients",
    "cluster_loop_max_ns",
];

/// The node's metrics: a shared mapping (or private memory when no path is
/// given). Dereferences to [`Shared`].
pub struct Metrics {
    ptr: *mut Shared,
    len: usize,
}

// SAFETY: `Shared` is only atomics; the mapping lives as long as `Metrics`.
unsafe impl Send for Metrics {}
unsafe impl Sync for Metrics {}

impl std::ops::Deref for Metrics {
    type Target = Shared;
    fn deref(&self) -> &Shared {
        // SAFETY: `ptr` points to a live, zero-initialized mapping of
        // `size_of::<Shared>()` bytes; all fields are atomics, valid when zero.
        unsafe { &*self.ptr }
    }
}

impl Drop for Metrics {
    fn drop(&mut self) {
        // SAFETY: unmapping what `map` mapped.
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

fn map(fd: Option<i32>, len: usize, writable: bool) -> io::Result<*mut Shared> {
    let prot = if writable {
        libc::PROT_READ | libc::PROT_WRITE
    } else {
        libc::PROT_READ
    };
    let (flags, fd) = match fd {
        Some(fd) => (libc::MAP_SHARED, fd),
        None => (libc::MAP_SHARED | libc::MAP_ANONYMOUS, -1),
    };
    // SAFETY: a fresh mapping; the kernel validates the arguments.
    let p = unsafe { libc::mmap(std::ptr::null_mut(), len, prot, flags, fd, 0) };
    if p == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(p as *mut Shared)
}

impl Metrics {
    /// Creates (truncating) the metrics file at `path`, or private memory.
    pub fn create(path: Option<&Path>, stream_id: u64) -> io::Result<Metrics> {
        use std::os::fd::AsRawFd;
        let len = std::mem::size_of::<Shared>();
        let ptr = match path {
            Some(p) => {
                let f = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(p)?;
                f.set_len(len as u64)?; // zero-filled
                map(Some(f.as_raw_fd()), len, true)?
            }
            None => map(None, len, true)?,
        };
        let m = Metrics { ptr, len };
        m.pid.store(std::process::id() as u64, Ordering::Relaxed);
        m.stream_id.store(stream_id, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64);
        m.started_unix_ns.store(now.unwrap_or(0), Ordering::Relaxed);
        m.layout_version.store(LAYOUT_VERSION, Ordering::Relaxed);
        m.magic.store(MAGIC, Ordering::Release);
        Ok(m)
    }

    /// Maps an existing metrics file read-only (for `krusty-stat`).
    pub fn open(path: &Path) -> io::Result<Metrics> {
        use std::os::fd::AsRawFd;
        let len = std::mem::size_of::<Shared>();
        let f = OpenOptions::new().read(true).open(path)?;
        if f.metadata()?.len() < len as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "metrics file too small",
            ));
        }
        let m = Metrics {
            ptr: map(Some(f.as_raw_fd()), len, false)?,
            len,
        };
        if m.magic.load(Ordering::Acquire) != MAGIC
            || m.layout_version.load(Ordering::Relaxed) != LAYOUT_VERSION
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a krusty-node metrics file of this version",
            ));
        }
        Ok(m)
    }
}

/// CLOCK_MONOTONIC in ns (vDSO; no system call).
pub fn mono_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: writes the provided timespec.
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_is_shared_with_a_reader() {
        let dir = std::env::temp_dir().join(format!("krusty-metrics-test-{}", std::process::id()));
        let w = Metrics::create(Some(&dir), 7).unwrap();
        w.published.fetch_add(5, Ordering::Relaxed);
        w.core_batch.record(1500);
        let r = Metrics::open(&dir).unwrap();
        assert_eq!(r.stream_id.load(Ordering::Relaxed), 7);
        assert!(r.counters().contains(&("published", 5)));
        assert_eq!(quantile(&r.core_batch.snapshot(), 0.5), Some(2048));
        drop((w, r));
        std::fs::remove_file(&dir).unwrap();
    }
}
