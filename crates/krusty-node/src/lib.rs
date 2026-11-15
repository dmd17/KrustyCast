//! The KrustyCast node (single node, M1): pinned stages connected by
//! lock-free rings around the pure sequencer core (ARCHITECTURE §6).
//!
//! ```text
//!  clients ─▶ [ingress] ─ring A─▶ [core] ─ring B─▶ [egress] ─▶ feed, clients
//!                                   │ history
//!                                   ▼
//!  consumers ─RECOVER─▶ [recovery] ─▶ DATA (retransmit) / UNAVAILABLE
//! ```
//!
//! - ingress: `recvmmsg`, validates SUBMITs at the edge, copies them with
//!   their source address into ring A; answers BUSY when the ring is full.
//! - core: applies each submission to `krusty-core` with the batch's wall
//!   clock time as the leader timestamp, packs accepted messages into DATA
//!   frames, records them in the history, and queues frames and feedback.
//! - egress: `sendmmsg` of frames to the feed and feedback to clients.
//! - recovery: answers RECOVER from the history.
//!
//! Nothing on these paths allocates after startup. With storage (M2), a
//! storage stage writes the log (`storage`). With a cluster (M3), the core
//! stage also runs Raft (`cluster`): a replication stage moves Raft
//! messages, every node applies committed entries, and only the leader
//! publishes the feed and answers clients.

mod cluster;
pub mod config;
pub mod metrics;
pub mod stages;
pub mod storage;

pub use storage::Durability;

use krusty_core::Config as CoreConfig;
use std::net::{Ipv4Addr, SocketAddrV4};

/// How a stage waits when it has nothing to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Idle {
    /// Busy-spin (lowest latency; needs a dedicated core).
    Spin,
    /// Spin briefly, then sleep (`sleep_us`).
    Sleep { sleep_us: u64 },
}

/// The optional MoldUDP64 adapter (ADR-11): the same committed sequence
/// and global seqnos, published as standard MoldUDP64, off the latency path.
#[derive(Clone, Copy, Debug)]
pub struct MoldConfig {
    pub session: krusty_mold::Session,
    /// Where MoldUDP64 packets go (usually a multicast group).
    pub feed: SocketAddrV4,
    /// The standard re-request server's address.
    pub rerequest: SocketAddrV4,
}

/// Replication (M3): this node's Raft identity and its peers.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// This node's id (≥ 1).
    pub id: u32,
    /// Every member's replication address, by id (this node's too: it binds
    /// there).
    pub members: Vec<(u32, SocketAddrV4)>,
    pub election_min_us: u64,
    pub election_max_us: u64,
    /// Raft heartbeat (an empty Append) interval.
    pub heartbeat_us: u64,
    /// Most entry bytes per Append datagram.
    pub max_append_bytes: usize,
    pub max_inflight: u32,
    /// Committed entries kept in memory for lagging followers (older ones
    /// are read from the log).
    pub retain_entries: u64,
    /// Core for the replication I/O stage (None: not pinned).
    pub core: Option<usize>,
}

impl ClusterConfig {
    pub fn new(id: u32, members: Vec<(u32, SocketAddrV4)>) -> ClusterConfig {
        ClusterConfig {
            id,
            members,
            election_min_us: 50_000,
            election_max_us: 100_000,
            heartbeat_us: 10_000,
            max_append_bytes: 60_000,
            max_inflight: 16,
            retain_entries: 1 << 16,
            core: None,
        }
    }

    pub fn addr_of(&self, id: u32) -> Option<SocketAddrV4> {
        self.members.iter().find(|m| m.0 == id).map(|m| m.1)
    }
}

/// Storage (M2): the log, its durability mode and the snapshot cadence.
#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub log: krusty_log::LogConfig,
    pub durability: Durability,
    /// Take a core snapshot every this many log entries.
    pub snapshot_every: u64,
}

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Where clients send SUBMITs.
    pub ingress: SocketAddrV4,
    /// Where the native stream goes (multicast group or unicast address).
    pub feed: SocketAddrV4,
    /// Where consumers send RECOVER.
    pub recovery: SocketAddrV4,
    /// Interface for multicast (unspecified = OS default; see BASELINE.md
    /// finding 1 before relying on the default route).
    pub interface: Ipv4Addr,
    pub stream_id: u64,
    pub core: CoreConfig,
    /// Largest datagram sent on the feed (DATA frames are packed up to this).
    pub mtu: usize,
    /// Largest accepted payload; must fit one message per frame.
    pub max_payload: usize,
    /// Messages kept for recovery (a power of two).
    pub history: usize,
    pub ring_bytes: usize,
    pub batch: usize,
    pub heartbeat_us: u64,
    pub idle: Idle,
    /// Cores for ingress, core, egress, recovery (None: not pinned).
    pub cores: [Option<usize>; 4],
    /// MoldUDP64 adapter (None: off).
    pub mold: Option<MoldConfig>,
    /// Durable storage (None: in memory only).
    pub storage: Option<StorageConfig>,
    /// Replication (None: a single node). Needs storage in mode (a).
    pub cluster: Option<ClusterConfig>,
    /// Suppression (ARCHITECTURE §8.5): when a second consumer asks for the
    /// same range within this window, retransmit once on the feed instead
    /// of answering each one (0: always answer unicast).
    pub recover_suppress_us: u64,
    /// Shared-memory metrics file for `krusty-stat` (None: private memory).
    pub metrics_path: Option<std::path::PathBuf>,
    /// Called at the start of every stage thread (tests use it to mark the
    /// threads whose allocations they count).
    pub stage_init: Option<fn()>,
}

impl NodeConfig {
    pub fn new(ingress: SocketAddrV4, feed: SocketAddrV4, recovery: SocketAddrV4) -> NodeConfig {
        NodeConfig {
            ingress,
            feed,
            recovery,
            interface: Ipv4Addr::UNSPECIFIED,
            stream_id: 1,
            core: CoreConfig::DEFAULT,
            mtu: 1472,
            max_payload: 1024,
            history: 1 << 16,
            ring_bytes: 1 << 22,
            batch: 64,
            heartbeat_us: 100_000,
            idle: Idle::Sleep { sleep_us: 50 },
            cores: [None; 4],
            mold: None,
            storage: None,
            cluster: None,
            recover_suppress_us: 1_000,
            metrics_path: None,
            stage_init: None,
        }
    }
}

pub use stages::{Node, start};
