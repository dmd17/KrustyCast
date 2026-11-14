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
//! Nothing on these paths allocates after startup. State is in memory only
//! (durability arrives with storage and replication, M2/M3).

pub mod config;
pub mod metrics;
pub mod stages;

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
            metrics_path: None,
            stage_init: None,
        }
    }
}

pub use stages::{Node, start};
