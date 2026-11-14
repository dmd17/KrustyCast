//! KrustyCast client SDK (FR-10; gaps K1–K3).
//!
//! - [`Publisher`]: submits messages through a `krusty-session` session and
//!   reports exactly one outcome per message: accepted, rejected or failed.
//! - [`Consumer`]: follows the native stream with a `krusty-stream`
//!   tracker and delivers messages in order, with their seqno and the
//!   leader's timestamp (use it instead of a local clock to build
//!   deterministic consumers and replicas). It can start at any position
//!   and recovers what it misses from the cluster, including from disk.
//!
//! Both are poll-driven: the application calls `poll` from its own loop;
//! the SDK starts no threads.
#![forbid(unsafe_code)]

mod consumer;
mod publisher;

pub use consumer::{Consumer, ConsumerConfig, Delivery, Event as ConsumerEvent};
pub use publisher::{Event as PublisherEvent, Publisher, PublisherConfig};

/// SplitMix64, for recovery jitter.
pub(crate) struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}
