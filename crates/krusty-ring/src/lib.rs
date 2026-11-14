//! Lock-free plumbing between the node's pinned stages (ARCHITECTURE §6).
//!
//! - [`byte_ring`]: a single-producer single-consumer ring of
//!   variable-length records (datagrams, frames). The producer publishes
//!   once per batch, so the cost of the release store is amortized; both
//!   sides cache the other's index to avoid cache-line traffic.
//! - [`history`]: the published stream, written by the core stage and read
//!   concurrently by recovery servers through a seqlock built from atomics
//!   (no `unsafe`).
//!
//! The only `unsafe` is the byte ring's buffer access, argued in place and
//! checked with Miri (`cargo +nightly miri test -p krusty-ring`). A verified
//! (Verus) ring is gap Q4 (M2); see `spikes/verus-ring/REPORT.md` for why it
//! must publish in batches.

pub mod byte_ring;
pub mod history;

/// Pads to two cache lines (adjacent-line prefetch) to avoid false sharing.
#[repr(align(128))]
#[derive(Default)]
pub struct CachePadded<T>(pub T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
