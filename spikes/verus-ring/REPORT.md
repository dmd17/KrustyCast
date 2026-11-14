# M0 spike: Verus on an SPSC ring

Date: 2026-11-14. Question (GAP-ANALYSIS M0): how does Verus build and ship
with cargo, and what does a verified SPSC ring cost at run time?

**Answer: it builds and ships cleanly, but `vstd` atomics are SeqCst-only,
which makes one-way streaming 2.6–4.7× slower than the same ring with
Acquire/Release on this laptop. Round-trip latency is unaffected.** The ring
for `krusty-ring` needs a batched-publish protocol (one atomic store per
batch) before the verified version is fast enough for the hot path.

## What was built

`src/lib.rs`: the SPSC queue from the Verus tutorial
(`examples/state_machines/tutorial/fifo.rs`, a port of the LinearDafny
queue): a tokenized state machine `FifoQueue`, cells as `PCell<T>`, head and
tail as `vstd::atomic_ghost::AtomicU64`. Changed for the spike:

- Non-blocking `try_push` / `try_pop` (the tutorial spins inside the ring).
- Public API; a `decreases` clause on the setup loop instead of disabling
  termination checking.

Verified (13 items, 0 errors, about 2 s with cached `vstd`): memory safety and
data-race freedom (each cell is owned by exactly one side at a time; a popped
cell was written by a push), no panics or overflow. **Not verified: FIFO order
of values.** The two-thread test checks it at run time.

## Building and shipping

| Step | Result |
|---|---|
| Install | Release zip `0.2026.11.02.8bf68ee` plus `rustup install 1.99.1` (Verus pins its toolchain; normal builds keep using stable). No other system changes. |
| Project setup | `cargo verus new --lib`. It also runs `git init`, which nested a repo inside ours (removed). |
| Dependencies | `vstd` and `verus_state_machines_macros` from crates.io, pinned with `=` to the release's date stamp (`0.0.0-2026-11-01-0251`). The macros crate is not added by `cargo verus new`. |
| Verify | `cargo verus verify`: 2 s (the first run builds `vstd` for about 90 s) |
| Plain build | `cargo build` / `cargo test` on **stable** 1.98 work; ghost code is erased |
| Miri | `cargo +nightly miri test` with 4 scheduling seeds: no UB. It found a **leak**: values still in the ring when it is dropped are never dropped (the cells hold `MaybeUninit<T>` and there is no verified `Drop`). |

The leak is accepted for now: `krusty-ring` carries `Copy` handles into
preallocated pools (architecture P3), so a ring never owns heap data. A typed
API should enforce `T: Copy`.

## Cost of SeqCst

`vstd`'s atomics expose no `Ordering` parameter; every operation is `SeqCst`
(`source/vstd/atomic.rs`). On x86, SeqCst loads are plain `mov` but SeqCst
stores are `xchg` (a full barrier), once per push and once per pop.

`examples/bench.rs` compares the verified ring with an unverified twin that is
identical except for Acquire/Release orderings. Capacity 1024, `u64` values,
threads pinned, best of 5 runs, i7-9750H, PREEMPT_RT kernel, hyperthreading
off. These are comparisons on a laptop, not PRD numbers.

| Cores | Variant | Stream ns/msg | RTT p50 | RTT p99 | RTT p99.99 |
|---|---|---|---|---|---|
| 4→5 | verified (SeqCst) | 58.8–65.3 | 227–240 | 280–304 | 309–368 |
| 4→5 | twin (Acq/Rel) | 17.2–22.7 | 214–244 | 260–282 | 299–308 |
| 2→3 | verified (SeqCst) | 61.2 | 224 | 270 | 324 |
| 2→3 | twin (Acq/Rel) | 13.1 | 218 | 257 | 295 |

Streaming throughput suffers 2.6–4.7×; a 1-message round trip is dominated by
the cache-line transfer between cores and barely changes.

## Options for `krusty-ring`

1. **Batched publish in the verified protocol (recommended).** The producer
   checks out several cells, writes them, and publishes the new tail with one
   store (and the consumer likewise frees a batch with one store). This
   amortizes the `xchg` and matches how the pipeline stages already work in
   batches (ARCHITECTURE §6). It needs a state-machine change
   (`Producing(start, count)`) and new invariant proofs.
2. Verify with SeqCst in Verus and ship an Acquire/Release twin checked with
   `loom` and Miri's weak-memory emulation. Faster to build, but then the
   shipped code is not the verified code. Not recommended for a product whose
   pitch is verified code.
3. Contribute orderings to `vstd` (Verus does not model weak memory; this is a
   research-level change).

## Changes to ARCHITECTURE §11

- Verus crates use `vstd` atomics only through a batched protocol; the
  per-element `xchg` is not acceptable on the hot path.
- Rings are typed over `Copy` handles; values in a dropped ring are leaked.
- Miri (with several seeds) runs on the erased code of every Verus crate in CI,
  because the Verus proof covers the ghost model, not the erased binary.
- Pin `vstd` and `verus_state_machines_macros` with `=` to the Verus release's
  date stamp; CI installs that release and its toolchain.

## Batched ring (Q4, M2), 2026-11-14

`src/batched.rs` implements option 1 above and is fully verified (36 items,
0 errors, including the executable `push_batch`/`pop_batch`):

- Protocol `BatchQueue`: the producer reserves up to `n` free cells with one
  transition (`produce_start(n)`, inside the atomic load of `head`),
  receiving their permissions as a map; it writes them and publishes them
  with one transition (`produce_end`, inside the atomic store of `tail`).
  The consumer is symmetric. A batch never crosses the end of the buffer
  (callers loop), so ranges stay contiguous and the invariants stay linear.
- Proved: the per-cell storage invariant (each cell owned by exactly one
  side; published cells initialized, free cells empty), bounds, and that a
  reservation stays valid while the other side moves. A few small lemmas
  (`lemma_free`, `lemma_produce_grows`, `lemma_consume_shrinks`,
  `lemma_consume_keeps`) carry the circular arithmetic.
- Plain-cargo tests (including 5 M values across two threads in order) and
  Miri with 4 seeds pass. FIFO order is still checked by tests, not proved.

Streaming throughput, batches of up to 64, cores 4→5 and 2→3, best of 5:

| Variant | ns/msg |
|---|---|
| verified batched (SeqCst) | 3.3 |
| twin batched (Acquire/Release) | 2.5 |
| verified per element (SeqCst) | 65.7 |
| twin per element (Acquire/Release) | 18.7 |

The verified ring's cost over the unverified one drops from 2.6–4.7× to
1.3×, and it is 20× faster than the per-element verified ring: the SeqCst
store is now paid once per batch.

## Next steps (not done)

- FIFO order as a verified property: add a persistent ghost history
  (`#[sharding(persistent_map)]`, index ↦ value) and prove each pop returns
  the next history element.
- `T: Copy` bound and capacity as a const generic.
