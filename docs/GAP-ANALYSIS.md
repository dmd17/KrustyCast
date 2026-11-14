# KrustyCast: Gap Analysis (current code → v1)

Status: **draft v0.1** (2026-11-13). Measures the code on branch
`moldudp64-downstream` against the [PRD](PRD.md) and the
[architecture](ARCHITECTURE.md).

## 1. Summary

Today KrustyCast is a **single-node prototype**: about 3.2k lines of Rust
in one crate, including 442 lines of end-to-end tests. Its *semantics* are
largely right, and several are already what v1 needs:

- total order
- idempotent client resends with incarnations
- conditional append (per-target compare-and-swap)
- a standard MoldUDP64 feed with re-requests
- a crash-recovery journal

Its *implementation* is not built for the PRD's correctness or performance
bar:

| Dimension | Status |
|---|---|
| High availability | none: one process |
| Performance | **unmeasured**; the hot path has locks, allocation per message, a syscall per datagram and polling timeouts |
| Overload behavior | unbounded queues |
| Verification | none |
| Tests | 25 unit tests and 10 end-to-end tests on loopback; no simulation, fuzzing, benchmarks or CI |

Most of the work is restructuring (sans-I/O cores, staged threading) and
new subsystems (replication, storage, verification), not fixing today's
logic.

## 2. What to keep

| Asset | Where | How it carries forward |
|---|---|---|
| Session semantics: incarnations, idempotent outcomes, NAK, resend | `protocol.rs`, `sequencer.rs` (`Core`), `client/client.rs` | Becomes ingress v2 (architecture §8.1). Outcomes move into replicated state. |
| Conditional append | `sequencer.rs` `process_batch` | Becomes FR-3's conditional mode; unconditional mode is added. |
| Sequencer decision logic | `sequencer.rs:192` `Core::process_batch` | Almost pure already. Remove the journal I/O and it becomes `krusty-core::step`, the first Aeneas target. |
| Consumer stream tracker | `stream.rs` (pure, 9 unit tests) | Becomes `krusty-stream` almost as is. **Best first Aeneas spike.** |
| MoldUDP64 codec and interop tests | `protocol.rs`, `tests/e2e.rs` | Keep the format; rewrite as zero-copy. Keep the `moldudp` interop tests. |
| Journal format ideas | `journal.rs`: CRC, torn-tail truncation, session header | Carry into the segment format (CRC32C, preallocation). |
| Fault scenarios | `tests/e2e.rs`: lossy proxy, crash and resume, lost REJECT | Become seeded scenarios in `krusty-sim`, run under many seeds. |

## 3. Gaps

Columns:
- **Why**: C = correctness, P = performance, O = operations.
- **Size**: S / M / L / XL (relative effort).
- **M**: the milestone that closes it.

### 3.1 Architecture and structure

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| A1 | Logic mixed with I/O. The client sends on the socket while holding its state locks (`client.rs:267`, `:284`). The sequencer core writes the journal inside `process_batch` (`sequencer.rs:279`). | Pure sans-I/O cores plus thin I/O shells (architecture §5) | C, P | L | M1 |
| A2 | One crate with mixed concerns | Crate layout in architecture §4; `forbid(unsafe_code)` in pure crates | O, C | M | M1 |
| A3 | Ad-hoc shutdown: `AtomicBool` flags plus 20–100 ms read timeouts | Staged runtime with explicit stage lifecycle (`krusty-rt`) | P, O | M | M1 |

### 3.2 Replication and availability

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| R1 | Single sequencer process; recovery is a manual restart | Raft cluster with automatic failover (FR-5, NFR-6) | C, O | **XL** | M3 |
| R2 | The journal records *decisions* and lives on one disk | Log of raw inputs, replicated; the state machine re-derives decisions on every node (ADR-4) | C | L | M3 |
| R3 | Clients target one fixed sequencer address | Member list, `NOT_LEADER` redirect, client rotation | C | M | M3 |
| R4 | No snapshots: restart replays the whole journal | Snapshots of core state plus log suffix replay (FR-9) | O | M | M2 |

### 3.3 Hot path performance

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| P1 | Unbounded crossbeam channels (`sequencer.rs:401`, `client.rs:210`) | Bounded SPSC rings; BUSY when full (G9) | C, P | M | M1 |
| P2 | Mutexes on the hot path: `Arc<Mutex<History>>` (`sequencer.rs:148`, locked per batch); client state in three mutexes (`client.rs:102–104`) | Single writer per state; history readable by the re-request thread through a lock-free published index | P | M | M1 |
| P3 | Allocation per message: decoders copy payloads (`protocol.rs:102`, `:304`, `:423`); history stores a `Vec<u8>` per message; encoders allocate | Zero-copy views (`zerocopy`), preallocated slots and slabs; zero allocations enforced in CI (NFR-7) | P | M | M1 |
| P4 | One `recv_from`/`send_to` syscall per datagram; blocking reads with timeouts (`client.rs:122`, `:191`) | `recvmmsg`/`sendmmsg`, busy-poll, pinned threads, spin/back-off strategies | P | M | M1 |
| P5 | Synchronous fsync on the sequencing thread: `j.commit()` blocks sequencing on every batch | Storage on its own thread (io_uring); durability mode (a) takes fsync off the latency path | P | M | M2 |
| P6 | No kernel-bypass path | Transport trait; Onload compatibility; AF_XDP (Tier B) | P | L | M4 |
| P7 | `std::collections::HashMap` for sender and target state (`sequencer.rs:144`) | Fixed-capacity tables: predictable and verifiable | P, C | M | M1 |

### 3.4 Protocol

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| W1 | No magic or version field on any internal format | Versioned formats (FR-15) | O | S | M1 |
| W2 | Every message is a conditional claim (`target_id`, `target_seq`); no unconditional append | Two append modes (FR-3) | C | S | M1 |
| W3 | No BUSY, NOT_LEADER or SESSION_EXPIRED feedback | Full outcome set (architecture §8.1) | C | S | M1 |
| W4 | Dedup tables grow without limit: one entry per incarnation, forever; outcome ring of 4096 per sender (`sequencer.rs:25`) | Bounded, replicated, deterministic eviction | C | M | M1 |
| W5 | Messages limited to one datagram, with no explicit maximum | Explicit maximum size (PRD open question 2); optional fragmentation | C | S–M | M1 |
| W6 | Big-endian hand-written upstream codecs | Little-endian fixed layouts (ADR-8); Kani-checked decoders | P, C | M | M1 |

### 3.5 Storage

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| S1 | One growing journal file through `BufWriter`, CRC32 | Preallocated segments, CRC32C, io_uring, alignment | P, O | L | M2 |
| S2 | Re-requests served only from in-memory history (last 1 M messages) | Served from segments via index and mmap for every retained session (FR-7) | C | M | M2 |
| S3 | No indices, retention or purge | Seqno and log-index indices; per-session retention | O | M | M2 |

### 3.6 Client and consumer SDK

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| K1 | Re-requests sent immediately to one server address (storm risk) | Random delay, spread across nodes (architecture §8.4) | P | S | M2 |
| K2 | Callback API `Fn(Vec<u8>)`: copies, runs under a mutex, no position | Zero-copy delivery with seqno and position; replay-from-position; deterministic-consumer API (FR-10) | P | M | M2 |
| K3 | Abandoned sends only counted and logged (`failed()`) | Explicit failure delivered to the application | C | S | M2 |
| K4 | Client logic lives in the I/O shell | `krusty-session` sans-I/O core | C | M | M1 |
| K5 | Multicast-only egress | Unicast fan-out (FR-6b) | O | M | M4 |

### 3.7 Assurance

IDs here are gap IDs; the PRD column names the assurance requirement each one closes. PRD V5 (unsafe code confined and checked) has no gap yet: the code has no `unsafe` today.

| ID | PRD | Current | Target | Why | Size | M |
|---|---|---|---|---|---|---|
| Q1 | V1 | No specification | TLA+/Quint spec of sessions, sequencer, Raft and egress, with G1–G7 | C | L | M0–M3 |
| Q2 | V2 | No proofs | Aeneas→Lean proof of `krusty-core::step` (refinement plus invariants) | C | XL | M1–M3 |
| Q3 | V3 | — | Trace validation of `krusty-raft` against the spec | C | L | M3 |
| Q4 | V4 | — | Verus-verified SPSC ring (or loom + Kani) | C | L | M2 |
| Q5 | V6 | No simulation | `krusty-sim`: seeded network, disk and clock faults; G1–G9 checkers; nightly runs | C | L | M1→ |
| Q6 | V7 | No fuzzing | `cargo-fuzz` on all decoders and on ingress | C | S | M1 |
| Q7 | — | No CI | CI: build, tests, Kani/Miri, model checking, proof re-check, alloc guard, perf smoke test | O | M | M0 |
| Q8 | V8 | — | Jepsen-style real-network fault testing | C | M | M4 |

### 3.8 Performance evidence

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| E1 | No benchmark at all | Open-loop load generator; coordinated-omission-correct histograms; submit-to-feed round trip (PRD §7) | P | M | M0 |
| E2 | No reference hardware | Defined reference setup; tuning guide (isolated cores, IRQ affinity, busy-poll) | P | S | M0 |
| E3 | No comparison | Aeron Cluster baseline on the same hardware and workload | P | M | M0 / M4 |

### 3.9 Operations and security

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| O1 | CLI flags only | TOML config; static membership | O | S | M1 |
| O2 | `println!`/`eprintln!` logging | Shared-memory counters, per-stage histograms, binary event ring, `krusty-stat` | O, P | M | M1 |
| O3 | No cluster tooling | Status, snapshot, session roll, leader handoff from the CLI | O | M | M4 |
| O4 | No authentication | Pre-shared-key MAC on ingress and replication (FR-14) | C | M | P1 |

## 4. Replace rather than evolve

These parts are cheaper to rewrite than to refactor:

- **The thread-and-mutex client shell** (`client/client.rs`). Keep its
  semantics and tests; rebuild it as `krusty-session` plus a staged shell.
- **The socket loops in `sequencer.rs`.** Keep `Core`; replace everything
  around it with `krusty-rt` stages.
- **`journal.rs` as storage.** Keep the record format ideas. Segments,
  indices and io_uring are a new design.
- **crossbeam channels on the data path.** They stay in the control plane
  only.

## 5. Roadmap

Ordered by dependency. Sizes are relative; calendar estimates come after
the M0 spikes.

**M0: Baselines and spikes.**
- Build the benchmark harness (E1, E2).
- Measure today's single node and Aeron Cluster on the reference hardware
  (E3).
- CI (Q7).
- TLA+ skeleton (Q1).
- Tool spikes:
  - Charon/Aeneas on `stream.rs`: does it translate, and how much
    rewriting does it need?
  - Verus on a small SPSC ring: how does it build and ship with cargo?
- *Exit:*
  - Baseline numbers published.
  - PRD §7 targets confirmed or revised.
  - Coding rules for the verified subset confirmed (architecture §11.3).

**M1: Single-node performance core.**
- Gaps A1–A3, P1–P4, P7, W1–W6, K4, Q5 (single-node simulation), Q6, O1, O2.
- *Exit:*
  - Single-node submit-to-feed round trip meets the Tier A targets minus
    the replication hop.
  - Zero allocations on the hot path.
  - Simulation runs nightly.
  - Lean translation of `krusty-core` compiles and the proof is started.

**M2: Storage and consumer SDK.**
- Gaps P5, R4, S1–S3, K1–K3, Q4.
- *Exit:*
  - Full-session replay and re-request from disk.
  - Snapshot recovery within its budget.
  - Mode (b) target met.

**M3: Replication and failover.**
- Gaps R1–R3, Q3; Q1 complete, Q2 complete.
- *Exit:*
  - 3-node cluster meets NFR-1–6.
  - Model checking and trace validation are green.
  - Nightly simulation with faults runs clean.
  - The core proof is checked in CI.

**M4: Hardening and release.**
- Gaps P6 (Tier B), K5, O3, Q8, E3 (final comparison), O4 (if pulled
  forward).
- *Exit:* the PRD §10 release criteria.

The verification track runs alongside from M0. It blocks a release only at
the PRD §10 criteria, so proof progress never stalls engineering, and
engineering can never ship without proofs.

## 6. Transition risks

| Risk | Mitigation |
|---|---|
| The M0 baseline shows the targets are unrealistic for kernel UDP | Revise the targets explicitly in the PRD; pull Tier B forward. |
| The Aeneas spike shows the core needs heavy rewriting to translate | Acceptable: the core is small. If the tools block us, fall back to Verus for the core, or to TLA+ plus simulation with proofs deferred. |
| Raft takes longer than planned | M1 and M2 deliver standalone value (a faster single node with durable storage). M3 can start against the spec in parallel with M2. |
| Refactoring breaks today's tested semantics | Port the e2e scenarios into `krusty-sim` *before* restructuring, and keep them green throughout. |
