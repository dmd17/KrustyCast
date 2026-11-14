# KrustyCast: Architecture

Status: **draft v0.2** (2026-11-14). Implements the [PRD](PRD.md). The
current code's distance from this design is tracked in the
[gap analysis](GAP-ANALYSIS.md).

## 1. Context

A KrustyCast cluster is 3 (or 5) nodes. Clients (gateways, services)
submit messages to the leader. The cluster replicates them, decides each
one's outcome deterministically, and publishes accepted messages on the
**native stream** (§8.3). Consumers, including the deterministic applications
built on top, read the stream and recover gaps from any node. Optionally,
the same sequence is also published as a standard **MoldUDP64** feed for
external consumers (§8.4).

```
   clients / gateways                                 consumers / replicas
          │  ▲                                                ▲   │
 submit   │  │ outcome                          native stream │   │ recover
 (UDP)    ▼  │ (ACCEPTED/REJECTED/BUSY/...)    (mcast/unicast) │   ▼  (any node)
   ┌─────────────────────── KrustyCast cluster ───────────────────────────┐
   │  ┌──────────── leader ────────────┐      ┌──── followers (×2) ────┐  │
   │  │ ingress ─▶ consensus ─▶ sequencer ─▶ egress    consensus         │  │
   │  │              │  ▲       core        │        sequencer core      │  │
   │  │              ▼  │ acks              │        recovery server     │  │
   │  │          replication ◀──────────────┼──────▶ storage             │  │
   │  │          storage                    │                            │  │
   │  └─────────────────────────────────────┘      └────────────────────┘  │
   └──────────────────────────────────────────────────────────────────────┘
```

The optional MoldUDP64 adapter (§8.4) publishes the same sequence as a
second feed for external consumers; it is not shown.

## 2. Design principles

| # | Principle | Why it matters |
|---|---|---|
| P1 | **Deterministic, sans-I/O cores.** Consensus, sequencing, client sessions and consumer stream tracking are pure state machines: `step(state, input) -> outputs`. Time and randomness are inputs. | Enables formal proofs (Aeneas needs pure, safe code), deterministic simulation, replay, and bit-identical replicas. |
| P2 | **Single writer per piece of state.** Each core is owned by one thread, and threads talk through SPSC rings. | No locks and no contention on the hot path; reasoning stays sequential. |
| P3 | **Bounded everything.** Fixed-capacity rings, tables and pools; overload produces an explicit BUSY. | Predictable memory and tail latency (G9). |
| P4 | **Hot path: no allocation, no locks, no syscall per message.** | Low jitter: allocators, futexes and syscalls are the main sources of tail latency once GC is gone. |
| P5 | **Publish after commit.** Nothing leaves the cluster until it is committed. | No retractions (G3); egress needs no coordination (§8.3). |
| P6 | **Native protocols inside, standards as adapters.** Our own versioned protocols wherever we control both ends; standard formats (MoldUDP64) as adapters at the edge. | We get the semantics we need (back pressure, recovery, cloud) without giving up interoperability; rolling upgrades. |
| P7 | **Verify the core, test the shell.** | Puts proof effort where bugs are subtle (§11). |

## 3. Key decisions (ADR summaries)

| ADR | Decision | Alternatives considered | Rationale |
|---|---|---|---|
| 1 | Rust only. Best-available crates first, replaced as needed (§15). | Java/Aeron-based | The PRD hypothesis; verification tools; memory footprint. |
| 2 | **A sequencer, not an application container.** Applications are external deterministic consumers of the feed. Their outputs are submitted back as inputs. | Host services in-cluster (Aeron Cluster's `ClusteredService`) | Keeps the verified core small and the latency path short; apps scale and deploy independently. Revisit for v2. |
| 3 | **Raft, our own sans-I/O implementation** (**accepted** 2026-11-14). Uses pre-vote and check-quorum, and pipelined batched replication. | `raft-rs` (TiKV), `openraft` (evaluated, §17), Viewstamped Replication | We need a sans-I/O, allocation-free, verifiable core shaped for latency. Raft has the most formal material (e.g. Ongaro's TLA+ spec). `openraft` and `raft-rs` serve as design references and as checklists for features and tests. |
| 4 | **The log holds raw inputs.** The sequencer state machine runs deterministically on every node over committed entries. Egress is a projection of that. | Log only accepted outputs | All nodes hold identical state, including dedup and outcome tables, so failover is seamless and REJECTs survive failover (this fixes a current gap). |
| 5 | Publish after commit (P5). | Speculative publish with rollback | Rollback is unacceptable to feed consumers. |
| 6 | **Durability by replication** by default (mode a); fsync-before-commit as an option (mode b). | fsync always | Mode (a) takes fsync off the latency path. Mode (b) is for deployments where every node can lose power at once. |
| 7 | **Thread-per-core with busy-polling**; no async runtime on the hot path. Tokio is allowed in the control plane. | Tokio everywhere | Async executors add wakeups, scheduling jitter and allocation. Spin and back-off strategies stay configurable (like Agrona's idle strategies). |
| 8 | **Our wire formats are fixed-layout, little-endian, zero-copy.** Only the MoldUDP64 adapter is big-endian. Views are hand-written with safe little-endian accessors over `&[u8]` (`krusty-wire`, 2026-11-14), not `zerocopy`: its derive and trait machinery falls outside what Aeneas and Kani handle well, and the formats are small. | SBE (it has a Rust generator); `zerocopy` derives | Native byte order; zero-copy views; formats simple enough to verify. SBE remains an option if schemas multiply. |
| 9 | **Verification split** (§11): TLA+/Quint for protocols; Aeneas→Lean for pure cores; Verus for low-level concurrent structures; Kani/Miri for unsafe code; simulation for everything. | One tool for all | Each tool is strongest at a different layer; none covers everything. |
| 10 | **One total order per cluster** in v1. Scale out with multiple independent clusters (shards). | Multi-stream clusters | Simplest correct design; most venue cores need exactly one order. |
| 11 | **The native stream is the egress backbone; MoldUDP64 is an optional adapter** (**accepted** 2026-11-14). Both are projections of the same committed sequence and share global seqnos. | MoldUDP64 as the only egress (the previous design) | Mold is the right format for external feed consumers (UC4), but it has no back pressure, no "unavailable, use a snapshot" answer, re-requests that storm, and multicast-only assumptions. Those gaps land on our main use cases (UC1–UC3), where our own SDK is on both ends and interoperability buys nothing. |

## 4. Crate layout

| Crate | Responsibility | Pure? | Verification |
|---|---|---|---|
| `krusty-wire` | Ingress, replication and native stream formats. Zero-copy views; version and magic checks. | yes | Kani (no panics, bounds); Aeneas/Lean (decode∘encode = id) for core formats |
| `krusty-mold` | MoldUDP64 adapter: publisher and standard re-request server, fed from the committed sequence (today's MoldUDP64 code). | codec yes, server no | Kani on the codec; `moldudp` interop tests |
| `krusty-core` | **Sequencer state machine.** Dedup, append modes, conditional append, outcome table, egress projection, snapshot state. | **yes** | **Aeneas→Lean proof against the spec** (V2) |
| `krusty-raft` | Raft as a sans-I/O state machine (election, replication, commit index, membership). | yes | TLA+ model checking plus trace validation (V3); Lean proof is a stretch goal |
| `krusty-session` | Client-side session core: claims, resends, outcome handling. | yes | Aeneas→Lean (P1) |
| `krusty-stream` | Consumer stream tracker for the native stream: ordering, gaps, recovery scheduling, UNAVAILABLE handling (evolves from today's `stream.rs`). | yes | Aeneas→Lean (V2b) |
| `krusty-log` | Segmented log storage, indices, snapshots, recovery. | no (I/O shell plus a pure index core) | Simulation with disk faults; Verus for offset/segment arithmetic |
| `krusty-ring` | SPSC rings, buffer pools, cache-padded counters. | no (atomics) | **Verus** (V4) or loom + Kani; Miri |
| `krusty-net` | Transport trait. Kernel UDP (`recvmmsg`/`sendmmsg`, busy-poll); AF_XDP later; Onload-compatible. | no | Simulation (simulated transport); fuzzing |
| `krusty-rt` | Thread pinning, spin/back-off strategies, timer wheel, stage wiring. | no | loom on the wiring; benchmarks |
| `krusty-node` | The node binary: wires cores to I/O; config; control plane. | no | Simulation; real-network fault tests |
| `krusty-client` | Client and consumer SDK (shells around `krusty-session` and `krusty-stream`). | no | Simulation; e2e |
| `krusty-sim` | Deterministic simulator: drives all pure cores with simulated network, disk and clocks; invariant checkers. | yes | — (it is the checker) |
| `krusty-tools` | `krusty-stat`, log inspector, cluster control, benchmark harness. | no | — |
| `spec/` | TLA+/Quint specs, Lean specs and proofs, trace-validation glue. | — | — |

Rule: **pure crates do no I/O.** They read no clock and no randomness, and
use no `unsafe`. They avoid trait objects, closures and `async`, and use
fixed-capacity collections that we implement ourselves. This keeps them
inside Aeneas's supported subset (§11.3).

## 5. Core state machines (sans-I/O shape)

Every core has the same shape. Here is the sequencer core:

```rust
// krusty-core (simplified)
pub enum Input<'a> {
    Committed { log_index: u64, entry: &'a [u8] }, // from consensus, in log order
    SnapshotLoad { bytes: &'a [u8] },
}

pub enum Output<'a> {
    Publish { seqno: u64, data: &'a [u8] },        // accepted → egress
    Outcome { to: ClientRef, outcome: Outcome },   // → feedback (leader only)
}

pub struct Core { /* fixed-capacity dedup table, key versions, next seqno, ... */ }

impl Core {
    /// Deterministic: same state + same input ⇒ same state' + same outputs.
    pub fn step(&mut self, input: Input<'_>, out: &mut Outbox) { ... }
}
```

The node's I/O shell feeds `Input`s from rings and drains `Output`s to
rings. The simulator feeds the same cores from a seeded event queue. The
Lean proof is about `Core::step` itself, not a model of it.

`krusty-raft` has the same shape. Its inputs are peer messages,
`Tick { now }`, client entries, and storage completions ("durable up to
index i"). Its outputs are messages to peers, storage writes, and commit
index advances. Election timeouts come from a seeded PRNG that is part of
the state, so they are deterministic too.

## 6. Data path and threading (per node)

Each stage is a pinned thread on an isolated core. Stages are connected by
SPSC rings and drain whatever is available, up to a batch limit. That gives
natural batching: no batching timers, low latency when lightly loaded,
high throughput when heavily loaded.

```
 NIC ─▶ [T-ingress]  recvmmsg, busy-poll, validate, zero-copy into slots
            │ SPSC
            ▼
        [T-core]  raft step + sequencer step (single writer of all core state)
         │   │  ▲ acks/commit       ▲ durable index
         │   ▼  │                   │
         │ [T-repl] ⇄ followers   [T-store] io_uring segment writes, fsync policy
         ▼ SPSC
        [T-egress] sendmmsg: native stream frames + client outcomes
        [T-recov]  recovery server: reads committed log via mmap (any node)
        [T-mold]   optional MoldUDP64 adapter + re-request server (own core,
                   off the latency path)
        [control]  config, CLI, counters export (unpinned, may use tokio)
```

On followers, `T-repl` receives AppendEntries, `T-store` persists, and
`T-core` applies committed entries to its own sequencer core. Every node
can therefore serve recovery (and unicast subscribers) and take over
immediately.

## 7. Latency budget (Tier A, estimates to be validated in M0)

| Step | Estimate |
|---|---|
| Client → leader (kernel UDP, busy-poll) | 5–10 µs |
| Ingress decode and ring hand-off | < 1 µs |
| Leader → follower AppendEntries | 5–10 µs |
| Follower append (mode a) and ack | ~1 µs |
| Ack → leader | 5–10 µs |
| Commit, sequencer step, egress encode | < 1 µs |
| Leader → consumer multicast (native stream) | 5–10 µs |
| **Submit → feed, one-way total** | **~22–43 µs** |

The PRD's submit-to-feed *round-trip* target also covers getting the
result back to the submitting host. The budget is dominated by four
network hops, which is why Tier B (kernel bypass, ~1–2 µs per hop) is the
next lever after software inefficiencies are removed.

## 8. Protocols

### 8.1 Ingress (client ⇄ leader), protocol v2

Fixed little-endian 48-byte header: `magic u16 | version u8 | type u8 |
flags u16 | mode u8 | reserved u8 | client_id u32 | len u16 | reserved u16 |
incarnation u64 | client_seqno u64 | key u64 | expected_version u64`, then
the payload. Fields are naturally aligned. The exact layouts of all
native formats live in `krusty-wire` (module docs), which is the reference.
- `mode` = unconditional or conditional.
- `incarnation` is random per client process, as it is today.

Feedback is a fixed 48-byte message carrying `client_id`, `incarnation`,
`client_seqno` and two type-specific fields. The incarnation lets a
restarted client ignore outcomes meant for its previous incarnation.
Types:

| Type | Meaning |
|---|---|
| `ACCEPTED{seqno, global_seqno}` | Sequenced. |
| `REJECTED{seqno, key, current_version}` | Conditional append lost. |
| `NAK{expected}` | Gap in this client's seqnos. |
| `BUSY{retry_after_us}` | Back pressure. |
| `NOT_LEADER{leader}` | Talk to another node. |
| `SESSION_EXPIRED{}` | Dedup state for this incarnation was evicted. |

The **dedup table is replicated state** (ADR-4):
- It is bounded: a fixed number of sessions, each holding a ring of recent
  outcomes.
- Idle sessions are evicted deterministically (by log position, never by
  wall clock).
- Eviction is answered with `SESSION_EXPIRED`, so clients never get a
  silent duplicate.

Clients resend unanswered messages with back-off, as they do today. After
failover they get `NOT_LEADER` or time out, and rotate through the member
list.

### 8.2 Replication (leader ⇄ followers)

- Raft AppendEntries carrying batches of raw ingress entries. Each entry
  carries the leader's append timestamp, so the native stream's
  `timestamp_ns` is replicated state, not a local clock reading.
- Pipelined: several batches in flight, bounded by a per-follower window.
- Pre-vote and check-quorum prevent disruptive elections.
- Heartbeat and election timeouts are tuned for the NFR-6 failover target.
- Followers ack on receipt (mode a) or after fsync (mode b).
- The commit index advances at quorum. Log index and egress seqno are
  different numbers: rejected entries consume a log index but no seqno.

### 8.3 Native stream (cluster → consumers)

The backbone egress protocol: our SDK on one end, the cluster on the
other. Fixed little-endian layout, versioned (P6, ADR-8). Sketch:

- **Frame header (24 bytes):** `magic u16 | version u8 | type u8 |
  flags u16 | count u16 | stream_id u64 | seqno u64`. `stream_id` identifies cluster and
  session; it replaces Mold's 10-byte ASCII session name.
- **Message block (40-byte header, padded to 8):** `len u32 | flags u32 |
  timestamp_ns u64 | client_id u32 | reserved u32 | key u64 | version u64 |
  payload`.
  - `timestamp_ns` is assigned by the leader when it appends the entry, so
    it is part of the replicated log and deterministic. Applications use it
    instead of reading their own clocks (G7).
- **Frame types:**

| Type | Direction | Meaning |
|---|---|---|
| `DATA` | cluster → consumer | Messages `seqno .. seqno+count`. A retransmit flag marks recovery traffic. |
| `HEARTBEAT{next_seqno}` | cluster → consumer | Liveness; exposes tail loss. |
| `END_OF_SESSION{next_seqno}` | cluster → consumer | Session finished. |
| `RECOVER{from, count}` | consumer → any node | Request a range. |
| `UNAVAILABLE{from, earliest_retained}` | node → consumer | The range has aged out: load an application snapshot or replay from an archive. Mold has no equivalent; there the consumer can only time out. |
| `SUBSCRIBE` / `STATUS{position}` | consumer → node | Unicast mode registration and keep-alive; position reports feed the lag counters. |

**Transport modes.** The same frames run in two modes:
- **Multicast** (on-prem/colo): only the leader publishes.
- **Unicast fan-out** (cloud, FR-6b): consumers `SUBSCRIBE` to a node,
  preferably a follower, and each node serves a bounded number of
  subscribers.

**Flow control.** The cluster never waits for a consumer: a slow consumer
must not slow the venue. Consumers that fall behind recover. In unicast
mode, a subscriber that lags beyond a configured limit is dropped with an
explicit reason and must resubscribe from its position. Consumer positions
are visible in counters, so lag is observable before it becomes loss.

**Leader changes.** Two leaders publishing at once during a handover is
harmless. By Raft's log-matching property and determinism (G7), both
publish the same messages for the same seqnos (packet boundaries may
differ), and consumers drop duplicates by seqno. Egress safety therefore
doesn't depend on a single publisher.

**Indexing.** Every node keeps an egress index (seqno → log position) and
serves recovery from memory or mmap.

### 8.4 MoldUDP64 adapter (optional, ADR-11)

For external consumers that expect a standard feed (UC4):
- A separate multicast group carrying the same global seqnos as the native
  stream, so the two are always cross-checkable.
- A standard MoldUDP64 re-request server, as the current code implements
  and tests against the `moldudp` crate.
- Session name, heartbeats and end-of-session per the MoldUDP64 spec.
- Message data is the current big-endian `SequencedMessage` layout (sender,
  target, target seq, payload), plus the timestamp in a later version.
- Runs on its own stage and core (`T-mold`). It is not on the native
  stream's latency path, and the PRD latency targets are measured on the
  native stream.

### 8.5 Consumer recovery (native stream)

- **Delayed, randomized requests.** A consumer that detects a gap waits a
  small random delay before sending `RECOVER`. Requests are spread across
  nodes.
- **Suppression.** If several consumers request the same range within a
  short window, the node retransmits it once on multicast. Consumers
  waiting on that range cancel their own pending requests when they see
  the retransmission. This prevents the re-request storm that plain Mold
  suffers.
- **Rate limits.** Per-consumer recovery rate limits keep one consumer from
  starving the others.
- **Aged-out data.** `UNAVAILABLE` sends the consumer to its snapshot path
  (FR-10b) instead of retrying forever.

## 9. Storage

- **Segments.** Preallocated fixed-size files (e.g. 1 GiB). Records are
  length-prefixed with CRC32C and aligned. Written by `T-store` via
  io_uring. A torn tail is truncated on recovery, as today.
- **Indices.** log index → offset; egress seqno → log index. Both are
  rebuildable from segments.
- **Snapshots.** Sequencer core state (dedup and outcome rings, key
  versions, next seqno) at a log index, written asynchronously. Recovery
  loads the latest snapshot and replays the log suffix. Raft log compaction
  is permitted only below both the snapshot and the retention horizon.
- **Retention.** Whole sessions are retained (for replay and audit, UC3) up
  to a configured budget. Retention runs per session, not per message.

## 10. Performance engineering

- **Memory.**
  - Preallocated rings of fixed slots and payload slabs; no allocation per
    message.
  - Decoding is a zero-copy view (`zerocopy::Ref`).
  - CI runs hot-path stages under an allocation-forbidding guard (e.g.
    `assert_no_alloc`) after warmup.
- **CPU.**
  - Pinned threads on isolated cores, NUMA-local to the NIC.
  - Cache-padded ring indices.
  - Hand-rolled fixed-capacity tables instead of `HashMap` (predictable,
    and verifiable).
- **Syscalls.** Batched receive and send (`recvmmsg`/`sendmmsg`),
  `SO_BUSY_POLL`, and UDP GSO for egress bursts where it helps.
- **Transport tiers.**
  - Tier A: tuned kernel UDP.
  - Onload: transparent, no code change.
  - AF_XDP via `krusty-net` (e.g. `xsk-rs`).
  - Vendor APIs (ef_vi, DPDK) only if a customer requires them.
- **Time.** TSC-based clock (`quanta`) for measurement. Wall time is used
  only in the control plane.
- **Logging.** No formatting on the hot path. Events go to a binary ring
  that is decoded offline.

## 11. Verification architecture

### 11.1 Layers

| Layer | Artifact | Tool | Properties | In CI |
|---|---|---|---|---|
| Protocol design | `spec/*.tla` (or Quint): sessions, sequencer state machine, Raft (based on Ongaro's spec), egress and consumer | TLC / Apalache | G1–G7 invariants; G8 under fairness, for small bounds | every PR (small bounds); nightly (larger) |
| Pure cores | `krusty-core`, `krusty-stream`, `krusty-session`, core codecs | Charon + Aeneas → Lean | Refinement: `step` implements the spec actions; invariants preserved; codec round-trip | proofs re-checked on each PR touching the core |
| Consensus implementation | `krusty-raft` | Trace validation: simulation traces checked against the TLA+ spec | The implementation's behavior is allowed by the spec | nightly |
| Concurrency primitives | `krusty-ring`, buffer pools | Verus (tokenized state machines, tracked permissions); loom; Kani | No data races; FIFO; no lost or duplicated slots | every PR |
| Unsafe and panics | all `unsafe`; decoders | Kani, Miri | No UB; decoders total (never panic) | every PR |
| Whole system | `krusty-sim` | Deterministic simulation with fault injection, swarm testing | Executable G1–G9 checkers | PR (short); nightly (≥ 100 M ops) |
| Real world | cluster on real NICs | Jepsen-style harness | G1–G9 under real faults | per release |
| Inputs | all decoders, ingress | `cargo-fuzz` | No crashes; spec-conformant rejection | continuous |

### 11.2 Proof structure for the sequencer core

1. Specify the abstract sequencer in Lean (mirroring the TLA+ actions):
   state = (dedup map, key versions, next seqno); action = apply one
   committed entry.
2. Aeneas translates `Core::step` to a Lean function.
3. Prove a refinement: an abstraction function `α(Core)` maps concrete
   state to spec state, and `α(step(s, e)) = spec_step(α(s), e)`.
4. Prove the invariants (G4, G5, G7) on the spec, so they carry over to the
   code.

The TLA+ model checks the *interaction*: Raft plus sessions plus egress. The
Lean proof checks that the *code* matches the model's sequencer action.

### 11.3 Coding rules for verified crates

These come from current tool limitations, confirmed by the M0 spike
(`spikes/aeneas-stream/REPORT.md`, Aeneas `0d21a9ec`):
- **Aeneas:** a subset of safe Rust. No early `return` inside any loop and
  no `break`/`continue` to outer loops. No generics instantiated with mutable
  references. Standard-library items need hand-written Lean models: the
  unmodified `stream.rs` translates, but depends on 54 unmodelled externals
  (`BTreeMap`, iterators, `Instant`), so nothing about it can be proved
  without first modelling those.
- **Charon:** describes itself as beta.

The rules:
- `#![forbid(unsafe_code)]`. No interior mutability, `async`, trait
  objects or closures in core logic.
- Own fixed-capacity collections (no `std::collections`) with Lean models
  that we write once.
- Loops written in the supported shapes: no early `return` inside a loop;
  return after the loop or carry a flag. A lint script enforces this
  before running Charon.
- Don't `match` on an array or `Vec` element in place; copy it to a local
  first. Until an Aeneas bug is fixed, don't assign enum values into array
  elements inside loops (it generates ill-typed Lean); use whole-array
  assignment.
- Arithmetic on values from the wire is range-validated at decode time or
  checked. Aeneas models every `+` as able to fail, so proofs carry explicit
  no-overflow preconditions.
- No `#[derive(Debug)]` or other unused std impls in verified crates (or gate
  them behind a `cfg`): each becomes an axiom in the Lean output. Every proof
  module ends with `#print axioms` for its main theorems. CI fails on
  `sorryAx` or on any axiom outside an allowlist.
- Generated Lean is checked in and regenerated by CI; `lake build` re-checks
  the proofs on every change. The verification suite includes mutation checks
  (break the Rust, confirm the proof fails).
- **Verus** (M0 spike, `spikes/verus-ring/REPORT.md`): `vstd` atomics are
  SeqCst only (Verus does not model weaker orderings). A SeqCst store is an
  `xchg` on x86, which made a per-element verified ring 2.6–4.7× slower at
  streaming than an Acquire/Release twin. Verified rings therefore publish
  in batches (one atomic store per batch). Rings carry `Copy` handles (a
  dropped ring leaks what it holds). Miri runs on the erased code of every
  Verus crate, since the proof covers the ghost model, not the binary.
- Toolchains pinned: Charon pins its Rust toolchain; Verus ships its own
  (`vstd` and `verus_state_machines_macros` pinned with `=` to the
  release's date stamp). Proofs live in `spec/` with a pinned
  `lean-toolchain`.

The spike proved, for the restricted rewrite of the stream tracker, that
delivering the next expected message emits a gap-free, in-order run and cannot
fail (about 130 lines of Lean, with most bounds discharged by Aeneas's `step`
tactics).

### 11.4 Trusted computing base (assumed, not verified)

- The Rust compiler and LLVM.
- The Lean kernel, Z3, and the Aeneas/Charon translation.
- The OS, NIC and drivers.
- The I/O shell code (covered by simulation and testing, not proof).
- The specifications themselves: a wrong spec proves the wrong thing.
  Reviewing the spec is part of the design process.

## 12. Observability

- Counters in a shared-memory file. `krusty-stat` reads them live.
  Examples: per-stage ring depth, BUSY count, NAKs, re-requests served,
  elections, commit lag per follower.
- HdrHistogram latency per stage, plus end to end, from TSC timestamps
  carried in slot metadata.
- Binary event ring: elections, membership, session evictions, journal
  truncations.
- Control-plane health endpoint and cluster status in the CLI.

## 13. Configuration and operations

- One TOML file: members, addresses, cores, durability mode, sizes and
  limits, retention.
- Static membership in v1. Dynamic membership via Raft joint consensus in
  P2.
- Session roll (end-of-session, then a new session name) and controlled
  leader handoff from the CLI.
- Every wire format is versioned. Rolling upgrades (P1) require nodes to
  accept version N and N+1.

## 14. Security (v1 assumptions)

- An isolated, trusted network.
- P1: per-client pre-shared-key MAC on ingress, and authenticated
  replication links.
- P2: encryption.
- The recovery and re-request servers serve only committed stream data.

## 15. Crate inventory (adopt now, replace when justified)

| Need | Now | Replace when |
|---|---|---|
| Socket setup | `socket2`, `libc`/`nix` (`recvmmsg`, `sendmmsg`) | AF_XDP (`xsk-rs`) for Tier B |
| Storage I/O | `io-uring`, `memmap2` | — |
| Zero-copy formats | `zerocopy` | SBE generator if schemas grow |
| Checksums | `crc32c` (hardware accelerated) | — |
| Rings | `rtrb` (SPSC) initially | Our own Verus-verified `krusty-ring` (V4) |
| Clock / metrics | `quanta`, `hdrhistogram` | — |
| Pinning | `core_affinity` | — |
| Control plane | `tokio`, `clap`, `serde` + `toml`, `tracing` | — |
| Testing | `proptest`, `cargo-fuzz`/`libfuzzer-sys`, `loom`, Kani, Miri, `criterion` | — |
| Raft references | `openraft`, `raft-rs`: design and test references, not dependencies (§17) | — |
| Interop tests | `moldudp` (tests only) | — |

## 16. Open design questions

1. Should the leader's own append count toward quorum before its local
   write completes in mode (a)? (Latency versus correlated-failure
   exposure.)
2. Outcome-ring depth per session and dedup capacity. This needs PRD
   answers on client counts.
3. Unicast egress at scale: followers only, or a separate relay tier?
4. Is the MoldUDP64 adapter P0 or P1? It depends on the first market
   (PRD open question 1).
5. Whether to adopt SBE now for schema evolution, or stay with `zerocopy`
   structs plus explicit versioning.
6. Lean (via Aeneas) or Verus for `krusty-session`. Decide after the M0
   spikes.

## 17. Prior art evaluated

Recorded so these don't get re-litigated. Each was evaluated against the
upstream source as of 2026-11-14.

**openraft** (databendlabs/openraft, v0.10.0, MIT/Apache-2.0). *Decision:
not adopted (ADR-3); used as a reference.*
- **Strengths:**
  - mature and actively maintained
  - complete feature set: pre-vote, CheckQuorum, leader transfer and
    lease reads, joint consensus, learners, snapshots, pipelined
    replication
  - a deterministic simulation fuzzer (Turmoil) on every CI build and
    Jepsen on every push to main
  - its internal `Engine` (events in, `Command`s out) confirms our
    sans-I/O approach
- **Why not adopted:**
  - **Latency.** Its README reports 33,000 writes/s with a single writer
    (~30 µs per sequential write), apparently in its minimal benchmark
    with networking and storage stubbed out. That alone would consume our
    p50 budget. It publishes no latency percentiles.
  - **Async model.** It is built from async tasks and channels with
    per-request oneshot channels, which conflicts with ADR-7.
  - **The engine can't be used as a pure core.** It is `pub(crate)`, reads
    the clock itself (`C::now()`), carries runtime oneshot senders in its
    commands, and allocates.
  - **Too large to verify.** About 6k lines in the engine (~64k for the
    crate), heavily generic over `RaftTypeConfig`; well outside Aeneas's
    subset.
  - **API unstable** before 1.0.
- **How we use it:**
  - Its feature list is the scope checklist for `krusty-raft`.
  - Its invariant checks and Jepsen fault scenarios seed our checkers in
    `krusty-sim`.
  - Its engine/command split serves as a design reference.

**aeron-rs** (UnitedTraders/aeron-rs, v0.1.8, Apache-2.0). *Decision: no
value for KrustyCast.*
- It is an Aeron *client* port only: it needs Aeron's C media driver, and
  has no Archive or Cluster client.
- Low activity.
- Its shared-memory primitives are unsound under Rust's memory model:
  `get_volatile` and `put_ordered` use plain non-atomic reads and writes
  with fences. This is the bug class that `krusty-ring`'s verification
  (V4/V5) targets.
- For an Aeron baseline benchmark, use Aeron's own benchmarks
  (aeron-io/benchmarks). For any future Aeron bridge, prefer bindings to
  Aeron's official C client over a port.
