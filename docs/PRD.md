# KrustyCast: Product Requirements

Status: **draft v0.2** (2026-11-14). Companion documents:
[Architecture](ARCHITECTURE.md), [Gap analysis](GAP-ANALYSIS.md).

## 1. Summary

KrustyCast is a Rust-native, replicated, totally ordered message
sequencer for trading and exchange-style systems. Clients submit messages,
and a 3- or 5-node cluster puts them in one order that survives failures.
The ordered stream is published on KrustyCast's native stream protocol.
Deterministic applications (matching engines, risk, position keeping,
replicas) consume it through the SDK and get identical state. The same
sequence can also be published as a standard MoldUDP64 feed for external
consumers.

The product makes two claims and has to back both:

1. **Correct**: its guarantees are written down as a formal
   specification. They are model-checked, and in the core they are proven
   against the code.
2. **Fast**: latency and jitter targets are stated up front. They are
   measured with a reproducible benchmark and compared on the same hardware
   with the state of the art (Aeron).

We do not aim to replicate Aeron. We aim to cover a well-defined subset of
its use cases (sequencing plus replicated log plus replay) credibly.

## 2. Problem

Teams building venues and sequenced trading systems need a total-order log
that is fast, highly available and trustworthy. Today they choose between:

- **Aeron Cluster / Aeron Sequencer.** Proven and fast. JVM-centric, with a
  C media driver. Kernel-bypass and encryption are proprietary add-ons.
  Rust is not an official client language. Correctness rests on testing and
  production use, not machine-checked proofs.
- **In-house sequencers.** Expensive to build. Rarely specified formally.
  Correctness is folklore.
- **General-purpose logs (Kafka and similar) and Raft libraries.** Built
  for throughput or general consistency, not microsecond latency and low
  jitter.

**Hypothesis.** A Rust implementation built around a deterministic,
verifiable core can match or beat JVM-based systems at the tail (p99 and
above) while using less memory. It can also offer stronger, checkable
correctness guarantees.

The performance half is a hypothesis, not a given. Aeron is already
allocation-free in steady state and has a C driver. The advantages we
expect from Rust are:

- no JIT warmup
- no GC safepoints
- control over memory layout
- a smaller footprint
- access to Rust verification tools

These must be shown with published benchmarks (§10).

## 3. Users and use cases

**Primary users**
- Engineers building a venue core: crypto exchanges, ATSs, prediction
  markets, internal crossing engines.
- Trading-firm platform teams building sequenced architectures for OMS,
  risk and position services.

**In scope (v1)**

| ID | Use case | Description |
|---|---|---|
| UC1 | Venue core sequencing | Gateways submit orders. KrustyCast orders them. Matching engines consume the stream. Their outputs (fills, acks) are submitted back and sequenced too. |
| UC2 | Deterministic replicated services | Several replicas of a service consume the same stream and stay identical: active/active, or a hot standby. |
| UC3 | Recovery and audit | Any consumer can replay a session from its start, or from a position, and get exactly the same messages. The session log is the audit trail. |
| UC4 | Standard feed distribution | The ordered stream is also available as a MoldUDP64 feed with a standard re-request service, so off-the-shelf Mold receivers can consume it. |
| UC5 | Conditional append | A message can carry "key K is at version V". The sequencer accepts it only if that's still true. At most one writer wins each version. This lets clients coordinate without locks. |

**Non-goals (v1)**

| Non-goal | Why |
|---|---|
| General pub/sub with many streams (an Aeron Transport replacement) | Different product; huge surface. |
| Hosting application code inside the cluster (Aeron Cluster's `ClusteredService` model) | Applications are external deterministic consumers (ADR-2 in the architecture doc). This keeps the verified core small. |
| Synchronous geo-replication, Byzantine fault tolerance | Different latency and threat models. |
| Wire encryption | P2. v1 assumes an isolated network. |
| Non-Linux platforms; non-Rust clients | P2: a C ABI for the client SDK. |

## 4. Principles

1. **Correct first, then fast; measure always.** No performance claim
   without a reproducible benchmark. No correctness claim without a spec.
2. **Deterministic core.** All protocol logic is a pure state machine. Time,
   randomness and I/O are inputs. This is what makes verification,
   simulation and replay possible.
3. **Nothing unbounded.** Every queue, table and buffer has a configured
   limit. Overload produces explicit back pressure, never unbounded growth.
4. **Publish only what's committed.** Nothing a client or consumer can see
   is ever retracted.
5. **Native protocols inside, standards as adapters.** Our own documented,
   versioned protocols where we control both ends; MoldUDP64 as an adapter
   for external consumers.
6. **Verify the core, test the shell.** Formal methods go where bugs are
   subtle and expensive. Simulation and fuzzing cover the rest.

## 5. Functional requirements

Priority: **P0** = required for v1, **P1** = soon after v1, **P2** = later.

| ID | Requirement | P |
|---|---|---|
| FR-1 | **Ingress sessions.** Clients submit over UDP to the cluster leader. Each message is identified by (client ID, incarnation, client seqno). Resends are idempotent and return the original outcome. Gaps are NAKed. | P0 |
| FR-2 | **Total order.** Every accepted message gets a global seqno. Seqnos are contiguous and start at 1 in each session. | P0 |
| FR-3 | **Append modes.** *Unconditional* append, or *conditional* append on a key with an expected version (UC5). | P0 |
| FR-4 | **Outcomes.** Each message's outcome is reported to its client: ACCEPTED (with global seqno), REJECTED (with the key's current version), BUSY (back pressure, with a retry hint) or NOT_LEADER (with the leader's address). | P0 |
| FR-5 | **Replication and failover.** 3- or 5-node clusters. Automatic leader election. Committed data survives the failure of up to f nodes, where n = 2f+1. | P0 |
| FR-6 | **Native stream egress.** Multicast stream of accepted messages, each with a deterministic cluster timestamp. Heartbeats, end-of-session, and an explicit UNAVAILABLE answer for data that has aged out. The cluster never waits for slow consumers. | P0 |
| FR-6a | **MoldUDP64 adapter.** The same sequence (same global seqnos) as a standard MoldUDP64 feed with a standard re-request server, off the native stream's latency path. Priority depends on the first market (§13). | P0 / P1 |
| FR-6b | **Unicast egress** for networks without multicast (cloud): consumers subscribe to a node, preferably a follower, which fans out to them. | P1 |
| FR-7 | **Recovery service** on every node, serving any seqno in the retained sessions from memory or disk. Requests are delayed and suppressed so that many consumers missing the same data don't cause a request storm. | P0 |
| FR-8 | **Durability modes.** (a) Committed when a quorum holds the entry in memory, fsync asynchronous. (b) Committed when a quorum has fsynced it. | P0 |
| FR-9 | **Snapshots** of sequencer state (dedup tables, key versions) for fast node recovery. Log retention per session. | P1 |
| FR-10 | **Consumer SDK.** In-order delivery, gap recovery, replay from a position, a "deterministic consumer" API for building replicas. | P0 |
| FR-10b | Application snapshot helpers in the SDK (store and load app state at a stream position). | P1 |
| FR-11 | **Session lifecycle.** Start, end-of-session, roll to a new session (e.g. daily). | P0 |
| FR-12 | **Membership.** Static in v1. Dynamic add/remove in P2. | P0 / P2 |
| FR-13 | **Operations.** Config file, live counters, CLI tools (status, counters, log inspector, snapshot trigger). | P0 |
| FR-14 | **Client authentication** (pre-shared-key MAC). TLS-class encryption is P2. | P1 |
| FR-15 | **Protocol versioning** on every wire format (P0). Rolling upgrades (P1). | P0 / P1 |

## 6. Correctness guarantees (the contract)

**Failure model.**
- Nodes crash and restart (crash-recover).
- The network may lose, duplicate, reorder and delay packets, and may
  partition.
- No Byzantine behavior.
- Clocks are never needed for safety, only for liveness (timeouts).

| ID | Guarantee |
|---|---|
| G1 | **Agreement.** Any two consumers that deliver global seqno *n* deliver the same message. |
| G2 | **Gap-free prefix delivery.** A consumer delivers *n* only after 1..*n*−1, unless it explicitly reports loss. Loss is possible only for data outside retention, or, if the consumer chose to give up on gaps no node fills (gap policy `GiveUpAfter(n)`, the SDK default), after n unanswered recovery requests. With the `Wait` policy the consumer stalls instead. |
| G3 | **Durability.** No outcome reported to a client, and no message published on egress, is ever lost or changed while at most f nodes fail. In mode (a) this also assumes that not every node loses power at once. |
| G4 | **At-most-once sequencing** per (client, incarnation, seqno). With client resends this gives exactly-once for any client that stays alive until it gets its outcome. |
| G5 | **Conditional append.** At most one message is accepted per (key, version), and accepted versions are contiguous. |
| G6 | **Per-session FIFO.** One client session's messages are sequenced in seqno order. A rejected-then-retried conditional message is a *new* message and may land after later ones. This is documented behavior. |
| G7 | **Determinism.** The egress stream is a pure function of the committed log. Replaying the log reproduces it byte for byte. |
| G8 | **Liveness.** If a majority of nodes are up and connected and the network eventually stabilizes, a leader is elected and the cluster makes progress. |
| G9 | **Bounded resources.** Memory stays within configured limits. Overload produces BUSY, never out-of-memory. |

G1–G7 and G9 are safety properties. Each must appear as an invariant in the
formal specification (§9) and as an executable checker in simulation
testing.

## 7. Performance requirements

**Reference setup.**
- 3 nodes on one switch, 25 GbE NICs.
- Current-generation x86 servers, Linux tuned with isolated cores and
  busy-polling.
- 64-byte payloads, durability mode (a).
- Open-loop load at 50% of the measured maximum sustained throughput.
  Latency is corrected for coordinated omission (the load generator never
  waits on responses, so stalls aren't hidden).

**Primary metric: submit-to-feed round trip.** A client submits a message
and receives that same message on the native stream. This is measured on one
host's clock, so no cross-host clock sync is needed.

| ID | Metric | Tier A (kernel UDP) P0 | Tier B (kernel bypass) P1 |
|---|---|---|---|
| NFR-1 | Submit-to-feed round trip p50 | ≤ 40 µs | ≤ 15 µs |
| NFR-2 | Round trip p99 | ≤ 80 µs | ≤ 30 µs |
| NFR-3 | Round trip p99.99 | ≤ 200 µs | ≤ 60 µs |
| NFR-4 | Maximum over a 10-minute run | ≤ 1 ms | ≤ 250 µs |
| NFR-5 | Sustained throughput (64 B) | ≥ 1 M msg/s | ≥ 3 M msg/s |
| NFR-6 | Failover: leader crash to first new message on the feed, p99 | ≤ 200 ms | ≤ 200 ms |
| NFR-7 | Heap allocation on the hot path after warmup | zero (enforced in CI) | zero |
| NFR-8 | Resident memory | ≤ configured budget + 10% | same |
| NFR-9 | Mode (b) round trip p99 | ≤ 2 × device fsync p99 + 30 µs, with group commit (relaxed 2026-11-15, open question 7) | same |
| NFR-10 | Node restart: start to serving (latest snapshot + replay of the log suffix), snapshot every 1 M entries | ≤ 1 s | same |

These targets come from a hop budget (architecture §7): four network hops
plus processing. **They are provisional.** Milestone M0 measures baselines
on real hardware, after which the targets are confirmed or revised. That
review is a required step, not an option.

## 8. Operability requirements

- One static binary per node, configured by one config file.
- Live counters readable without stopping the process (shared-memory
  counters file plus CLI).
- Latency histograms per pipeline stage.
- Session roll, snapshot and controlled leader handoff from the CLI.
- Every failure is diagnosable from counters plus a structured event log.
  No formatted logging on the hot path.

## 9. Verification and assurance requirements

| ID | Requirement | P |
|---|---|---|
| V1 | **Protocol specification** in TLA+ (or Quint) covering ingress sessions, the sequencer state machine, replication (Raft) and egress, with G1–G7 as invariants. Model-checked in CI for small configurations. | P0 |
| V2 | **Proven sequencer core.** The sequencer step function is translated to Lean with Aeneas and proven to implement the spec's actions and preserve G4, G5 and G7. | P0 |
| V2b | Proven consumer stream logic (in-order delivery, G2) and codec properties (decode never panics; decode inverts encode). | P1 |
| V3 | **Replication conformance.** Traces from simulation runs are checked against the TLA+ Raft spec ("trace validation"). A full code-level proof of the Raft implementation is a P2 research goal. | P0 |
| V4 | **Concurrency primitives** (ring buffers, buffer pool) verified in Verus, or at minimum exhaustively checked with loom and Kani. | P1 |
| V5 | **Unsafe code** confined to designated modules. Every unsafe block is checked with Miri and Kani. | P0 |
| V6 | **Deterministic simulation testing.** The whole cluster runs under a seeded simulator with network, disk and clock faults. Every failure reproduces from its seed. | P0 |
| V7 | **Fuzzing** of every decoder and of the ingress path. | P0 |
| V8 | **Real-network fault testing** (Jepsen-style) before each release. | P0 |

The trusted computing base must be documented: what is assumed rather
than verified (compiler, OS, NIC, translation tools, the spec itself).

## 10. Success metrics and v1 release criteria

1. All P0 functional requirements.
2. Every P0 metric in §7 met on reference hardware, with the benchmark
   reproducible from the repository by a third party.
3. A published comparison with Aeron Cluster on identical hardware and
   workload. Target: equal or better at p99 and above, and lower memory.
4. V1, V2, V3, V5, V6, V7 and V8 complete. Nightly simulation finds zero
   invariant violations for 30 consecutive nights at ≥ 100 M simulated
   operations per night.
5. One design-partner pilot running a realistic workload.

## 11. Milestones

Detailed in the [gap analysis](GAP-ANALYSIS.md#5-roadmap). In outline:

| Milestone | Contents |
|---|---|
| M0 | Baselines and tool spikes |
| M1 | Single-node performance core |
| M2 | Storage and consumer SDK |
| M3 | Replication and failover |
| M4 | Hardening and release |

Verification runs alongside from M0.

## 12. Risks

| Risk | Mitigation |
|---|---|
| Verification tools are young. Charon describes itself as beta; Aeneas covers a subset of safe Rust; Verus is under active development. | Keep the proven core in a deliberately simple Rust subset. Pin toolchains. Run proofs in CI but initially non-blocking. TLA+ plus simulation stays the safety net. |
| Performance hypothesis fails | M0 measures before we commit. Targets are reviewed after M0. |
| Writing our own Raft | Spec first. Model checking, trace validation and simulation with faults. Reuse public Raft test suites as oracles. |
| Scope creep toward "Aeron in Rust" | Non-goals in §3. Every new feature has to name the use case it serves. |
| Lean/Verus expertise is scarce | Budget for training or a specialist hire. Keep proofs to the core. |
| Kernel-bypass vendor lock-in | Transport trait. Tier A must stand on its own; Tier B is an accelerator. |

## 13. Open questions

1. Which beachhead first: crypto venues (cloud-heavy, so unicast egress
   becomes P0) or traditional ATS / colo (multicast, where ITCH-style
   MoldUDP64 feeds are expected, so the Mold adapter becomes P0)?
2. Maximum message size. One MTU, or fragmentation and reassembly?
3. How many clients and keys per cluster? This sizes the dedup and version
   tables.
4. Is one total order per cluster enough, or do users need several
   independent streams (sharding) in v1?
5. License and business model: open-source core with proprietary
   accelerators, or something else?
6. Should 5-node clusters be supported at launch?
7. ~~NFR-9~~ **Decided 2026-11-15: relaxed** to ≤ 2 × device fsync p99 +
   30 µs. With one group commit at a time, a message that arrives just
   after an fsync started waits for it and then for its own, so mode (b)
   costs one to two fsyncs (p99 was 2.1–2.6 ms against a device fsync p99
   of about 1 ms on a consumer NVMe drive, docs/BASELINE.md). No drive
   requirement. In a cluster, mode (b) commits once a majority has
   fsynced; leader and followers fsync in parallel.
