# KrustyCast: Gap Analysis (current code → v1)

Status: **draft v0.2** (2026-11-14). Measures the code on branch
`moldudp64-downstream` against the [PRD](PRD.md) and the
[architecture](ARCHITECTURE.md). §1–§4 describe that starting point, the
single-crate prototype. It was replaced crate by crate and removed on
2026-11-16 (in the git history up to 131dc5e). The rows in §3 and the
roadmap in §5 track progress since.

## 1. Summary

Today KrustyCast is a **single-node prototype**: about 3.2k lines of Rust
in one crate, including 442 lines of end-to-end tests. Its *semantics* are
largely right, and several are already what v1 needs:

- total order
- idempotent client resends with incarnations
- conditional append (per-target compare-and-swap)
- a standard MoldUDP64 feed with re-requests (becoming an optional
  adapter, architecture ADR-11)
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
| MoldUDP64 codec, re-request server and interop tests | `protocol.rs`, `sequencer.rs`, `tests/e2e.rs` | Becomes the `krusty-mold` adapter (ADR-11), largely as is. Keep the `moldudp` interop tests. |
| Stream recovery logic | `stream.rs` | Its gap, retry and give-up logic carries over to the native stream's tracker. |
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
| A1 | **Done 2026-11-16:** every core is sans-I/O behind thin shells; the prototype is removed. Was: logic mixed with I/O. The client sends on the socket while holding its state locks (`client.rs:267`, `:284`). The sequencer core writes the journal inside `process_batch` (`sequencer.rs:279`). | Pure sans-I/O cores plus thin I/O shells (architecture §5) | C, P | L | M1 |
| A2 | **Started 2026-11-14:** workspace with `krusty-wire`, `-core`, `-session`, `-stream`, `-sim`, `-ring`, `-net`, `-node`; the old crate remains until the node replaces it. One crate with mixed concerns | Crate layout in architecture §4; `forbid(unsafe_code)` in pure crates | O, C | M | M1 |
| A3 | **Done in `krusty-node` 2026-11-14:** explicit stage lifecycle (stop → END_OF_SESSION → egress drains). Ad-hoc shutdown: `AtomicBool` flags plus 20–100 ms read timeouts | Staged runtime with explicit stage lifecycle (`krusty-rt`) | P, O | M | M1 |

### 3.2 Replication and availability

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| R1 | **Done 2026-11-15** (`krusty-node` cluster mode, durability modes (a) and (b); in mode (b) followers acknowledge only fsynced entries, checked in `krusty-sim` with whole-cluster power losses): 3-node Raft with automatic failover; on a laptop over loopback, new messages reach the feed 70 ms (p50) / 171 ms (max of 100) after the leader stops (BASELINE.md). InstallSnapshot and membership changes are not done. Single sequencer process; recovery is a manual restart | Raft cluster with automatic failover (FR-5, NFR-6) | C, O | **XL** | M3 |
| R2 | **Done 2026-11-15**, in simulation (`krusty-sim::cluster`) and in the node (`krusty-node/src/cluster.rs`): the replicated log holds raw inputs with the leader's timestamp; every node re-derives decisions. The journal records *decisions* and lives on one disk | Log of raw inputs, replicated; the state machine re-derives decisions on every node (ADR-4) | C | L | M3 |
| R3 | **Done 2026-11-15:** `[cluster] members`; followers answer `NOT_LEADER` naming the leader; `krusty-client` goes to the named leader, or rotates after silence. Clients target one fixed sequencer address | Member list, `NOT_LEADER` redirect, client rotation | C | M | M3 |
| R4 | **Done 2026-11-14:** snapshots every N entries; restart = latest snapshot + log suffix replay. No snapshots: restart replays the whole journal | Snapshots of core state plus log suffix replay (FR-9) | O | M | M2 |

### 3.3 Hot path performance

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| P1 | **Done in `krusty-node` 2026-11-14:** byte rings between stages; BUSY from ingress when full. Unbounded crossbeam channels (`sequencer.rs:401`, `client.rs:210`) | Bounded SPSC rings; BUSY when full (G9) | C, P | M | M1 |
| P2 | **Done in `krusty-node` 2026-11-14:** single writer per state; the history is read by recovery through per-slot seqlocks. Mutexes on the hot path: `Arc<Mutex<History>>` (`sequencer.rs:148`, locked per batch); client state in three mutexes (`client.rs:102–104`) | Single writer per state; history readable by the re-request thread through a lock-free published index | P | M | M1 |
| P3 | **Done in `krusty-node` 2026-11-14:** zero-copy views, preallocated buffers; a test fails on any allocation in the stage threads after warm-up. Allocation per message: decoders copy payloads (`protocol.rs:102`, `:304`, `:423`); history stores a `Vec<u8>` per message; encoders allocate | Zero-copy views (`zerocopy`), preallocated slots and slabs; zero allocations enforced in CI (NFR-7) | P | M | M1 |
| P4 | **Done in `krusty-node` 2026-11-14:** `recvmmsg`/`sendmmsg`, pinned spinning stages (`--spin`). One `recv_from`/`send_to` syscall per datagram; blocking reads with timeouts (`client.rs:122`, `:191`) | `recvmmsg`/`sendmmsg`, busy-poll, pinned threads, spin/back-off strategies | P | M | M1 |
| P5 | **Done 2026-11-14:** storage on its own stage; mode (a) publishes without waiting, mode (b) after the batch's fsync (group commit). Synchronous fsync on the sequencing thread: `j.commit()` blocks sequencing on every batch | Storage on its own thread (io_uring); durability mode (a) takes fsync off the latency path | P | M | M2 |
| P6 | No kernel-bypass path | Transport trait; Onload compatibility; AF_XDP (Tier B) | P | L | M4 |
| P7 | **Done in `krusty-core` 2026-11-14.** `std::collections::HashMap` for sender and target state (`sequencer.rs:144`) | Fixed-capacity tables: predictable and verifiable | P, C | M | M1 |

### 3.4 Protocol

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| W1 | **Done in `krusty-wire` 2026-11-14** (not yet used by the node). No magic or version field on any internal format | Versioned formats (FR-15) | O | S | M1 |
| W2 | **Done in `krusty-core` 2026-11-14.** Every message is a conditional claim (`target_id`, `target_seq`); no unconditional append | Two append modes (FR-3) | C | S | M1 |
| W3 | **BUSY and SESSION_EXPIRED done in `krusty-core` 2026-11-14 (NOT_LEADER comes with Raft).** No BUSY, NOT_LEADER or SESSION_EXPIRED feedback | Full outcome set (architecture §8.1) | C | S | M1 |
| W4 | **Done in `krusty-core` 2026-11-14 (ADR-12, proposed).** Dedup tables grow without limit: one entry per incarnation, forever; outcome ring of 4096 per sender (`sequencer.rs:25`) | Bounded, replicated, deterministic eviction | C | M | M1 |
| W5 | **Done 2026-11-14:** `max_payload` enforced at ingress, sized so one message fits a frame (fragmentation not planned for v1). Messages limited to one datagram, with no explicit maximum | Explicit maximum size (PRD open question 2); optional fragmentation | C | S–M | M1 |
| W6 | **Done in `krusty-wire` 2026-11-14** (not yet used by the node). Big-endian hand-written upstream codecs | Little-endian fixed layouts (ADR-8); Kani-checked decoders | P, C | M | M1 |
| W7 | **Done 2026-11-14:** native stream from `krusty-node`; MoldUDP64 is an optional adapter stage (`krusty-mold`, same seqnos, standard re-requests). MoldUDP64 is the only egress: no back pressure policy, no UNAVAILABLE answer, no cluster timestamp, multicast only | Native stream protocol (architecture §8.3) as the backbone; MoldUDP64 moved behind the adapter (§8.4) | C, P | M | M1 |
| W8 | Seqno arithmetic on wire values overflows near `u64::MAX` (`p.seqno + i`, `next += 1`, `seqno + count` in `stream.rs`): panic in debug, wrap in release. Found by the Aeneas spike. **Fixed 2026-11-14:** `decode` rejects such packets; the tracker ignores them (`Outcome::Invalid`) | Range-validate at decode (`seqno + count ≤ u64::MAX`) or checked arithmetic; no-overflow preconditions in proofs | C | S | M1 |
| W9 | **Done in `krusty-stream` 2026-11-14** (not yet used by the node). Consumer reorder buffer is an unbounded `BTreeMap` (`stream.rs`): a hostile or buggy sender grows memory without limit | Fixed reorder window; messages beyond it recovered by re-request (prototype in `spikes/aeneas-stream/subset`) | C, P | S | M1 |

### 3.5 Storage

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| S1 | **Done 2026-11-14** (`krusty-log`): preallocated segments, CRC32C, 8-byte alignment, torn-tail cut; `pwrite`/`fdatasync` rather than io_uring so far. One growing journal file through `BufWriter`, CRC32 | Preallocated segments, CRC32C, io_uring, alignment | P, O | L | M2 |
| S2 | **Done 2026-11-14:** RECOVER served from the in-memory history, else from disk through the egress index. Re-requests served only from in-memory history (last 1 M messages) | Served from segments via index and mmap for every retained session (FR-7) | C | M | M2 |
| S3 | **Done 2026-11-14:** egress (seqno) index; retention by whole segments below the latest snapshot (one stream per log, so per session). No indices, retention or purge | Seqno and log-index indices; per-session retention | O | M | M2 |

### 3.6 Client and consumer SDK

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| K1 | **Done 2026-11-14:** `krusty-client` delays a first RECOVER randomly and rotates nodes, cancels it if a retransmission arrives; the node answers on the feed once when several consumers ask for the same range. Per-consumer rate limits still to do. Re-requests sent immediately to one server address (storm risk) | Delayed, randomized recovery spread across nodes, with multicast suppression (architecture §8.5) | P | M | M2 |
| K2 | **Done 2026-11-14:** `krusty-client::Consumer` delivers seqno, leader timestamp, client and payload (borrowed from the receive buffer when in order); replay from any position, recovered from disk. Callback API `Fn(Vec<u8>)`: copies, runs under a mutex, no position | Zero-copy delivery with seqno and position; replay-from-position; deterministic-consumer API (FR-10) | P | M | M2 |
| K3 | **Done 2026-11-14:** `krusty-client::Publisher` reports Accepted/Rejected/Failed per message and restarts on SESSION_EXPIRED. **Core done in `krusty-session` 2026-11-14:** every message ends in exactly one Accepted, Rejected or Failed event. Abandoned sends only counted and logged (`failed()`) | Explicit failure delivered to the application | C | S | M2 |
| K4 | **Done in `krusty-session` 2026-11-14** (not yet used by the SDK shell). Client logic lives in the I/O shell | `krusty-session` sans-I/O core | C | M | M1 |
| K5 | Multicast-only egress | Unicast fan-out on the native stream (FR-6b) | O | M | M4 |

### 3.7 Assurance

IDs here are gap IDs; the PRD column names the assurance requirement each one closes. PRD V5 (unsafe code confined and checked) has no gap yet: the code has no `unsafe` today.

| ID | PRD | Current | Target | Why | Size | M |
|---|---|---|---|---|---|---|
| Q1 | V1 | **Done 2026-11-16:** `Sequencer.tla`/`SequencerBounded.tla` cover sessions and the sequencer over any single log (G4–G6, outcome consistency); `RaftKC.tla` covers Raft as implemented (one committed log that never changes: G3's core, G7's input); `Egress.tla` covers the native stream and consumers over a lossy network with concurrent publishers (G1, G2); composition argument in spec/README.md; all checked in CI. The consumer's gap policy (stall, or report loss after n unanswered recoveries) is configurable and both settings are checked; PRD G2 says which loss each allows. Not modelled: liveness (G8), power loss in mode (a) 2026-11-19: `Cluster.tla`, the cluster shell over abstracted Raft, with liveness (every message gets an outcome) and a bound on resend copies; it found two bugs in the new resend filter (spec/README.md). `RaftKC` checks `LeaderAppendOnly`. 2026-11-20: the shell is trace-validated against `Cluster.tla` nightly (`TraceCluster.tla`). | TLA+/Quint spec of sessions, sequencer, Raft and egress, with G1–G7 | C | L | M0–M3 |
| Q2 | V2 | **Done 2026-11-16:** `krusty-core` translates to Lean (`spec/lean`, `regen.sh`); `Core::apply` is proved to refine an abstract sequencer spec under an invariant that `Core::new` establishes, with G4 and G5 proved on the spec and G4 carried to the code (spec/README.md). The table (`IdMap`) is proved a finite map. CI re-checks everything and fails if the Lean is stale. Snapshot restore is not covered | Aeneas→Lean proof of `krusty-core::step` (refinement plus invariants) | C | XL | M1–M3 |
| Q3 | V3 | **Done 2026-11-15:** `spec/tla/TraceRaftKC.tla` replays traces of the real code (randomized harness with faults) with RaftKC's actions; all traces accepted, and five seeded Raft bugs are each rejected (spec/README.md). CI checks 3 traces, nightly 20 | Trace validation of `krusty-raft` against the spec | C | L | M3 |
| Q4 | V4 | **Done 2026-11-14:** Verus-verified batched SPSC ring of `Copy` values (memory safety, data-race freedom; FIFO tested, not proved), 1.3× the cost of an Acquire/Release twin; not yet used by the node (its rings carry variable-length records). — | Verus-verified SPSC ring (or loom + Kani) | C | L | M2 |
| Q5 | V6 | **2026-11-15:** cluster mode (3 and 5 nodes, partitions, crash-restarts, failover), nightly. **Started 2026-11-14:** `krusty-sim` drives clients, core and consumers through the real codecs over a faulty network; G1, G2, G4–G6, exactly-once and liveness checkers; 5 scenarios; nightly sweep of 100 k runs. No simulation | `krusty-sim`: seeded network, disk and clock faults; G1–G9 checkers; nightly runs | C | L | M1→ |
| Q6 | V7 | **Started 2026-11-14:** `cargo-fuzz` target for every `krusty-wire` decoder (canonical re-encoding asserted), 60 s per CI run. No fuzzing | `cargo-fuzz` on all decoders and on ingress | C | S | M1 |
| Q7 | — | **Started 2026-11-14:** fmt, clippy, tests, TLC model checking and Lean proof re-check in GitHub Actions | CI: build, tests, Kani/Miri, model checking, proof re-check, alloc guard, perf smoke test | O | M | M0 |
| Q8 | V8 | Planned: fault tests over an emulated network (e.g. containernet) before the reference servers | Jepsen-style real-network fault testing | C | M | M4 |
| Q9 | — | **Planned 2026-11-22** (`spec/RAFT-VERIFICATION.md`): machine-checked safety of `krusty-raft` as built. Protocol: refinement to Ongaro's `raft.tla` and a feature table against etcd's and CCF's specs, then the inductive invariant (Apalache, then TLAPS or Lean), durability modes in the model, liveness with pre-vote, check-quorum and transfer. Code: trace validation of the real node under fault injection, then a per-node refinement proof of `krusty-raft` in Lean (Aeneas), as for `krusty-core`. Found while planning: in mode (a) one node's power loss breaks Raft safety (`RaftKCPower.tla`: two leaders in a term; with the hard state fsynced, a leader without a committed entry), against PRD G3; fixed 2026-11-23: cautious restart after a power loss, model-checked first (`RaftKCRecover.tla`), then implemented and tested (spec/RAFT-VERIFICATION.md). 2026-11-23: real `krusty-node` processes under faults (`krusty-node/tests/processes.rs`: a fault proxy on peer links, pauses, kills, power losses), with end-to-end checks and the shell traces against Cluster.tla; nightly. With the fix taken out it loses committed data. It also found that in mode (a) a killed process lost entries it had acknowledged (still in the storage ring): two kills lost published messages. Fixed: write before acknowledging, with fsyncs and snapshots on their own thread (E3 re-measured: BASELINE "matched durability"). Still to do: Raft traces from the node against RaftKC | Raft implementation proved against its protocol model | C | XL | M4→ |

### 3.8 Performance evidence

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| E1 | **Done:** `krusty-load` (open loop, HDR histograms, CO-correct latency, single node and in-process cluster) replaced the prototype's `loadgen` (2026-11-14); results in `docs/BASELINE.md` | Open-loop load generator; coordinated-omission-correct histograms; submit-to-feed round trip (PRD §7) | P | M | M0 |
| E2 | No reference hardware. Found on the laptop: multicast via the default route went over Wi-Fi and capped the node near 5–10 k msg/s | Defined reference setup; tuning guide (isolated cores, IRQ affinity, busy-poll) | P | S | M0 |
| E3 | **Done on nuc2 2026-11-17, current results 2026-11-23** (BASELINE.md E3, "Current results: matched durability"; reference servers still to do): Aeron Cluster 1.54 and KrustyCast, three nodes each, under one Rust load generator (`bench/rig`; no JVM on either client side), same CPUs, one session, and since 2026-11-23 matched durability (both acknowledge what is written to the log file). Medians are close (on tmpfs p50 41 against 47 µs at 100k, 53 against 51 at 300k, 65 against 59 at 500k); the tail is far shorter on tmpfs (max 0.21-0.49 ms against Aeron's 3.4-4.7 ms, its JVM safepoints) and shorter on disk (p99.9 3-4 against 7-9 ms). The 2026-11-22 numbers (KrustyCast ahead at every percentile) measured a weaker mode (a) that acknowledged entries still in memory. It sustains 750k msg/s, where Aeron falls behind at about 560-580k. It took batching (several messages per datagram), 2 Appends in flight instead of 16, and O(1) session work per message. The comparison found and fixed five cluster bugs (fsync per 128 entries, rollover stalls, stale Raft clock, Raft stopping under overload, resends logged again). Was: no comparison | Aeron Cluster baseline on the same hardware and workload | P | M | M0 / M4 |

### 3.9 Operations and security

| ID | Current | Target | Why | Size | M |
|---|---|---|---|---|---|
| O1 | **Config file done 2026-11-14** (`krusty-node --config`, `config/node.example.toml`; unknown keys rejected, values validated); static membership comes with Raft. CLI flags only | TOML config; static membership | O | S | M1 |
| O2 | **Started 2026-11-14:** shared-memory counters and per-stage latency histograms (`/dev/shm/krusty-<stream>.stats`), read live by `krusty-stat`; the binary event ring is still to do. `println!`/`eprintln!` logging | Shared-memory counters, per-stage histograms, binary event ring, `krusty-stat` | O, P | M | M1 |
| O3 | **Done 2026-11-16:** per-node admin endpoint and `krusty-admin`: status, snapshot on demand, leadership transfer (Raft TimeoutNow; validated against RaftKC traces), session roll (new stream, fresh core, replicated; USER-GUIDE §7). Was: no cluster tooling | Status, snapshot, session roll, leader handoff from the CLI | O | M | M4 |
| O4 | No authentication | Pre-shared-key MAC on ingress and replication (FR-14) | C | M | P1 |
| D1 | **Done 2026-11-16:** [USER-GUIDE.md](USER-GUIDE.md). Was: no user documentation beyond README and design docs | User guide: concepts and guarantees, deploying a node and a cluster, configuration reference, tuning (isolated cores, IRQ affinity), SDK usage (publisher, consumer, gap policy), operations (metrics, restarts, retention) | O | M | M4 |

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
    **Done 2026-11-14** (`spikes/aeneas-stream/REPORT.md`): a restricted
    rewrite translates and the first loop theorem is proved.
  - Verus on a small SPSC ring: how does it build and ship with cargo?
    **Done 2026-11-14** (`spikes/verus-ring/REPORT.md`): builds and ships
    with plain cargo; SeqCst-only atomics require a batched ring.
- *Exit:*
  - Baseline numbers published.
  - PRD §7 targets confirmed or revised.
  - Coding rules for the verified subset confirmed (architecture §11.3).

**M1: Single-node performance core.**
- Gaps A1–A3, P1–P4, P7, W1–W7, K4, Q5 (single-node simulation), Q6, O1, O2.
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
  - Mode (b) target met. (Met 2026-11-15 on a consumer NVMe, after group commit went from two flushes to one; BASELINE M2.)

**M3: Replication and failover.**
- Gaps R1–R3, Q3; Q1 complete, Q2 complete.
- *Exit:*
  - 3-node cluster meets NFR-1–6. (NFR-1–4 and 6 met 2026-11-16 on one
    8-core machine over loopback with isolated cores: 10-minute runs at
    75 000 msg/s, p50 25 µs, p99 38 µs, p99.99 66 µs, max 450 µs;
    failover max 164 ms of 100. NFR-5 is not: 150 000 msg/s maximum,
    one ingress thread; BASELINE M3.)
  - Model checking and trace validation are green.
  - Nightly simulation with faults runs clean. (Run 43841393191 on
    2026-11-16, at 1442d7d.)
  - The core proof is checked in CI. (Met 2026-11-16: refinement of
    `Core::apply`, G4 and G5; spec/README.md.)

**M4: Hardening and release.**
- Gaps P6 (Tier B), K5, O3, Q8, E3 (final comparison), D1 (user guide),
  O4 (if pulled forward).
- Carried from M3: NFR-5 (1 M msg/s). (A proof that snapshot restore
  yields the core invariant: done 2026-11-19, `spec/lean/KrustyCore/Restore.lean`.) E3 showed the cluster sustains 250 k msg/s
  in mode (a) with two client threads saturated: the next step to NFR-5
  is batching submits and feedback (several per datagram), then more
  ingress capacity or Tier B.
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
