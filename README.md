# KrustyCast

A replicated, low-latency message sequencer in Rust. Clients submit
messages to a cluster. The cluster puts all of them in one total order and
publishes them as a multicast stream. Consumers follow the stream and
recover anything they miss.

```
publishers ──UDP──▶ leader ──Raft──▶ followers (3 or 5 nodes)
    ▲                 │
    └── outcomes ─────┤
                      ├──▶ native stream (multicast) ──▶ consumers
                      └──▶ MoldUDP64 stream (optional)      │
         recovery (retransmit, from memory or disk) ◀───────┘
```

## What it guarantees

From [the PRD](docs/PRD.md) §5:

- **Agreement and gap-free delivery (G1, G2).** Every consumer sees the
  same message at each seqno, in order. Loss is reported explicitly and is
  possible only beyond retention.
- **Durability (G3).** Nothing reported to a client or published is lost
  while at most f nodes fail. Mode (a) acknowledges before fsync and assumes
  not every node loses power at once. Mode (b) publishes only after a
  quorum has fsynced.
- **At-most-once sequencing (G4).** One global seqno per (client,
  incarnation, client seqno). With the SDK's resends this gives
  exactly-once.
- **Conditional append (G5).** A message can claim the next version of a
  key. The first claim wins, and losers get REJECTED with the current
  version.
- **Per-session FIFO (G6), determinism (G7), liveness (G8), bounded
  memory (G9).** Overload produces BUSY, never out-of-memory.

## Status

Milestones M0–M3 of the [roadmap](docs/GAP-ANALYSIS.md) are done, except
for one throughput target. The current state:

- **Replication.** A 3-node Raft cluster with automatic failover. After the
  leader crashes, new messages reach the feed in 70 ms at p50 (worst case
  164 ms over 100 trials).
- **Latency on one 8-core machine** (loopback, cores isolated, 3 nodes,
  durability (a), 75 000 msg/s for 10 minutes):

  | p50 | p99 | p99.99 | max |
  |---|---|---|---|
  | 25 µs | 38 µs | 66 µs | 450 µs |

  Mode (b) on a consumer NVMe: p99 below twice the device's fsync p99.
- **Throughput.** About 150 000 msg/s per cluster on that machine (one
  ingress thread). The 1 M msg/s target (NFR-5) is M4 work: several
  ingress threads or kernel bypass.
- Numbers, methods and caveats are in [BASELINE.md](docs/BASELINE.md).
  Results on real servers and NICs are still to come.

## Workspace

| Crate | Role |
|---|---|
| `krusty-core` | The sequencer state machine: sessions, dedup, conditional appends, bounded tables. Sans-I/O and deterministic; proved in Lean. |
| `krusty-raft` | Sans-I/O Raft core: pre-vote, check-quorum, pipelined batched replication. |
| `krusty-node` | The node: pinned ingress, core, egress, recovery, storage and replication stages around the pure cores. Binaries `krusty-node`, `krusty-load` and `krusty-stat`. |
| `krusty-log` | Segmented log of committed entries, seqno index, snapshots, retention. |
| `krusty-wire` | Native wire formats: ingress, feedback and stream. Fixed little-endian, zero-copy. |
| `krusty-session` | Client session core: seqnos, window, resends, outcomes (sans-I/O). |
| `krusty-stream` | Consumer stream tracker: ordering, gaps, recovery requests, explicit loss (sans-I/O). |
| `krusty-client` | SDK: `Publisher` and `Consumer` over UDP, poll-driven, no threads. |
| `krusty-mold` | MoldUDP64 codec for the optional adapter. |
| `krusty-net` | Batched UDP I/O (`recvmmsg`/`sendmmsg`, Linux). |
| `krusty-ring` | Lock-free SPSC rings and the published history between stages. |
| `krusty-sim` | Deterministic simulator: clients, core, Raft cluster and consumers over a faulty network, with invariant checkers. |

The root crate (`src/`: `sequencer`, `client1`, `loadgen`) is the original
single-process MoldUDP64 prototype. It is kept for its interop tests and
as the M0 baseline.

## Running

A single node, with every setting in a TOML file (see
[`config/node.example.toml`](config/node.example.toml)). Flags override the
file:

```sh
cargo run --release -p krusty-node --bin krusty-node -- --config config/node.example.toml
cargo run --release -p krusty-node --bin krusty-stat -- --watch 1   # live metrics from /dev/shm
```

For a cluster, give each node the same `[cluster] members` list, its own
`id`, and a `[storage]` directory.

Load generator, with latency measured from each message's scheduled send
time:

```sh
# single node, in-process
cargo run --release -p krusty-node --bin krusty-load -- --rate 100000 --duration 10 --spin
# 3-node cluster in one process, durability mode (a)
cargo run --release -p krusty-node --bin krusty-load -- --rate 50000 --duration 10 --spin \
    --cluster 3 --storage /tmp/krusty-bench --durability a
```

For core pinning, see the `--leader-cores` and `--follower-cores` options
in `krusty-load` and the commands in BASELINE.md.

Failover trials (stops the leader repeatedly while a publisher and a
consumer run):

```sh
cargo run --release -p krusty-client --example failover -- 100
```

## Testing and verification

```sh
cargo test                               # unit, integration and end-to-end tests
cargo run --release -p krusty-sim -- --scenario all --seeds 0..1000
spec/tla/check.sh                        # TLA+ models (add --large for the big configs)
PATH=$HOME/.elan/bin:$PATH scripts/check-axioms.sh spec/lean   # Lean proofs
```

- **TLA+** ([spec/README.md](spec/README.md)):
  - `Sequencer.tla` and `SequencerBounded.tla` model sessions and the
    sequencer.
  - `RaftKC.tla` models Raft as `krusty-raft` implements it.
  - Trace validation replays randomized runs of the real `krusty-raft`
    against `RaftKC`.
  - `Egress.tla` models the native stream, recovery and the consumer
    tracker (G1, G2).
- **Lean** (`spec/lean`, against the Aeneas translation of `krusty-core`):
  - `Core::apply` refines an abstract sequencer spec, under an invariant
    that `Core::new` establishes.
  - G4 and G5 are proved on the spec, and G4 is carried over to the code.
  - The client and key tables are proved to behave as finite maps.
- **Simulation.** `krusty-sim` sweeps new seeds every night, single-node
  and cluster scenarios, with partitions, crash-restarts and power loss.
- **CI** (`.github/workflows`): `ci` runs format, clippy, tests and a perf
  smoke test, the bounded TLA+ configs with trace validation, a fuzz smoke
  test, and Verus plus Miri on the ring.
  `proofs` checks the Lean proofs, and fails if the translation is stale
  or any theorem uses `sorry`. `nightly` runs the simulation sweep, trace
  validation and failover trials.

Toolchains: stable Rust. TLA+ needs Java 11+ and `tla2tools.jar` v1.8.0
in `~/.local/share/krusty-tools/tla/`. Lean needs elan and an Aeneas
checkout (see `spec/lean/lakefile.lean` and `.github/workflows/proofs.yml`).

## Design documents

- [Product requirements](docs/PRD.md): scope, guarantees, performance targets
- [Architecture](docs/ARCHITECTURE.md): design, protocols, storage, verification
- [Gap analysis](docs/GAP-ANALYSIS.md): status against the target, with roadmap
- [Baseline](docs/BASELINE.md): measurements per milestone
- [Specs](spec/README.md): TLA+ models, trace validation and Lean proofs; spike reports in `spikes/`
