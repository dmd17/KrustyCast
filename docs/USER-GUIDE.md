# KrustyCast user guide

How to run a KrustyCast cluster, publish to it, consume from it, and operate
it. For the design behind it, see [ARCHITECTURE.md](ARCHITECTURE.md); for
what it guarantees and why, [PRD.md](PRD.md) §5.

Contents:
1. [Concepts](#1-concepts)
2. [Quick start](#2-quick-start)
3. [Deploying](#3-deploying)
4. [Configuration reference](#4-configuration-reference)
5. [Publishing](#5-publishing)
6. [Consuming](#6-consuming)
7. [Operating a cluster](#7-operating-a-cluster)
8. [Tuning for latency](#8-tuning-for-latency)
9. [Measuring](#9-measuring)
10. [Limits](#10-limits)

## 1. Concepts

**The stream.** A cluster takes messages from many publishers and puts them
in one total order. Each message gets the next *seqno* (1, 2, 3, …) and is
published on the *native stream*, a UDP feed (multicast or unicast) that any
number of consumers follow. A stream is named by a 64-bit *stream id*.

**Sessions and incarnations.** A publisher is a *client* with a 32-bit
client id. Each run of a client is an *incarnation* (the SDK uses its start
time in nanoseconds), and numbers its messages 1, 2, 3, … (*client
seqnos*). The cluster remembers each client's session, so a message
resent after a lost reply is recognized and answered with its original
outcome. It is never sequenced twice (G4).

**Outcomes.** Every submitted message gets exactly one answer:

| Outcome | Meaning |
|---|---|
| `ACCEPTED(seqno)` | Sequenced at this stream seqno. |
| `REJECTED(key, version)` | A conditional append lost: the key is at `version`. |
| `BUSY` | The cluster is overloaded; the SDK backs off and resends. |
| `NOT_LEADER(leader)` | Ask another node; the SDK moves on by itself. |
| `SESSION_EXPIRED(min)` | The cluster no longer has this session (evicted, or a session roll): the SDK restarts with an incarnation of at least `min`. |

**Conditional append.** A message may claim the next version of a key
(optimistic concurrency: compare-and-set on a per-key counter). The first
claim on version *v* wins and sets the key to *v* + 1. Later claims on *v*
are REJECTED with the key's current version. Accepted versions per key are
contiguous (G5).

**Cluster.** Three (or five) nodes replicate a log with Raft. Only the
leader accepts messages and publishes; every node applies the same log to
the same deterministic state machine, so any node can serve recovery and
any node can take over (G1, G7). Failover takes about 70 ms at p50
(BASELINE.md).

**Durability modes.**
- *Mode (a)*: a message is acknowledged and published once a majority
  *holds* it; fsync happens off the latency path. It survives any minority
  of nodes failing, but not every node losing power at once.
- *Mode (b)*: acknowledged and published only after a majority has
  *fsynced* it. Survives anything short of losing a majority's disks;
  adds about one device flush to latency.

**Consumers and recovery.** A consumer delivers messages in seqno order. When
datagrams are lost it asks a node (any node) to retransmit the gap; old data
comes from the node's disk. Only data that has aged out of retention can be
lost, and then the consumer is told so explicitly (G2). See
[gap policy](#gap-policy) for what happens when no node answers.

**Session roll.** A cluster can end its stream and start a new one: a new
stream id, seqnos from 1, and a fresh state machine (no client sessions, no
key versions). This is a trading-session boundary, like a new MoldUDP64
session each day. See [§7.4](#74-session-roll).

## 2. Quick start

Build:

```sh
cargo build --release
```

**A single node**, in memory, on loopback:

```sh
./target/release/krusty-node --ingress 127.0.0.1:7000 --feed 127.0.0.1:7001 \
    --recovery 127.0.0.1:7002 --admin 127.0.0.1:7010
./target/release/krusty-admin --node 127.0.0.1:7010 status
```

**A load test** against an in-process node or cluster (no separate node
needed):

```sh
./target/release/krusty-load --rate 10000 --duration 5 --spin
./target/release/krusty-load --rate 10000 --duration 5 --spin \
    --cluster 3 --storage /tmp/krusty-bench --durability a
```

**Failover trials** (stops the leader repeatedly while a publisher and a
consumer run):

```sh
cargo run --release -p krusty-client --example failover -- 20
```

`crates/krusty-client/examples/failover.rs` is also a compact example of a
cluster, a publisher and a consumer in one program.

## 3. Deploying

### 3.1 A cluster

Each node runs `krusty-node --config node.toml`. The nodes share a member
list; each has its own `id` and storage directory:

```toml
# node 1 of 3
ingress  = "0.0.0.0:7000"
feed     = "239.1.1.1:7001"      # the native stream (multicast)
interface = "10.0.0.5"           # this host's address on the feed network
recovery = "0.0.0.0:7002"
stream_id = 20261117
spin = true
cores = [2, 3, 4, 5]             # ingress, core, egress, recovery
admin = "127.0.0.1:7010"

[storage]
dir = "/var/lib/krusty/stream"
durability = "a"

[cluster]
id = 1
members = ["1@10.0.0.5:7100", "2@10.0.0.6:7100", "3@10.0.0.7:7100"]
core = 6                         # replication stage
```

Nodes 2 and 3 differ in `id`, `interface` and (if run on one host) ports
and `dir`. Start them in any order. They elect a leader within an election
timeout (50–100 ms). Publishers are given every node's ingress address;
consumers, every node's recovery address.

Use an odd number of members: 3 tolerates one failure, 5 tolerates two.

### 3.2 A single node

Leave out `[cluster]`. With `[storage]` the node is durable and restarts
from its log. Without it, it is in memory only. To use [session
rolls](#74-session-roll) on a single node, configure it as a one-member
cluster (`members = ["1@127.0.0.1:7100"]`). Raft then elects it at once.

### 3.3 Networks

- **The feed.** Multicast (`feed` = a group address) reaches any number of
  consumers; give `interface`, or the kernel picks the default route, which
  may be a slow link (on the M0 laptop it was Wi-Fi). A unicast `feed`
  sends to one consumer address, which is useful for tests.
- **Replication** (`[cluster] members`) and **ingress** are unicast UDP.
- **The admin endpoint has no authentication.** Bind it to loopback or to a
  management network only. Neither ingress nor replication is
  authenticated yet (gap O4): run the cluster on a trusted network.

### 3.4 Storage and retention

The log is segmented (`segment_mib`, default 256 MiB). Every
`snapshot_every` entries (default 1 M) the node snapshots its state; a
restart loads the latest snapshot and replays the log after it (0.15–0.3 s
with a snapshot every 1 M entries, warm page cache; BASELINE.md M2). Whole segments older than the latest snapshot
are deleted once the log exceeds `retain_gib`. Messages in deleted
segments can no longer be recovered by consumers: size retention for the
longest consumer outage you want to ride out.

### 3.5 MoldUDP64

`[mold]` adds a standard MoldUDP64 feed carrying the same messages with the
same seqnos, plus a re-request server. It is off the native stream's
latency path. The session name is `session` for the configured stream;
after a session roll, the stream id in decimal, zero-padded to 10
characters (so date-like stream ids such as `20261117` read well:
`0020261117`).

## 4. Configuration reference

Every key is optional; unknown keys are errors. Command-line flags of
`krusty-node` override the file: `--ingress`, `--feed`, `--interface`,
`--recovery`, `--stream-id`, `--spin`, `--cores a,b,c,d`, `--admin`,
`--mold-session`, `--mold-feed`, `--mold-rerequest`.

**Top level**

| Key | Default | Meaning |
|---|---|---|
| `ingress` | `0.0.0.0:7000` | Where publishers send. |
| `feed` | `127.0.0.1:7001` | The native stream's destination (multicast group or unicast address). |
| `interface` | | Local address for multicast (required for a multicast feed). |
| `recovery` | `0.0.0.0:7002` | Where consumers send RECOVER requests. |
| `stream_id` | `1` | The first stream's id. After a session roll the current id comes from the log. |
| `spin` | `false` | Stages busy-poll instead of sleeping (lowest latency; one core each). |
| `cores` | | CPUs for ingress, core, egress, recovery. |
| `admin` | off | `krusty-admin` endpoint (no authentication). |
| `metrics` | `/dev/shm/krusty-<stream_id>.stats` | Shared-memory metrics file for `krusty-stat`. |

**`[limits]`**

| Key | Default | Meaning |
|---|---|---|
| `mtu` | 1472 | Largest datagram on the feed. |
| `max_payload` | 1024 | Largest message payload. |
| `history` | 65536 | Messages kept in memory for recovery (a power of two). |
| `max_clients` | 4096 | Client sessions the cluster remembers. |
| `outcome_ring` | 64 | Outcomes remembered per session; a publisher's `window` must not exceed it. |
| `max_keys` | 65536 | Keys with a version (conditional appends). |
| `evict_idle_ms` | 60000 | When the client table is full, a session idle at least this long may be evicted for a new client. Its client later gets SESSION_EXPIRED. |

**`[storage]`**

| Key | Default | Meaning |
|---|---|---|
| `dir` | required | Log directory. |
| `durability` | `"a"` | `"a"` or `"b"` (§1). |
| `snapshot_every` | 1000000 | Log entries between snapshots. |
| `segment_mib` | 256 | Segment size. |
| `retain_gib` | 64 | Keep at least this much log. |
| `core` | | CPU for the storage stage. |

**`[mold]`**

| Key | Default | Meaning |
|---|---|---|
| `session` | required | MoldUDP64 session name (1–10 printable ASCII). |
| `feed` | required | MoldUDP64 feed destination. |
| `rerequest` | `0.0.0.0:7003` | Re-request server. |

**`[cluster]`**

| Key | Default | Meaning |
|---|---|---|
| `id` | required | This node's id (≥ 1). |
| `members` | required | `"<id>@<ip>:<port>"` for every member, this one included (its replication address). |
| `election_min_ms`, `election_max_ms` | 50, 100 | Election timeout range. |
| `heartbeat_ms` | 10 | Leader heartbeat. Must be below `election_min_ms`. |
| `max_append_bytes` | 60000 | Most entry bytes per replication datagram. |
| `max_inflight` | 2 | Appends sent to a follower and not yet acknowledged. Keep it small: the leader sends an Append on every pass with new entries, and a deep pipeline only queues them (at 16, p50 tripled under load; docs/BASELINE.md E3). Raise it only for long round trips, where 2 × `max_append_bytes` per round trip would limit throughput. |
| `retain_entries` | 65536 | Committed entries kept in memory for lagging followers (older ones are read from disk). |
| `core` | | CPU for the replication stage. |

`config/node.example.toml` lists every key with a comment.

## 5. Publishing

Rust SDK: `krusty-client`. A `Publisher` is poll-driven (no threads):

```rust
use krusty_client::{Publisher, PublisherConfig, PublisherEvent};
use krusty_wire::ingress::Append;

let nodes = vec!["10.0.0.5:7000".parse()?, "10.0.0.6:7000".parse()?, "10.0.0.7:7000".parse()?];
let mut p = Publisher::new(PublisherConfig::new(42, nodes))?;   // client id 42
let seq = p.submit(Append::Unconditional, b"order 1")?;          // its client seqno
loop {
    p.poll(|e| match e {
        PublisherEvent::Accepted { client_seqno, global_seqno } => { /* sequenced */ }
        PublisherEvent::Rejected { client_seqno, key, current_version } => { /* CAS lost */ }
        PublisherEvent::Failed { client_seqno, reason } => { /* fate unknown: see below */ }
        PublisherEvent::Restarted { incarnation } => { /* new session */ }
    })?;
    // ... submit more, do other work ...
}
```

- **Conditional append**: `Append::Conditional { key, expected_version }`.
  The first message for a new key expects version 0.
- **The window** (`window`, default 64) caps unanswered messages; `submit`
  returns `WindowFull` until outcomes arrive. It must not exceed the
  cluster's `outcome_ring`.
- **Retries** are automatic: a message is resent every `retry_interval_ns`
  (20 ms) until it has an outcome, up to `max_attempts` (50). A resend is
  never sequenced twice; while the original is still being committed, the
  leader drops it (`resends_filtered`), so retries do not multiply the log
  when commits are slower than the retry interval.
- **Batching** is automatic: everything due at a `poll` goes out packed
  back to back in as few datagrams as fit under `mtu` (default 1472), and
  outcomes come back packed the same way. Nothing waits: at low rates a
  datagram carries one message. Set `mtu` to your path MTU less 28 bytes
  (IP and UDP headers).
- **Failover** is automatic: the publisher follows NOT_LEADER to the named
  leader, or moves to the next node after `failover_ns` (60 ms) of silence.
- **`Failed`** means the fate is unknown: the message may or may not have
  been sequenced. Reasons: `NoAnswer` (`max_attempts` exhausted),
  `SessionExpired` (the session was evicted, or the cluster rolled to a new
  stream), `Restarted`. After `SessionExpired` the publisher restarts with a
  new incarnation by itself (`Restarted` event). If a failed message
  matters, check the stream for it (by content) before resubmitting, or
  make messages idempotent at the application level.

## 6. Consuming

```rust
use krusty_client::{Consumer, ConsumerConfig, ConsumerEvent};

let recovery = vec!["10.0.0.5:7002".parse()?, "10.0.0.6:7002".parse()?, "10.0.0.7:7002".parse()?];
let mut cfg = ConsumerConfig::new("239.1.1.1:7001".parse()?, recovery);
cfg.interface = "10.0.0.9".parse()?;   // this host, for the multicast join
cfg.start_seqno = Some(1);             // from the start (None: from the first frame seen)
let mut c = Consumer::new(cfg)?;
loop {
    c.poll(|e| match e {
        ConsumerEvent::Message(d) => { /* d.seqno, d.timestamp_ns, d.client_id, d.conditional, d.payload */ }
        ConsumerEvent::Lost { count } => { /* count messages are gone: see below */ }
        ConsumerEvent::EndOfStream => { /* the stream ended (shutdown, or a session roll) */ }
    })?;
}
```

- Messages arrive in seqno order with no duplicates.
- **Use `d.timestamp_ns`** (the leader's clock, replicated) instead of your
  own clock when building state from the stream: every consumer and every
  replay then computes the same thing.
- **Start anywhere**: `start_seqno = Some(n)` replays from `n`, recovering
  what the feed no longer carries from the nodes (memory or disk).
- **Streams**: `stream_id = None` (the default) follows the first stream
  seen and, after its end, the next one (session rolls). `Some(id)` pins one
  stream.

### Gap policy

When datagrams are lost, the consumer waits a short random delay (so many
consumers missing the same data do not all ask at once), then asks a node
for the gap, and retries while no answer comes. If a node answers that the
data has aged out of retention, the consumer reports `Lost` and moves on.
What it does when **no node answers** is a policy choice,
`ConsumerConfig::gap_policy`:

| Policy | Behaviour |
|---|---|
| `GapPolicy::GiveUpAfter(n)` (default, n = 50) | After n unanswered requests (about 1 s at the default retry interval), report the gap `Lost` and deliver what follows. Delivery keeps moving, but the loss may cover data the cluster still holds. |
| `GapPolicy::Wait` | Keep asking until the data arrives or a node says it has aged out. Loss is only ever for data outside retention, but delivery stalls while no node answers. |

Choose `Wait` when completeness matters more than progress (a replica
building state), `GiveUpAfter` when progress does (a live display). Both
are model-checked (spec/README.md, `Egress.tla`).

## 7. Operating a cluster

### 7.1 krusty-admin

`krusty-admin` talks to nodes' admin endpoints. `--node` may repeat; the
default is `127.0.0.1:7010`.

```sh
krusty-admin --node 10.0.0.5:7010 --node 10.0.0.6:7010 --node 10.0.0.7:7010 status
```

```
NODE                    ID ROLE           TERM LEADER     COMMIT    APPLIED   STREAM       NEXT   SNAPSHOT CLIENTS   UPTIME
10.0.0.5:7010            1 leader            3      1     120034     120034 20261117     118722     100000      12    3600s
10.0.0.6:7010            2 follower          3      1     120034     120034 20261117     118722     100000      12    3600s
10.0.0.7:7010            3 follower          3      1     120034     120031 20261117     118719     100000      12    3600s
```

| Command | What it does |
|---|---|
| `status` | One row per node. `--raw` prints every field, including counters. |
| `snapshot` | Each node snapshots now (with storage); prints the log index. |
| `transfer ID` | The leader (found among the nodes given) hands leadership to node ID. |
| `roll [STREAM]` | Session roll (§7.4). |

The admin protocol is one UDP datagram each way, plain text (`status`,
`snapshot`, `transfer 2`, `roll 20261118`), so `nc -u` works too.

`krusty-stat` reads one node's shared-memory metrics locally
(`krusty-stat --watch 1`): counters with rates, gauges, and per-stage
latency histograms.

### 7.2 Restarts and failures

- **A follower** that stops and restarts loads its latest snapshot, replays
  its log, and catches up from the leader. Nothing is lost.
- **The leader** failing: a follower is elected within 50–100 ms; publishers
  and consumers follow on their own.
- **A majority** down: the cluster stops accepting (it cannot commit) and
  resumes when a majority is back.
- **Clean shutdown**: Ctrl-C (SIGINT/SIGTERM). A single node ends its stream
  with END_OF_SESSION; a cluster member leaving does not (the stream goes
  on).

### 7.3 Planned maintenance

To take the leader down for maintenance without an election timeout, first
move leadership:

```sh
krusty-admin --node 10.0.0.5:7010 --node 10.0.0.6:7010 --node 10.0.0.7:7010 transfer 2
```

The leader stops taking new messages, brings node 2 up to date, and tells
it to stand for election at once. Publishers are pointed at node 2 while
this happens. If node 2 has not taken over within an election timeout, the
old leader carries on.

### 7.4 Session roll

```sh
krusty-admin --node ... roll 20261118
```

On the leader's instruction every node, at the same log entry:
1. ends the current stream (END_OF_SESSION at its next seqno),
2. starts stream 20261118 at seqno 1 with a fresh state machine (no client
   sessions, no key versions).

- **Publishers** started before the roll get SESSION_EXPIRED on their next
  message, restart with a new incarnation by themselves, and report their
  in-flight messages `Failed`. Nothing is sequenced twice.
- **Consumers** (unpinned) report `EndOfStream`, then follow the new stream
  from seqno 1. The old stream's tail stays recoverable by its stream id.
- **MoldUDP64** ends its session and continues as session `0020261118`.
- Stream ids are never reused; the default is the current id + 1. A roll
  needs `[cluster]` and `[storage]`. It is refused during a leadership
  transfer.

### 7.5 What to watch

| Signal | Where | Meaning |
|---|---|---|
| One leader, same term everywhere | `status` | Healthy. Changing terms: elections (network, overload). |
| `applied` lagging `commit` | `status` | A slow node (disk, CPU). |
| `busy` rising | `status --raw`, `krusty-stat` | Ingress overload: publishers are backing off. |
| `not_leader` rising | `status --raw` | Publishers talking to followers (normal briefly after failover). |
| `repl_unservable` | `krusty-stat` | A follower is so far behind that the leader can no longer feed it (InstallSnapshot is not implemented; §10). |
| `recovered_from_disk` | `krusty-stat` | Consumers recovering old data. |
| `mold_lost` | `krusty-stat` | The Mold adapter fell behind the history. |
| `raft_pre_votes`, `raft_elections`, `raft_quorum_lost` | `krusty-stat` | Elections this node started, and step-downs as leader because a majority went quiet. Rising under steady load: some node stopped answering (look at its `cluster_loop_max_ns`). |
| `resends_filtered` | `krusty-stat` | Resends the leader dropped because the original is still being committed. High: commits are slower than clients' retry interval (mode b on slow disks, overload). |
| `ingress_datagrams`, `feedback_datagrams` | `krusty-stat` | Datagrams in and out for clients. `submits` / `ingress_datagrams` is SUBMITs per datagram, and `feedback` / `feedback_datagrams` is outcomes per datagram: near 1 at low load, higher under load as publishers and the node pack (see Batching under the publisher). |
| `submits_deferred` | `krusty-stat` | Passes in which the leader left new submits waiting because storage or egress was behind (admission control: publishers then get BUSY). Rising: the node is at capacity. |
| `cluster_loop_max_ns`, `cluster_loop` | `krusty-stat` | The longest pass of the Raft thread, and their distribution. Passes near the election timeout (50 ms by default) cost leadership; long ones mean the thread waited on a full ring, usually storage behind a slow disk. |

## 8. Tuning for latency

The PRD targets (p50 ≤ 40 µs, p99 ≤ 80 µs, max ≤ 1 ms) were met on one
8-core machine with these settings (BASELINE.md, M3):

- **Spin and pin.** `spin = true`, and give each spinning stage its own
  CPU: `cores` (ingress, core, egress, recovery), `[cluster] core`
  (replication) and `[storage] core`. A node uses 4–6 CPUs.
- **Isolate those CPUs** from the scheduler and interrupts. Either at boot
  (`isolcpus=… nohz_full=… rcu_nocbs=…`) or at run time: confine every
  systemd slice to the housekeeping CPUs
  (`systemctl set-property --runtime system.slice AllowedCPUs=0,8`, the
  same for `user.slice` and `init.scope`), move IRQs there
  (`/proc/irq/*/smp_affinity_list`), set the workqueue mask
  (`/sys/devices/virtual/workqueue/cpumask`), and run the node in its own
  slice (`systemd-run --scope --slice=krusty.slice -p AllowedCPUs=…`).
- **The `performance` CPU governor.**
- **Hyper-threads.** On critical stages, avoid sharing a physical core
  with another spinning stage.
- **Mode (b)** costs about one device flush per group commit: use an
  enterprise NVMe with power-loss protection, whose flush is fast.

A sleeping node (`spin = false`) uses little CPU and adds tens of
microseconds; it is fine for most non-trading uses.

## 9. Measuring

`krusty-load` is an open-loop load generator: each message is due at a
scheduled time and latency is measured from then, so stalls count (no
coordinated omission). A run is INVALID (exit 1) if the generator fell
behind, anything was lost, or there was no progress for 5 s (STALLED, e.g. a
leader change: the generator does not follow the leader). It reports each
sender's busy fraction; near 100% means the generator, not the node, set
the pace. Its senders pack SUBMITs as the publisher does (`--mtu`,
default 1472; `--mtu 0` sends one per datagram, the behaviour before
2026-11-21).

```sh
krusty-load --rate 75000 --duration 600 --warmup 5 --spin --combined --pin-senders 7 \
    --cluster 3 --leader-cores 1,2,3,4,12 --follower-cores 5,13,10,10/6,14,11,11 \
    --storage /var/tmp/krusty-bench --durability a
```

It runs the nodes in its own process over loopback, or drives a running
node or cluster leader (`--connect INGRESS --feed-bind ADDR`, as in
`bench/krusty-cluster/run.sh`, which runs each node in its own process).
`--senders N --combined` spreads the load over N sessions, the first of
which also reads the feed. The comment at the top
of `crates/krusty-node/src/bin/krusty-load.rs` lists every option;
BASELINE.md has methods and results.

## 10. Limits

What v1 does not do yet:
- **No authentication or encryption** on ingress, replication or admin
  (gap O4). Run on a trusted network.
- **No membership changes**: the member list is fixed. A node that loses
  its disk cannot be replaced safely yet: copying a peer's directory would
  copy the peer's Raft vote too. Keep the disks.
- **No InstallSnapshot**: a follower that falls behind what the leader
  still retains on disk cannot catch up (`repl_unservable`). Size
  `retain_gib` so the longest node outage you plan for fits.
- **Throughput**: about 150 000 msg/s per cluster with one ingress thread
  (NFR-5's 1 M msg/s is M4 work).
- **IPv4 only**, Linux only (batched UDP via `recvmmsg`/`sendmmsg`).
- **Unicast fan-out** of the native stream for networks without multicast
  is not done (gap K5); use a unicast `feed` for a single consumer.
