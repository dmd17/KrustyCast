# M0 baseline: today's single node on a laptop

Date: 2026-11-14. Tool: `src/bin/loadgen.rs` (GAP-ANALYSIS E1), against the
original prototype. Both were removed on 2026-11-16 (in the git history up to
131dc5e); `krusty-load` replaces `loadgen` for everything after M0.
These are **laptop loopback numbers, for regression tracking and for finding
bottlenecks**. They are not comparable to the PRD §7 targets, which assume
the reference servers and real NICs (see E2).

## Method

- **Open loop.** Message *i* is due at `start + i / rate`. Latency is
  measured from the scheduled send time to arrival on the feed, so stalls in
  the generator or the system count against the result (no coordinated
  omission). Latency from the *actual* send time is reported alongside; the
  two differ when the generator falls behind. A run is marked **INVALID**,
  and the tool exits non-zero, if the generator could not sustain the
  offered rate or a message never arrived.
- Behaves like a real client: resends from the NAKed seqno (go back N).
- Sequencer in-process (`sequencer::start`), journal off unless stated.
- Feed published to a **unicast loopback** address (`--feed 127.0.0.1:…`):
  see finding 1 for why not multicast on this machine.
- i7-9750H (6 cores, HT off), PREEMPT_RT 7.0 kernel, sender and receiver
  threads pinned, no other load. 64-byte payloads, 5 s measured after 1 s
  of warm-up. HDR histograms, 3 significant digits.

Reproduce:

```
cargo build --release --bin loadgen
./target/release/loadgen --feed 127.0.0.1:17201 --rate 100000 \
    --pin-sender 4 --pin-receiver 5 --duration 5 --warmup 1
```

## Results

Submit-to-feed latency from the scheduled send time, µs:

| Offered msg/s | Senders | p50 | p90 | p99 | p99.9 | p99.99 | max | Notes |
|---|---|---|---|---|---|---|---|---|
| 5 000 | 1 | 21.7 | 22.6 | 28.5 | 49.0 | 69.0 | 78.7 | |
| 20 000 | 1 | 20.8 | 21.7 | 31.1 | 68.7 | 85.4 | 170.6 | |
| 50 000 | 1 | 19.7 | 20.4 | 34.0 | 62.6 | 86.5 | 233.0 | |
| 100 000 | 1 | 20.5 | 27.6 | 40.6 | 82.9 | 129.9 | 244.0 | |
| 200 000 | 1 | 22.6 | 37.5 | 81.7 | 185.2 | 556.5 | 746.0 | generator near its limit |
| 300 000 | 2 | 16.2 | 54.5 | 1 856 | 3 115 | 3 998 | 4 469 | sequencer saturating |
| 400 000 | 2 | 64.7 | 1 007 | 3 119 | 8 024 | 10 322 | 10 764 | overloaded |
| 600 000 | 2 | — | — | — | — | — | — | did not finish within 120 s |

With the journal (on ext4 on the laptop's NVMe):

| Offered msg/s | fsync | p50 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|---|
| 10 000 | never | 20.7 | 342 | 1 746 | 2 351 | 2 650 |
| 50 000 | never | 19.8 | 556 | 2 112 | 3 000 | 3 260 |
| 10 000 | every batch | 1 696 | 2 959 | 5 673 | 7 438 | 7 938 |
| 50 000 | every batch | 1 752 | 3 719 | 5 567 | 6 734 | 7 021 |

A single sender thread (one `sendto` per message) tops out near 245 k msg/s;
use `--senders` above that.

## Findings

1. **Multicast through the default route went out over Wi-Fi and capped the
   node near 5–10 k msg/s.** With the feed on `239.x` and
   `--interface 0.0.0.0`, the kernel sent it through `wlp5s0`, where
   multicast is transmitted at the low basic rate. The send buffer filled and
   the sequencing thread blocked in `sendto` while its CPU sat idle. This is
   an operational trap for anyone trying the product: the tuning guide (E2)
   must say to set `--interface` explicitly, and the node should warn when
   the multicast route is a wireless interface.
2. **Default socket receive buffers dropped packets** at modest rates
   (`rmem_default` 208 KiB holds a few hundred small datagrams). Fixed:
   `net::bind_receiver` now asks for 8 MiB (capped by `rmem_max`).
3. **One lost upstream message caused a NAK storm and a stderr storm.** The
   sequencer NAKs every message after a gap until the client goes back, and
   logged each NAK to stderr, which slowed it further. Fixed: that log line
   is verbose-only. (Gap W3's BUSY and a NAK rate limit remain to do.)
4. **The ingress path saturates near 300 k msg/s** on loopback: one thread
   does `recvfrom` + decode + an allocation + a channel send per datagram.
   Reaching the PRD's 1 M msg/s needs `recvmmsg`-style batching, preallocated
   buffers and SPSC rings instead of an unbounded channel (P1, P3).
5. **The journal adds tail latency even without fsync**: p99 rises from about
   30 µs to 0.3–0.6 ms, with multi-ms maxima, from appending to a growing
   ext4 file through `BufWriter` (S1: preallocated segments, io_uring).
6. **fsync per batch costs about 1.7 ms at p50 on this consumer NVMe**, as
   expected without power-loss protection. Durability mode (b) needs
   enterprise drives; mode (a) relies on quorum replication instead.

## M1 node (krusty-node), 2026-11-14

The new node: pinned ingress, core and egress stages connected by lock-free
rings, `recvmmsg`/`sendmmsg`, no allocation after start-up (enforced by a
test), and a session-based load generator (`krusty-load`).

**This machine has `isolcpus=2-5 nohz_full=2-5 rcu_nocbs=2-5`.** Core 1
carries the IRQ threads (SCHED_FIFO 50), and any thread placed there,
node stage or load generator, picks up millisecond tails. The numbers
below put the node's three spinning stages on isolated cores 2–4 and the
generator on isolated core 5, sending and reading the feed in the same
thread (`--combined`).

```
./target/release/krusty-load --rate 100000 --duration 5 --warmup 1 --spin \
    --node-cores 2,3,4 --pin-senders 5 --combined
```

Latency from the scheduled send time, µs (unicast loopback feed, 64-byte
payloads, one client with a window of 64):

| Offered msg/s | Path | p50 | p90 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|---|---|
| 10 000 | to feed | 14.6 | 15.5 | 17.5 | 21.7 | 35.2 | 47.5 |
| 10 000 | to ACCEPTED | 12.4 | 13.7 | 15.3 | 19.5 | 33.2 | 43.9 |
| 100 000 | to feed | 18.5 | 19.3 | 19.6 | 25.2 | 59.8 | 126.8 |
| 100 000 | to ACCEPTED | 16.7 | 17.6 | 17.9 | 21.3 | 55.5 | 116.1 |
| 150 000 | to feed | 33.1 | 38.9 | 45.2 | 68.1 | 139.0 | 193.9 |

Against the old node at 100 k msg/s (p50 20.5, p99 40.6, p99.99 130 µs),
p99 halves and the tail stays below 130 µs. This is within the PRD Tier A
latency targets (p50 ≤ 40, p99 ≤ 80, p99.99 ≤ 200 µs), but on loopback,
without a NIC hop, and without replication; the servers decide.

**Throughput could not be measured here.** Above about 150 k msg/s the
generator needs more cores than the laptop has isolated: extra senders
on cores 0–1 are descheduled for milliseconds, their windows stall, and
the result swings between 44 k and 170 k msg/s from run to run. In those
runs the node was not saturated: with idle sleeping enabled, the stages
used 40% (ingress), 18% (core) and 22% (egress) of a core at about
165 k msg/s. Ingress, at roughly 2.4 µs of CPU per datagram, would
saturate near 400 k msg/s, so the PRD's 1 M msg/s on kernel UDP will need
several ingress threads (or Tier B kernel bypass). That needs the servers
to confirm.

The egress stage sends one feedback datagram per message, which also
costs kernel time per datagram. Batching outcomes per client and core
batch is the obvious next protocol optimization.

## M2 storage, 2026-11-14

Same machine and placement as above (node stages on isolated cores 2–4,
generator on 5, `--combined`); log on the laptop's consumer NVMe (ext4).
The storage stage itself is unpinned (no isolated core left).

**Device floor.** `cargo run --release -p krusty-log --example fsync`
(append 112 bytes + `fdatasync`, 5000 times): p50 716–733 µs, p99
891–1109 µs, p99.9 1.2–2.8 ms across runs. Pre-writing segment files
instead of only `fallocate`-ing them made no difference: the time is the
drive's cache flush (no power-loss protection), not filesystem metadata.

Submit-to-feed latency from the scheduled send time, µs:

| Mode | Offered msg/s | p50 | p90 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|---|---|
| (a) | 1 000 | 16.1 | 18.8 | 25.1 | 29.6 | 35.5 | 35.5 |
| (a) | 10 000 | 15.7 | 16.6 | 18.5 | 22.0 | 37.4 | 66.3 |
| (a) | 50 000 | 15.5 | 16.4 | 16.7 | 17.7 | 37.9 | 84.1 |
| (b) | 1 000 | 1 276 | 1 885 | 2 058 | 2 730 | 5 714 | 5 714 |
| (b) | 10 000 | 1 595 | 1 994 | 2 568 | 4 370 | 6 001 | 6 222 |

- **Mode (a)** is unaffected by storage: the storage stage runs behind,
  off the latency path.
- **Mode (b) costs one to two fsyncs.** With one group commit at a time, a
  message that arrives just after an fsync started waits for it and then
  for its own (mean ~1.5×, tail ~2×). An experiment with 2–4 overlapping
  `fdatasync` threads did not help on this drive (it serializes cache
  flushes). NFR-9 was relaxed on 2026-11-15 to ≤ 2 × device fsync p99 +
  30 µs (PRD open question 7), 1.81–2.25 ms with the floor above. Against
  that, 1 000 msg/s (p99 2.06 ms) is borderline and 10 000 msg/s (p99
  2.57 ms) misses. The floor and the latencies were measured in separate
  runs; the re-measure below explains the gap. Mode (b) in a cluster: see M3.
- **Each group commit was two device flushes, not one.** `Log::sync`
  fdatasynced the segment *and* the egress index file; the `fsync` example
  writes no index, so it measured one. The index need not be durable per
  batch (recovery rewrites it from the last snapshot, and a snapshot
  syncs it fully), so the storage stage now calls `Log::sync_entries`
  (segments only). Fixed on 2026-11-15, together with full syncs now
  covering every index file written since the last one (they synced only
  the newest, so a roll between syncs could leave an older file's tail
  unsynced).

**Re-measured in one session, 2026-11-15**, on the second machine
(i9-9980HK, WD Blue SN570 consumer NVMe, ext4, no isolated cores; node on
cores 2–4, generator on 5; another workload using about 1.3 cores was
running). The floor and each latency run alternate; five rounds:

| | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| Device floor (`fsync` example) | 212–231 | 251–283 | 284–325 | 351–571 (one 2 839) | 1.4–5.7 ms |
| (b) 1 000 msg/s, two flushes | 375–380 | 402–490 | 510–536 | 736–6 029 | 1.6–9.7 ms |
| (b) 10 000 msg/s, two flushes | 572–575 | 746–750 | 969–1 022 | 1 684–4 239 | 3.2–6.7 ms |
| (b) 1 000 msg/s, one flush | 198–202 | 218–253 | 270 | 315–3 179 | 0.9–7.6 ms |
| (b) 10 000 msg/s, one flush | 292–294 | 374–376 | 495–499 | 1 492–2 994 | 4.7–7.7 ms |

(µs unless stated; submit to feed. The 10 000 msg/s one-flush row leaves
out one round in which the generator was more than 100 µs late 435 times,
p99 1 428 µs.) The target in the same session is 2 × 284–325 + 30 =
598–680 µs: **NFR-9 is met at both rates**, with p99 at about one flush
at 1 000 msg/s and 1.6 flushes at 10 000. The p99.9 and max are 2–10 ms,
from the drive (the floor's own max is 1.4–5.7 ms) and from the
neighbouring workload. NFR-9 bounds p99 only. This drive's flush is about
3× faster than the laptop's. Server NVMe with power-loss protection
acknowledges a flush from its capacitor-backed cache in tens of µs, so
there the stage's own overheads matter more than the device. Measure on
the servers.
- At 50 000 msg/s in mode (b) a single client is window-bound: 64 messages
  in flight over a ~2.5 ms round trip is ~25 k msg/s.

**Restart** (`cargo run --release -p krusty-node --example restore_time`):
log built directly, then `krusty_node::start` timed (restore happens before
it returns). Warm page cache (the log was just written):

| Entries | Snapshot at | Replayed | Ready in |
|---|---|---|---|
| 1 M | 1 M | 0 | 0.15 s |
| 1 M | none | 1 M | 0.27 s |
| 5 M | 4 M | 1 M | 0.29 s |
| 5 M | none | 5 M | 1.03 s |

Proposed budget (the PRD has none yet): **restart ≤ 1 s with a snapshot
every 1 M entries**; met here. Cold-cache restarts read the log from the
device (~0.5 s more per GB at NVMe speeds); not measured, since dropping
the page cache needs root. Two bugs found by this benchmark are fixed:
opening a log read every record with two system calls (1.4 s for a full
segment; now 1 MiB reads, 64 ms), and a restart from a snapshot older
than the last index file failed ("index entry out of order").

## M3 replication, 2026-11-15

Three replicated nodes in one process over loopback (`krusty-node` cluster
mode, durability (a) unless stated).

**Failover (NFR-6: leader crash to the first new message on the feed,
p99 ≤ 200 ms).** `crates/krusty-client/examples/failover.rs`: a publisher
sends one message every 200 µs, a consumer follows the feed, and each trial
stops the leader (its threads end, it sends nothing more), waits for a
message with a seqno beyond anything the old leader assigned, then
restarts the stopped node from its log. Default timeouts (election
50–100 ms, heartbeat 10 ms; publisher moves on after 30 ms of silence).
Each node's stages share one isolated core (`FAILOVER_CORES=2,3,4`).

```
FAILOVER_CORES=2,3,4 cargo run --release -p krusty-client --example failover -- 100
```

| Trials | New leader p50 | max | New messages on the feed p50 | p90 | max | Failed messages |
|---|---|---|---|---|---|---|
| 100 | 68.6 ms | 170.1 ms | 70.1 ms | 96.2 ms | 171.3 ms | 0 |

All 100 within 200 ms; most of it is the election timeout (the earliest
follower notices after 50–100 ms without a heartbeat). Unpinned, and even
confined to the two non-isolated cores (`taskset -c 0,1`, 20 trials), the
worst trial took 91 ms. This is loopback on a laptop; the servers decide.
The nightly workflow runs 20 trials and fails above 1 s.

**Mode (b) in a cluster** (followers acknowledge only fsynced entries,
`repl_held`; architecture §8.2). Measured on a second machine (i9-9980HK,
8 cores with HT on, 6.8 lowlatency kernel, no isolated cores, unpinned),
with mode (a) on the same machine for comparison:

```
FAILOVER_DURABILITY=b cargo run --release -p krusty-client --example failover -- 100
```

| Mode | Trials | New leader p50 | max | New messages on the feed p50 | p90 | max | Failed messages |
|---|---|---|---|---|---|---|---|
| (b) | 100 | 65.5 ms | 142.5 ms | 66.1 ms | 83.9 ms | 143.9 ms | 0 |
| (a) | 100 | 64.5 ms | 94.9 ms | 71.9 ms | 101.9 ms | 112.8 ms | 0 |

Mode (b) does not slow failover: the time is still mostly the election
timeout. The
`krusty-sim` scenario `cluster3-b` adds power losses, a third of them of
the whole cluster. 1 000 seeds are clean. The same scenario with followers
acknowledging unsynced entries loses ACCEPTED messages, or the histories
diverge, in 9 of 20 seeds.

The benchmark found two bugs, now fixed: finding an entry by log index read
1 MiB per lookup, and a restarted node looked up every re-applied entry
that way, so it fell seconds behind, missed heartbeats and set off repeated
elections (terms climbed by dozens). Lookups now read 16 KiB, and a
restart hands the storage stage the positions it already found.

**Latency and throughput could not be measured meaningfully here.** A
3-node cluster plus the generator needs about ten dedicated cores; this
laptop isolates four. With each node's stages sharing one core,
`krusty-load --cluster 3 --cluster-cores 2,3,4 --combined --pin-senders 5`
gives p50 ≈ 1.2 ms at 10 k msg/s and tops out at 45 k msg/s (window-bound:
64 messages per ≈ 1.4 ms round trip). For comparison, one *single* node
with all its stages on one core gives p50 310 µs (against 15 µs on
dedicated cores, M1): sharing a core dominates. The cluster mode of
`krusty-load` is there for the servers.

**Cluster latency on the second machine, 2026-11-16** (i9-9980HK, 8 cores
with HT, no isolated cores, the same neighbouring workload as in M2;
loopback, 64-byte payloads, 5 s runs, µs from the scheduled send).

*Sleeping stages woke up to 50 µs late (fixed).* Nodes sleep when idle by
default (`sleep_us` 50), and Linux lets a normal thread's sleep overrun by
its timer slack, 50 µs by default, whatever it asked for. Stage threads
now set a 1 µs slack before they first sleep. One default node at
10 000 msg/s (stages on cores 2–4, three runs each):

| | p50 | p99 | p99.9 |
|---|---|---|---|
| default slack | 61–81 | 140–209 | 235–2 220 |
| 1 µs slack | 35–41 | 85–101 | 121–709 |

In a cluster where the followers sleep and share one core each
(`--cluster-cores 7,15`, leader spinning on 2–5), p50 at 10 000 msg/s fell
from 219 to 130 µs and p99 from 363 to 264 µs. Shorter follower sleeps (1–10 µs)
make it worse, with five threads on one core waking constantly.

*Followers on cores of their own.* `--follower-cores c,repl,o/...`: each
follower spins, with its core stage on c, replication on repl, and its
idle ingress, egress and recovery stages sharing o. Leader on 2–5,
generator on 6, followers `7,15,1/0,8,9`:

```
krusty-load --rate 10000 --duration 5 --warmup 1 --spin --combined --pin-senders 6 \
    --cluster 3 --leader-cores 2,3,4,5 --follower-cores 7,15,1/0,8,9 \
    --storage DIR --durability a
```

| Mode | Offered msg/s | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| (a) | 10 000 | 36–43 | 45–48 | 56–82 | 428–660 | 1.3–3.9 ms |
| (a) | 50 000 | 29–37 | 34–49 | 41–88 | 66 – 2 486 | 2.1–2.5 ms |
| (a) | 100 000 | 126–148 | 211–218 | 262–264 | 862–2 208 | 2.0–3.3 ms |
| (b) | 1 000 | 280–309 | 340–816 | 426–2 380 | 1 026–7 479 | 4.1–7.5 ms |
| (b) | 10 000 | 587–591 | 813–817 | 1 155–1 182 | 2 090–2 972 | 3.3–4.7 ms |

- Replication adds about 15–25 µs at p50 over one node with spinning
  stages on the same machine (p50 9–16 µs, p99 27–36 µs at 10 000–50 000
  msg/s): two loopback hops and the follower's Raft step.
- 100 000 msg/s is past what this machine can drive. The leader, the
  generator, two followers and three unpinned storage stages make about
  fourteen spinning threads on sixteen hardware threads, plus the
  neighbour. The generator itself was late thousands of times, and one
  50 000 msg/s run (not in the table) stalled for 140 ms with the
  generator 60 000 submits behind. Six reruns of that case were clean.
- Mode (b) is about twice the single-node figure (M2) because the three
  nodes share one consumer NVMe, which serializes cache flushes, and a
  commit needs the leader's fsync and a follower's. On separate servers
  they flush in parallel.
- The storage stage could not be pinned then; it now takes a core
  (`[storage] core`, see below).

### nuc2 with isolated cores, 2026-11-16

The same machine with nothing else running and its cores isolated at
run time, without a reboot: every systemd slice, every movable IRQ and
the unbound workqueues on CPUs 0 and 8 (one physical core), and the
benchmark in a slice of its own on 1–7 and 9–15 (`systemd-run --scope
--slice=bench.slice -p AllowedCPUs=1-7,9-15`), with the `performance`
governor. The NVMe queue interrupts stay per CPU, so each storage stage
takes its own completions. The kernel tick still runs everywhere (no
`nohz_full`). osnoise on an idle isolated CPU: at most 38 µs of noise
at once, about 0.9% of the CPU, nothing at millisecond scale.

Layout (CPU n and n+8 are one physical core): leader ingress 1, core 2,
egress 3, replication 4, storage 12; follower A core 5, replication 13,
idle stages and storage 10; follower B 6, 14, 11; generator 7.

```
krusty-load --rate 75000 --duration 600 --warmup 5 --spin --combined --pin-senders 7 \
    --cluster 3 --leader-cores 1,2,3,4,12 --follower-cores 5,13,10,10/6,14,11,11 \
    --storage DIR --durability a
```

Maximum sustained rate: 150 000 msg/s (p99 94 µs to ACCEPTED); at
160 000 the generator falls behind and latency climbs to milliseconds;
200 000 achieves 167 000. *(Correction, E3: the cluster, not the
generator, set this ceiling: the storage stage fsynced every 128 entries.
Fixed; see E3 below.)* The PRD measures at half the maximum, so
75 000 msg/s, 64 B payloads, mode (a), for 10 minutes (45.4 M messages,
none lost, one leader term):

| Run | p50 | p90 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|---|
| 1, to feed | 24.6 | 28.1 | 37.5 | 49.9 | 64.0 | 370 |
| 2, to feed | 24.6 | 28.0 | 38.0 | 50.3 | 66.4 | 450 |
| Target (NFR-1–4) | ≤ 40 | | ≤ 80 | | ≤ 200 | ≤ 1 000 |

Shorter runs (10 s): 10 000 msg/s p50 25, p99 35, max 123; 50 000 msg/s
p50 19–20, p99 47–49, max 115–166; 100 000 msg/s p50 26, p99 40–41, max
118–130 (to ACCEPTED, µs).

- **NFR-1–4 are met on a 3-node cluster**, over loopback on one machine.
  Loopback has no NIC, switch or interrupt coalescing, so this
  understates the servers' hops; it does put every node's stages, the
  three storage stages and the generator on one 8-core machine.
- **A 2 ms stall at entry 65 536.** Before the fix every run had exactly
  one stall of about 2 ms (p99.99 1.0–1.4 ms, max 1.9–2.2 ms), at the
  same message count at every rate (1–2 s in at 50 000 msg/s, 2–3 s at
  25 000, inside the warm-up at 100 000). Raft keeps the last
  `retain_entries` (65 536) committed entries in memory in a `VecDeque`,
  which doubled and copied itself there on every node. The cluster shell
  now reserves room for twice `retain_entries` at start
  (`RaftLog::reserve`).
- **NFR-5 (1 M msg/s) is not met.** 150 000 msg/s is one generator
  thread and one ingress thread on this machine; 1 M needs several
  ingress threads or Tier B, and several generator cores (M4).
  *(Correction, E3: the limit was the storage stage's fsync per 128
  entries, not the generator or ingress. With group commit the same
  cluster sustains 250 000 msg/s in mode (a), where two generator
  threads saturate.)*
- **NFR-6 is met here too.** The failover example above, 100 trials,
  each node's stages on one core (`FAILOVER_CORES=2,3,4`, without the
  isolation): new messages on the feed p50 69.7 ms, p90 97.9 ms, max
  164.1 ms; new leader p50 68.5 ms; no failed messages.

## E3: Aeron Cluster on the same machine, 2026-11-17

The gap analysis's E3: Aeron Cluster (1.54.0, the aeron-io/benchmarks
suite's echo service and open-loop client) and KrustyCast, three nodes
each, on this machine (nuc2: 8 cores, 16 threads, 64 GB, one consumer
NVMe), isolated as in M3 (housekeeping on CPUs 0 and 8, the `performance`
governor). Loopback only; 64-byte payloads; open loop, latency from each
message's scheduled send time. Aeron's round trip (client, leader,
replication to a majority, the service, egress, client) is KrustyCast's
"to ACCEPTED". Harness: `bench/aeron-cluster/run.sh` and
`bench/krusty-cluster/run.sh` (each node its own process; the client in a
fourth); results and scripts under `/var/tmp/krusty-aeron-bench/` on nuc2.

**Layout** (CPU n and n+8 are one physical core). Both clusters have the
same CPUs, and **both clients get the same two, 7 and 15**:

| | Aeron Cluster | KrustyCast |
|---|---|---|
| Leader | driver receiver 1, sender 2; consensus 3, archive 4, service 12 | ingress 1, core 2, egress 3, replication 4, storage 12, recovery 9 |
| Follower | driver 5 or 6; consensus 13 or 14; archive 10 or 11; services share 9 | core 5 or 6, replication 13 or 14, the rest (and storage) 10 or 11 |
| Client | media driver 7, load client 15 | two sessions, 7 and 15 (the first also reads the feed) |
| Unpinned | each JVM's own threads (JIT, GC, conductors) on 0 and 8 | nothing |

**Matched durability**, paired by what a commit waits on (the middle row
is the closest pair, not an equal one):

| | Aeron Cluster | KrustyCast |
|---|---|---|
| No disk on the commit path | cluster and archive data on tmpfs | logs on tmpfs |
| Disk, no fsync before commit | archive on disk, `file.sync.level=0` (writes on the commit path) | mode (a) on disk (storage off the commit path) |
| fsync before commit | archive `file.sync.level=1` (fsync per recorded block) | mode (b): publish after the group's fsync, on two nodes |

### Results

10 minutes at 75 000 msg/s (µs):

| System, storage | p50 | p90 | p99 | p99.9 | p99.99 | max |
|---|---|---|---|---|---|---|
| Aeron, tmpfs | 41.0 | 59.2 | 66.7 | 78.8 | 94.5 | 4,202 |
| Aeron, disk, sync 0 | 45.4 | 59.9 | 67.8 | 167 | 9,675 | 20 ms |
| KrustyCast, tmpfs | 26.9 | 35.9 | 39.1 | 42.8 | 61.5 | 253 |
| KrustyCast, mode (a), disk | 25.8 | 35.1 | 39.9 | 45.2 | 52.3 | 422 |

60-second runs (10 s warm-up), µs; "Senders busy" is the share of each
KrustyCast client thread's time spent working (near 100%: the client, not
the cluster, sets the pace).

**Aeron, data on tmpfs**

| Offered | Achieved | p50 | p90 | p99 | p99.9 | p99.99 | max | Senders busy |
|---|---|---|---|---|---|---|---|---|
| 10k | as offered | 35.1 | 40.9 | 52.7 | 59.5 | 2,544 | 4,985 | |
| 50k | as offered | 40.9 | 47.8 | 66.0 | 71.4 | 122 | 3,783 | |
| 75k | as offered | 42.0 | 59.9 | 72.3 | 82.1 | 1,532 | 4,143 | |
| 100k | as offered | 45.6 | 58.8 | 70.5 | 77.4 | 1,417 | 3,797 | |
| 150k | as offered | 45.6 | 59.4 | 72.3 | 79.9 | 3,021 | 4,215 | |
| 200k | as offered | 45.9 | 61.0 | 71.8 | 84.6 | 3,273 | 4,354 | |
| 300k | as offered | 49.0 | 64.4 | 77.1 | 88.8 | 2,906 | 4,710 | |
| 400k | as offered | 50.8 | 65.6 | 78.2 | 108 | 2,929 | 3,822 | |
| 500k | as offered | 57.4 | 71.8 | 84.5 | 1,220 | 3,695 | 4,037 | |
| 750k | 587,011 (fell behind) | seconds | | | | | | |

**KrustyCast, logs on tmpfs**

| Offered | Achieved | p50 | p90 | p99 | p99.9 | p99.99 | max | Senders busy |
|---|---|---|---|---|---|---|---|---|
| 10k | 10,000 | 26.4 | 28.9 | 33.3 | 70.3 | 124 | 140 | 9%, 5% |
| 50k | 50,000 | 25.7 | 27.6 | 29.4 | 31.6 | 49.8 | 182 | 44%, 23% |
| 75k | 75,000 | 26.6 | 35.6 | 38.6 | 42.6 | 62.5 | 126 | 60%, 35% |
| 100k | 100,000 | 32.4 | 35.1 | 38.0 | 49.0 | 59.1 | 193 | 78%, 45% |
| 150k | 150,000 | 130 | 147 | 160 | 170 | 175 | 209 | 79%, 64% |
| 200k | 200,000 | 150 | 171 | 189 | 192 | 201 | 256 | 96%, 81% |
| 250k | 250,000 | 173 | 190 | 205 | 217 | 232 | 450 | 99%, 98% |
| 300k | 299,999 | 274 | 3,094 | 46 ms | 66 ms | 69 ms | 69 ms | 100%, 99% |

**Aeron, archive on disk, sync level 0**

| Offered | Achieved | p50 | p90 | p99 | p99.9 | p99.99 | max | Senders busy |
|---|---|---|---|---|---|---|---|---|
| 10k | as offered | 35.4 | 41.4 | 55.9 | 3,473 | 12 ms | 14 ms | |
| 50k | as offered | 40.8 | 48.1 | 66.2 | 87.9 | 14 ms | 19 ms | |
| 75k | as offered | 45.4 | 60.3 | 72.9 | 2,486 | 9,380 | 11 ms | |
| 100k | as offered | 45.9 | 60.6 | 71.0 | 80.8 | 5,444 | 10 ms | |
| 150k | as offered | 46.1 | 60.6 | 73.2 | 1,820 | 9,380 | 12 ms | |
| 200k | as offered | 47.0 | 62.7 | 76.5 | 2,556 | 9,839 | 12 ms | |
| 300k | as offered | 50.3 | 66.8 | 78.8 | 387 | 6,603 | 8,225 | |
| 400k | as offered | 52.9 | 68.8 | 83.5 | 7,426 | 13 ms | 15 ms | |
| 500k | as offered | 60.9 | 74.9 | 114 | 8,716 | 14 ms | 15 ms | |
| 750k | 583,969 (fell behind) | seconds | | | | | | |

**KrustyCast mode (a), on disk**

| Offered | Achieved | p50 | p90 | p99 | p99.9 | p99.99 | max | Senders busy |
|---|---|---|---|---|---|---|---|---|
| 10k | 10,000 | 28.0 | 31.6 | 34.8 | 37.4 | 46.0 | 66.7 | 9%, 5% |
| 50k | 50,000 | 25.4 | 28.1 | 32.0 | 35.1 | 50.4 | 106 | 44%, 24% |
| 75k | 75,000 | 25.9 | 35.2 | 40.4 | 45.7 | 93.6 | 187 | 61%, 35% |
| 100k | 100,000 | 30.5 | 36.2 | 49.2 | 54.3 | 101 | 492 | 77%, 44% |
| 150k | 150,000 | 49.9 | 88.2 | 104 | 144 | 187 | 476 | 92%, 66% |
| 200k | 200,000 | 75.2 | 111 | 140 | 179 | 213 | 283 | 98%, 85% |
| 250k | 250,000 | 143 | 175 | 197 | 220 | 264 | 584 | 99%, 98% |
| 300k | 286,850 (fell behind) | seconds | | | | | | |

**Aeron, archive sync level 1**

| Offered | Achieved | p50 | p90 | p99 | p99.9 | p99.99 | max | Senders busy |
|---|---|---|---|---|---|---|---|---|
| 10k | as offered | 659 | 1,043 | 1,622 | 4,817 | 7,078 | 9,839 | |
| 50k | as offered | 1,397 | 1,607 | 2,562 | 19 ms | 23 ms | 24 ms | |
| 75k | as offered | 1,370 | 1,594 | 1,837 | 3,570 | 4,415 | 7,434 | |
| 100k | as offered | 1,357 | 1,578 | 1,832 | 3,654 | 5,652 | 7,369 | |
| 150k | as offered | 1,388 | 1,586 | 1,826 | 3,688 | 4,764 | 5,931 | |
| 200k | as offered | 1,417 | 1,597 | 1,836 | 3,664 | 4,780 | 7,647 | |
| 300k | as offered | 1,427 | 1,607 | 1,985 | 3,721 | 4,119 | 6,877 | |
| 400k | as offered | 1,488 | 1,644 | 2,302 | 3,901 | 5,030 | 7,582 | |
| 500k | as offered | 1,521 | 1,686 | 2,421 | 3,955 | 5,530 | 7,873 | |

**KrustyCast mode (b), window 1024**

| Offered | Achieved | p50 | p90 | p99 | p99.9 | p99.99 | max | Senders busy |
|---|---|---|---|---|---|---|---|---|
| 10k | 10,000 | 635 | 1,020 | 1,428 | 3,551 | 5,636 | 6,898 | 12%, 8% |
| 50k | 49,999 | 1,705 | 2,063 | 3,299 | 7,463 | 23 ms | 24 ms | 46%, 38% |
| 75k | 74,998 | 1,790 | 2,148 | 4,231 | 27 ms | 35 ms | 37 ms | 64%, 56% |
| 100k | 99,967 | 1,863 | 2,238 | 21 ms | 32 ms | 34 ms | 35 ms | 79%, 71% |
| 150k (50 ms retries) | 149,995 | 1,974 | 2,370 | 5,952 | 23 ms | 31 ms | 33 ms | 99%, 97% |
| 250k (50 ms retries) | 249,367 | 25 ms | 257 ms | 323 ms | 324 ms | 325 ms | 325 ms | 99%, 99% |

Mode (b) above 100 k used `--retry-ms 50`: with a window of 1024 and
5 ms resends the generator spent itself on resends. Window 1024 because
2 sessions x 64 in flight over mode (b)'s ~1.8 ms commit caps throughput
near 70 k msg/s (Little's law); that needs `outcome_ring = 1024`.

### What limits throughput

| | Ceiling here | Set by |
|---|---|---|
| Aeron, no fsync | about 585 k msg/s | the cluster: at saturation the client's publication is back-pressured, its sender sits at the leader's flow-control limit, and the leader's log sender at its 32 MiB window (the followers) |
| Aeron, fsync per block | 500 k sustained (not pushed further) | |
| KrustyCast, tmpfs or mode (a) | 250-300 k msg/s | the clients: both sessions 98-100% busy. The cluster's own ceiling is higher and not measured; the client's cost is one datagram per message each way (Aeron's batches into the term buffer) |
| KrustyCast, mode (b) | at least 150 k at p50 2 ms | the clients again (97-99% busy) |

### Memory and disk

PSS (proportional set size: memory shared between processes counted
once), sampled each second; flat across rates for both:

| | Aeron Cluster | KrustyCast |
|---|---|---|
| Leader | driver 1.3 GB + node 1.4 GB | 129 MB |
| Each follower | 1.2 GB + 1.3 GB | 119 MB |
| Client | driver 1.2 GB + client 1.1 GB | 2 MB |
| Total | about 10 GB | about 370 MB |
| Of which | 8 x 1 GB pre-touched heap (our setting; the suite's is 4 GB); live heap a few MB; 1.3 GB of term buffers in /dev/shm | all allocated at start; `outcome_ring = 1024` (mode b runs) raises a node to about 240 MB |
| Disk per message per node | about 122 B (archive) | about 155 B (log, egress index) |

Aeron's floor with small heaps would be about 2 GB, mostly term buffers,
which its term length sets.

### Findings

**KrustyCast at equal footing is faster at every percentile and uses
370 MB against Aeron's 10 GB as configured (about 2 GB at Aeron's floor);
Aeron carries about twice the throughput per client CPU.** Without disk, 10 minutes at 75 k: p50 27 against 41 µs, p99
39 against 67, p99.99 62 against 95, max 0.25 against 4.2 ms. With disk,
KrustyCast's mode (a) keeps the disk off the commit path and its tail at
52 µs; Aeron's archive writes are on its path and its p99.99 is 9.7 ms.
With an fsync before every commit, p50 is 1.4 ms (Aeron) and 1.8-2.0 ms
(KrustyCast). Aeron's
client and protocol batch (several messages per datagram, flow control in
bytes); KrustyCast sends one datagram per message each way, so two client
CPUs saturate at 250-300 k: batching submits and feedback is the way to
the PRD's 1 M msg/s (NFR-5), not the cluster.

**What the comparison found in KrustyCast** (each fixed, with tests):
1. *The storage stage fsynced every 128 entries* (256 ring records, two
   per message): about 160 k msg/s on this NVMe, the cluster's whole
   throughput ceiling (M1 and M3 blamed the generator). Group commit now
   drains what has queued up, so slower fsyncs make larger groups. Mode
   (a) on disk: 250 k sustained, from 165 k.
2. *Creating the next segment stalled storage 50-130 ms* (preallocation
   on tmpfs; two device flushes on disk), the core waited on the full ring
   behind it, both followers' Raft threads stopped at the same log index
   and the leader lost its term: every run at 300 k on tmpfs. Next files
   are now prepared ahead on helper threads; a roll writes a header and
   renames. 0 of 6 runs stall.
3. *Raft stamped messages with the time of its last tick*, so a node that
   had itself been held up saw its peers as silent (`advance_clock`).
4. *Under overload the core waited on a full ring and stopped ticking*
   (157 ms in mode b at 250 k offered; the leader lost its term). The core
   now takes submits only while its output rings have room (admission
   control; ingress answers BUSY).
5. *Resends were logged again while their original was being committed*:
   when commits are slower than the retry interval, 40 log entries per
   message and goodput about 9 k msg/s. The leader now drops them (keyed
   by message since 2026-11-19: `Cluster.tla` showed the first,
   per-session filter let resends through after a NAK, and its first fix
   could starve a lower seq; spec/README.md). The
   nightly simulation hit the same bug that morning from the other side
   (`cluster3-harsh`: network delays near the retry interval, 5 of 1000
   seeds livelocked); `krusty-sim`'s cluster shell has the same filter
   now, and 4 x 8000 cluster seeds are clean.
6. Metrics to see these: `cluster_loop_max_ns` (how long Raft went
   without a tick), `raft_pre_votes`, `raft_quorum_lost`,
   `submits_deferred`, `resends_filtered`.

**What it took to run Aeron Cluster well here** (for whoever repeats it):
a SHARED media driver on the leader cannot drain its consensus socket and
loopback drops the followers' datagrams (NAK storms, 100-300 ms medians);
the leader needs a DEDICATED driver; the drivers read their properties
files over `-D` options; every spinning agent needs a CPU of its own; the
archive's catalog must sync at least as much as its files. Turning off
periodic safepoints did not change its tail; the disk did.

**Caveats.** One host, loopback: no NIC, switch or interrupt coalescing.
Three nodes share one consumer NVMe, which serializes their flushes (mode
b, Aeron sync level 1). We configured Aeron, not its authors; its tail
below 4 ms on tmpfs (a few ms at p99.99 in some 60-second runs) is not
explained. The services differ: Aeron's echoes, KrustyCast's sequences
(deduplication, conditional appends, a published feed). KrustyCast's
throughput figures are the clients' limits, so the cluster's are higher.

### After batching (2026-11-21)

Several messages per datagram, packed as Aeron does (d96edd5:
docs/ARCHITECTURE.md §8.1). Same layout, isolation, two senders on 7 and
15, and 60 s runs as above, with Aeron's MTU (1408) on the nodes and the
senders (`sweep11.sh`, results in `results-e3/batching/`). In and out
are messages per datagram on the leader (`ingress_datagrams`,
`feedback_datagrams`).

**KrustyCast, logs on tmpfs** (pairs with Aeron on tmpfs)

| Offered | Achieved | p50 | p90 | p99 | p99.9 | p99.99 | max | In | Out | Senders busy |
|---|---|---|---|---|---|---|---|---|---|---|
| 100k | 100,000 | 33.2 | 36.1 | 51.7 | 560 | 14 ms | 19 ms | 1.00 | 1.01 | 78%, 44% |
| 300k | 300,000 | 135 | 164 | 183 | 195 | 205 | 259 | 1.20 | 8.6 | 99%, 98% |
| 400k | 399,999 | 130 | 159 | 180 | 193 | 205 | 661 | 1.68 | 10.9 | 99%, 98% |
| 500k | 499,999 | 123 | 156 | 180 | 196 | 208 | 445 | 2.18 | 11.8 | 99%, 98% |
| 600k | 599,999 | 116 | 153 | 180 | 199 | 222 | 1,255 | 2.78 | 11.8 | 99%, 98% |
| 750k | 749,999 | 102 | 136 | 169 | 195 | 977 | 1,585 | 3.83 | 10.4 | 99%, 98% |
| 1M | 898,837 (fell behind; STALLED) | seconds | | | | | | | | 71%, 58% |

**KrustyCast, mode (a), disk** (pairs with Aeron, archive sync level 0)

| Offered | Achieved | p50 | p90 | p99 | p99.9 | p99.99 | max | In | Out | Senders busy |
|---|---|---|---|---|---|---|---|---|---|---|
| 100k | 100,000 | 31.2 | 37.4 | 50.2 | 257 | 587 | 817 | 1.00 | 1.00 | 77%, 44% |
| 300k | 300,000 | 103 | 142 | 177 | 199 | 222 | 354 | 1.24 | 5.4 | 99%, 98% |
| 400k | 400,000 | 94.8 | 147 | 183 | 209 | 231 | 288 | 1.73 | 5.9 | 99%, 98% |
| 500k | 500,000 | 85.2 | 150 | 189 | 216 | 241 | 289 | 2.34 | 5.9 | 99%, 99% |
| 600k | 599,999 | 84.6 | 155 | 197 | 222 | 246 | 300 | 2.92 | 6.6 | 99%, 99% |
| 750k | 749,998 | 93.8 | 160 | 209 | 3,707 | 6,312 | 7,078 | 4.02 | 8.4 | 97%, 96% |
| 1M | 914,809 (fell behind) | seconds | | | | | | | | 74%, 55% |

**Against Aeron** (its tables above):
- **Throughput:** KrustyCast now sustains 750k msg/s with either kind of
  storage. Aeron fell behind at 750k (about 585k), and before batching
  KrustyCast's limit was the generator, at 250–300k.
- **Latency, 300k to 600k:** worse than Aeron in the body. For example,
  at 500k the p50 is 85–123 µs against Aeron's 57–61, and the p99 is
  180–189 µs against 85–114. It is better in the tail: p99.99 and max
  stay under 0.7 ms through 500k, against Aeron's 3.7–14 ms.
- **Not yet shown to be the cluster's.** The knee is where it was before
  batching (p50 31–33 µs at 100k, then a flat 85–135 µs plateau from 300k
  to 750k). Both senders are 96–99% busy from 300k on. The leader's
  stages do not queue: at 500k, mode (a), ring A takes 0.5 µs at p50
  (16 µs at p99), ring O 8 µs (33 µs), and a Raft loop pass 2 µs (33 µs);
  followers' passes are about 1 µs. What no counter covers yet is propose
  to commit, the replication round trip. KrustyCast's generator also does
  work that Aeron's client does not: sender 0 reads the whole feed
  (`--combined`), every message, to measure feed latency, while Aeron's
  client receives only its echoes. Few SUBMITs are packed at these rates
  (1.2–4 per datagram), because each sender loop pass finds few new
  messages. Outcomes pack 5–12 per datagram.

### One load generator for both (2026-11-22)

To rule the generator out, aeron-io/benchmarks' LoadTestRig now drives
both clusters (`bench/aeron-rig-krusty`: a KrustyCast transceiver, its I/O
thread on 7 as Aeron's client's media driver, the rig on 15). Both ran in
one session, interleaved by rate, MTU 1408 on both (`sweep12.sh`,
`results-e3/one-rig/`). p50 / p99 / p99.99, µs:

| Offered | Aeron, tmpfs | KrustyCast, tmpfs | Aeron, disk, sync 0 | KrustyCast, mode (a), disk |
|---|---|---|---|---|
| 100k | 45.6 / 70.7 / 2,879 | 31.0 / 40.4 / 55.2 | 46.0 / 71.7 / 7,922 | 29.6 / 45.4 / 79.4 |
| 300k | 48.9 / 76.7 / 2,091 | 145 / 206 / 237 | 49.9 / 78.7 / 9,003 | 110 / 186 / 858 |
| 400k | 51.4 / 77.8 / 1,896 | 146 / 202 / 5,775 | 53.2 / 81.3 / 9,822 | 110 / 196 / 1,393 |
| 500k | 58.0 / 86.9 / 3,588 | 144 / 201 / 1,441 | 60.5 / 102 / 13 ms | 111 / 212 / 23 ms |
| 600k | fell behind | 142 / 202 / 8,659 | fell behind | 110 / 218 / 1,106 |
| 750k | fell behind | 131 / 211 / 285 | fell behind | 116 / 241 / 4,862 |

The generator was not it: under the same rig KrustyCast's p50 still steps
from about 30 µs to a 110–145 µs plateau between 100k and 300k. Outcomes
leave 17–20 to a datagram there, so commits come in bursts.

**The cause: Appends in flight.** The leader sends an Append on nearly
every loop pass and lets up to `max_inflight` (16) be in flight to each
follower. The p50 grows in proportion to that limit, about 7.7 µs per
Append at 300k and at 500k (`sweep13.sh`, logs on tmpfs). The window
stays full, and the latency is the wait behind it: the replication path
handles about one small Append per 7.7 µs.

| max_inflight | 300k p50 / p99 / p99.9 | 500k p50 / p99 / p99.9 | 750k p50 / p99 / p99.9 |
|---|---|---|---|
| 1 | 43.9 / 61.2 / 1,201 | 50.8 / 73.2 / 84.7 | 105 / 153 / 235 |
| 2 | 52.1 / 79.1 / 92.7 | 1,150 / 2,198 / 2,398 (see below) | 75.1 / 155 / 686 |
| 4 | 56.5 / 77.6 / 90.9 | 64.0 / 94.6 / 761 | 76.5 / 126 / 412 |
| 16 (default) | 142 / 208 / 471 | 145 / 202 / 223 | |
| 64 | 505 / 577 / 595 | 507 / 578 / 603 | |
| 256 | 1,965 / 2,072 / 2,120 | 1,972 / 2,078 / 2,142 | |

With one to four Appends in flight, fewer and larger Appends, KrustyCast
matches or beats Aeron's p50 and p99 at 300k and 500k, and still sustains
750k. Provisional: these are single runs, the tails (p99.99, max) vary
from run to run on both systems, and 500k with 2 in flight stayed in the
queued state for the whole run, with no leader change. Repeat runs, and a
replication design that bounds Appends by what they carry rather than by
count, come before any default changes. `cluster.max_inflight` is now a
node setting.

### Repeats, the feed sink, and where the time goes (2026-11-22)

**Repeats** (`sweep14.sh`, three runs per setting, interleaved): medians
and p99s repeat to within a few percent (for example 43.6–44.7 µs p50 at
300k with 1 in flight). p99.99 and max vary from run to run by orders of
magnitude (70 µs to 17 ms), on both systems.

**A harness artifact.** With the rig, nothing read KrustyCast's feed, and
every frame sent to the closed port made the kernel generate an ICMP
port-unreachable on the leader's egress thread (895,329 for 895,314
frames in a 5 s check). A multicast feed never pays that. `run.sh` now
holds the port (`RIG=1`), and the frames drop at a full socket buffer. The
"stuck" state (500k, 2 in flight: p50 about 1 ms in all three repeats)
was this: egress could not keep up with 146k small frames a second plus
ICMP, and outcomes queued about 1 ms in ring O. Without the ICMP the same
run gives 49 µs. The rig results above for KrustyCast carry this cost.
The 16-in-flight plateau does not come from it: there, outcomes wait 16 µs
in ring O.

**Where the time goes** (new histograms, metrics layout 11:
`propose_to_apply`, `append_rtt`, `peer_to_core`, `peer_handling`):
- Followers are not the limit. An Append waits under 1 µs at p50 in
  their ring R, and their core handles one in 0.5–2 µs.
- The leader's replication I/O thread is. With 16 in flight it handles
  about 250k datagrams a second each way at every rate (Appends out,
  replies in, for two followers): about 125k Appends a second per
  follower, the 7.7 µs per Append in the latency slope. `append_rtt` and
  `propose_to_apply` are 131 µs at p50 there, and 33–65 µs with 1 or 2 in
  flight.

**With the sink** (`sweep15.sh`, logs on tmpfs), p50 / p99 / p99.9, µs:

| Offered | Aeron (`sweep12.sh`) | KrustyCast, 1 in flight | 2 in flight | 4 in flight | 16 (default) |
|---|---|---|---|---|---|
| 300k | 48.9 / 76.7 / 88.1 | 38.1 / 53.2 / 78.0 | 44.3 / 63.4 / 3,168 | 56.8 / 76.0 / 92.2 | 146 / 206 / 219 |
| 500k | 58.0 / 86.9 / 2,148 | 57.7 / 79.5 / 93.4 | 49.2 / 70.4 / 79.6 | 61.5 / 87.9 / 108 | 143 / 201 / 316 |
| 750k | fell behind | 115 / 169 / 204 | 76.7 / 116 / 543 | 74.4 / 129 / 652 | 124 / 209 / 468 |

With 2 in flight KrustyCast is ahead of Aeron at 300k and 500k in the
p50, p99 and p99.9, and it sustains 750k, which Aeron does not. One in
flight is best at 300k but falls behind at 750k, where each message waits
a full round trip for the Append ahead of it.

## Next

- Run the same tool on the reference servers (real NICs, multicast, isolated
  cores) and record the results here, next to the laptop numbers; for M3,
  `krusty-load --cluster 3` across three machines (the in-process mode
  needs a multi-host variant) and the failover example.
- E3 again on the reference servers, and KrustyCast with batched submits
  and feedback (its clients set its throughput here).
- A perf smoke test in CI (a short low-rate run that must not lose messages)
  is in `ci.yml`. It fails on loss and on generator lag, not on latency,
  because shared CI runners are too noisy for latency.
