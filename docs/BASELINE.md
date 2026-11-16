# M0 baseline: today's single node on a laptop

Date: 2026-11-14. Tool: `src/bin/loadgen.rs` (GAP-ANALYSIS E1).
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
200 000 achieves 167 000. The PRD measures at half the maximum, so
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
- **NFR-6 is met here too.** The failover example above, 100 trials,
  each node's stages on one core (`FAILOVER_CORES=2,3,4`, without the
  isolation): new messages on the feed p50 69.7 ms, p90 97.9 ms, max
  164.1 ms; new leader p50 68.5 ms; no failed messages.

## Next

- Run the same tool on the reference servers (real NICs, multicast, isolated
  cores) and record the results here, next to the laptop numbers; for M3,
  `krusty-load --cluster 3` across three machines (the in-process mode
  needs a multi-host variant) and the failover example.
- Aeron Cluster baseline on the same hardware (E3).
- A perf smoke test in CI (a short low-rate run that must not lose messages)
  is in `ci.yml`. It fails on loss and on generator lag, not on latency,
  because shared CI runners are too noisy for latency.
