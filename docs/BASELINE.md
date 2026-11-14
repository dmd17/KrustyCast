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
- **Mode (b) misses NFR-9 as written** (p99 ≤ device fsync p99 + 30 µs,
  about 1 ms here): p99 is 2.1–2.6 ms. With one group commit at a time, a
  message that arrives just after an fsync started waits for it and then
  for its own, so latency is 1–2 fsyncs (mean ~1.5×, tail ~2×). An
  experiment with 2–4 overlapping `fdatasync` threads did not help on this
  drive (it serializes cache flushes). NFR-9 needs either revising (e.g.
  ≤ 2 × fsync p99 + 30 µs) or a qualifier: drives with power-loss
  protection, where an fsync costs tens of µs. A PRD decision.
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

## Next

- Run the same tool on the reference servers (real NICs, multicast, isolated
  cores) and record the results here, next to the laptop numbers.
- Aeron Cluster baseline on the same hardware (E3).
- A perf smoke test in CI (a short low-rate run that must not lose messages)
  is in `ci.yml`. It fails on loss and on generator lag, not on latency,
  because shared CI runners are too noisy for latency.
