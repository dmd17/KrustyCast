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

## Next

- Run the same tool on the reference servers (real NICs, multicast, isolated
  cores) and record the results here, next to the laptop numbers.
- Aeron Cluster baseline on the same hardware (E3).
- A perf smoke test in CI (a short low-rate run that must not lose messages)
  is in `ci.yml`. It fails on loss and on generator lag, not on latency,
  because shared CI runners are too noisy for latency.
