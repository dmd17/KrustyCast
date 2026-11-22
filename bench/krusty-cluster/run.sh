#!/bin/bash
# KrustyCast cluster baseline (gap E3): ./run.sh RATE SECONDS [WARMUP_SECONDS]
#
# Three krusty-node processes (node1-3.toml) and krusty-load in a fourth,
# on one host over loopback: the process structure of bench/aeron-cluster,
# with the thread layout of `krusty-load --cluster 3` (docs/BASELINE.md):
#
#   leader     ingress 1, core 2, egress 3, replication 4, storage 12,
#              recovery 9 (idle, but spins)
#   follower 2 core 5, replication 13, everything else 10
#   follower 3 core 6, replication 14, everything else 11
#   load       one sender that also reads the feed, on 7
#
# Each process starts on the housekeeping CPUs (HOUSE, default 0,8), as in
# bench/aeron-cluster; the stages then pin themselves.
# Node 1 leads: if another node wins the first election, run.sh hands
# leadership to it (krusty-admin transfer) and checks.
# Latency is krusty-load's, from each message's scheduled send time to the
# feed and to ACCEPTED (= Aeron's echo round trip).
#
# RIG=1: the client is the Rust rig (bench/rig, `e3-rig krusty`), the same
# load generator bench/aeron-cluster uses for Aeron (no JVM on either client
# side): the rig thread on 15, its I/O thread on 7 (where Aeron's client has
# its media driver). Latency is the rig's: scheduled send time to ACCEPTED.
# RIG_OPTS: more options (--window, --mtu, --retry-ms).
set -euo pipefail
RATE=$1; SECS=$2; WARM=${3:-10}
HERE=$(cd "$(dirname "$0")" && pwd)
HOUSE=${HOUSE:-0,8}
BIN=${KRUSTY_BIN:-$HERE/../../target/release}
LOGS=/var/tmp/krusty-aeron-bench/logs
# CONF: the directory holding node1-3.toml (default: this one).
CONF=${CONF:-$HERE}
rm -rf /var/tmp/krusty-aeron-bench/krusty-node1 /var/tmp/krusty-aeron-bench/krusty-node2 \
  /var/tmp/krusty-aeron-bench/krusty-node3 /dev/shm/kb-krusty-log1 /dev/shm/kb-krusty-log2 \
  /dev/shm/kb-krusty-log3
mkdir -p $LOGS
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap 'cleanup; rm -rf /dev/shm/kb-krusty-log1 /dev/shm/kb-krusty-log2 /dev/shm/kb-krusty-log3' EXIT

for n in 1 2 3; do
  taskset -c "$HOUSE" "$BIN/krusty-node" --config "$CONF/node$n.toml" > "$LOGS/krusty-node$n.out" 2>&1 &
  pids+=($!)
done
admin="--node 127.0.0.1:27201 --node 127.0.0.1:27202 --node 127.0.0.1:27203"
for i in $(seq 100); do
  st=$("$BIN/krusty-admin" $admin status 2>/dev/null) && grep -q leader <<<"$st" && break
  sleep 0.1
done
# Node 1 must lead (its row is the first): if another node won the first
# election (start-up can outweigh the shorter timeouts), hand it over.
if ! sed -n 2p <<<"$st" | grep -q leader; then
  "$BIN/krusty-admin" $admin transfer 1
  for i in $(seq 50); do
    st=$("$BIN/krusty-admin" $admin status 2>/dev/null) && sed -n 2p <<<"$st" | grep -q leader && break
    sleep 0.1
  done
fi
echo "$st"
sed -n 2p <<<"$st" | grep -q leader || { echo "run.sh: node 1 is not the leader" >&2; exit 1; }

# Kernel drops per UDP socket of ours (last column of /proc/net/udp), by port.
drops() { awk 'NR > 1 && $2 ~ /^0100007F:(69|6A|6B|6C)/ && $NF > 0 {
  split($2, a, ":"); printf " %d:%s", strtonum("0x" a[2]), $NF }' /proc/net/udp; echo; }
echo "udp drops before:$(drops)"
# LOAD (default: one sender on 7 that also reads the feed); e.g. two
# sessions on one physical core: LOAD="--senders 2 --combined --pin-senders 7,15".
if [ -n "${RIG:-}" ]; then
  # Nothing reads the feed (Aeron's echo benchmark has none). Hold its port
  # so frames are dropped at a full socket buffer: sent to a closed port,
  # each one made the kernel generate an ICMP port-unreachable on the
  # egress thread (one per frame, 2026-11-22), which a multicast feed never
  # costs. The drops show in "udp drops after" (port 27900).
  taskset -c "$HOUSE" python3 -c 'import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", 27900))
time.sleep(1e9)' &
  pids+=($!)
  # The Rust rig (bench/rig): the session on 15, its I/O thread on 7 (where
  # Aeron's client has its media driver).
  RIGBIN=${RIG_BIN:-$HERE/../rig/target/release/e3-rig}
  taskset -c "$HOUSE" "$RIGBIN" krusty --rate "$RATE" --duration "$SECS" --warmup "$WARM" \
    --rig-cpu 15 --io-cpu 7 --nodes 127.0.0.1:27001,127.0.0.1:27002,127.0.0.1:27003 ${RIG_OPTS:-} &
  client=$!
  pids+=($client)
  wait $client && rc=0 || rc=$?
else
  taskset -c "$HOUSE" "$BIN/krusty-load" --connect 127.0.0.1:27001 --feed-bind 127.0.0.1:27900 \
    --rate "$RATE" --duration "$SECS" --warmup "$WARM" --spin ${LOAD:---combined --pin-senders 7} &
  pids+=($!)   # stopped with the nodes if this script is
  wait $! && rc=0 || rc=$?
fi
echo "udp drops after:$(drops)"
for n in 1 2 3; do "$BIN/krusty-stat" --file /dev/shm/kb-krusty-node$n.stats; done
exit $rc
