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
# RIG=1: the client is aeron-io/benchmarks' LoadTestRig instead, the one
# that measures Aeron in bench/aeron-cluster, with the KrustyCast transceiver
# (bench/aeron-rig-krusty, built by its build.sh). Its JVM is started and
# pinned as Aeron's client: the rig thread on 15, and the transceiver's I/O
# thread (Aeron's client has its media driver there) on 7. Latency is then
# the rig's: scheduled send time to ACCEPTED. RIG_OPTS: more -D options
# (krusty.window, krusty.mtu, krusty.retry.ms).
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
  AB=${AERON_BENCH:-$HOME/.local/share/krusty-tools/aeron-benchmarks}
  export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
  RIGJAR=${RIGJAR:-/var/tmp/krusty-aeron-bench/krusty-rig/krusty-rig.jar}
  # (The launcher takes the last -cp: the suite's jars plus ours.)
  CP="$AB/benchmarks-all/build/libs/benchmarks.jar:$RIGJAR"
  OPTS="-Xms1G -Xmx1G -XX:GuaranteedSafepointInterval=0 -cp $CP \
 -Dio.aeron.benchmarks.message.transceiver=io.krustycast.bench.KrustyMessageTransceiver \
 -Dkrusty.nodes=127.0.0.1:27001,127.0.0.1:27002,127.0.0.1:27003 \
 -Dio.aeron.benchmarks.message.rate=$RATE -Dio.aeron.benchmarks.warmup.message.rate=$RATE \
 -Dio.aeron.benchmarks.iterations=$SECS -Dio.aeron.benchmarks.warmup.iterations=$WARM \
 -Dio.aeron.benchmarks.output.file=krusty-rig-$RATE ${RIG_OPTS:-}"
  PROCESS_FILE_NAME=kb-krusty-rig JVM_OPTS="$OPTS" taskset -c "$HOUSE" "$AB/scripts/run-java" \
    io.aeron.benchmarks.LoadTestRig "$AB/scripts/aeron/low-latency-driver.properties" \
    "$HERE/../aeron-cluster/cluster.properties" &
  client=$!
  pids+=($client)
  pin() { # thread-name-prefix cpu: waits up to 30 s for the thread
    local i t
    for i in $(seq 300); do
      for t in /proc/$client/task/*; do
        if [[ $(cat "$t/comm" 2>/dev/null) == "$1"* ]]; then
          taskset -pc "$2" "$(basename "$t")" > /dev/null; return 0
        fi
      done
      sleep 0.1
    done
    echo "run.sh: no thread $1 in the rig" >&2; return 1
  }
  pin krusty-io 7; pin load-test-rig 15
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
