#!/bin/bash
# Aeron Cluster baseline (gap E3): ./run.sh RATE SECONDS [WARMUP_SECONDS]
#
# Three Aeron Cluster nodes running the benchmark suite's echo service, and
# its open-loop load client (LoadTestRig), on one host over loopback.
#
# Node 0 is the appointed leader. Thread affinity, as Aeron is deployed for
# low latency: every JVM starts on the housekeeping CPUs (HOUSE, default
# 0,8), then each spinning agent thread moves to a CPU of its own (CPU n and
# n+8 are one physical core), mirroring krusty-load's cluster layout:
#
#   leader     driver receiver 1, sender 2 (DEDICATED; conductor on HOUSE),
#              consensus module 3, archive 4, echo service 12
#   follower 1 driver 5, consensus 13, archive 10
#   follower 2 driver 6, consensus 14, archive 11
#   both followers' services share 9 (off the latency path)
#   client     driver 7, load generator 15 (no JVM: Aeron's C media driver,
#              aeronmd, and the Rust rig bench/rig, e3-rig aeron)
#
# Why (measured 2026-11-17, docs/BASELINE.md): with a single shared driver
# thread on the leader, or with the JVMs' own threads (JIT, GC, client
# conductor) free to preempt the spinning agents, the leader stops draining
# its consensus socket, loopback drops the followers' small datagrams, and
# NAK/retransmit cycles put 100-300 ms on the median.
#
# Needs aeron-io/benchmarks built (./gradlew deployTar) at $AERON_BENCH.
# Latency is the round trip: client -> leader -> replicated to a majority
# and committed -> echo service -> egress -> client, from each message's
# scheduled send time. EXTRA_PROPS names a properties file loaded last by
# every process (the media driver ignores -D options that its files set).
set -euo pipefail
RATE=$1; SECS=$2; WARM=${3:-10}
HERE=$(cd "$(dirname "$0")" && pwd)
B=${AERON_BENCH:-$HOME/.local/share/krusty-tools/aeron-benchmarks}/scripts/aeron
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
HOUSE=${HOUSE:-0,8}
# The suite's scripts pre-touch a 4 GB heap; 1 GB is ample (the live set is a
# few MB). No periodic safepoints: with every JVM's housekeeping threads
# sharing two CPUs, reaching one took up to 19 ms, stopping the agents.
JVM_BASE="-Xms1G -Xmx1G -XX:GuaranteedSafepointInterval=0 ${JVM_EXTRA:-}"
X=${EXTRA_PROPS:-}
LOGS=/var/tmp/krusty-aeron-bench/logs
# Cluster and archive data on disk, or with TMPFS=1 in /dev/shm (no disk on
# the commit path, the counterpart of KrustyCast's logs on tmpfs).
DATA=/var/tmp/krusty-aeron-bench
[ -n "${TMPFS:-}" ] && DATA=/dev/shm/kb-aeron-data
rm -rf /var/tmp/krusty-aeron-bench/node0 /var/tmp/krusty-aeron-bench/node1 /var/tmp/krusty-aeron-bench/node2 \
  /dev/shm/kb-aeron-data
mkdir -p $LOGS /var/tmp/krusty-aeron-bench/results
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap 'cleanup; rm -rf /dev/shm/kb-aeron-data' EXIT

start() { # name script props... (JVM options in OPTS)
  local name=$1 script=$2; shift 2
  PROCESS_FILE_NAME=kb-$name JVM_OPTS="$JVM_BASE ${OPTS:-}" taskset -c "$HOUSE" "$B/$script" "$@" $X \
    > "$LOGS/$name.out" 2>&1 &
  pids+=($!)
}

pin() { # pid thread-name-prefix cpu: waits up to 30 s for the thread
  local pid=$1 name=$2 cpu=$3 i t
  [ -n "${NOPIN:-}" ] && return 0
  for i in $(seq 300); do
    for t in /proc/$pid/task/*; do
      if [[ $(cat "$t/comm" 2>/dev/null) == "$name"* ]]; then
        taskset -pc "$cpu" "$(basename "$t")" > /dev/null; return 0
      fi
    done
    sleep 0.1
  done
  echo "run.sh: no thread $name in $pid" >&2; return 1
}

C=$HERE/cluster.properties
start md0 media-driver $C $HERE/node0.properties
start md1 media-driver $C $HERE/node1.properties
start md2 media-driver $C $HERE/node2.properties
# The client's driver: Aeron's C media driver (SHARED: its one agent runs
# on the main thread), with the files the Java client driver read.
AERONMD=${AERONMD:-$HOME/.local/share/krusty-tools/aeron-c-src/aeron-1.54.0/build/binaries/aeronmd}
taskset -c 7 "$AERONMD" "$B/low-latency-driver.properties" $C $HERE/client.properties $X \
  > "$LOGS/mdc.out" 2>&1 &
pids+=($!)
for n in 0 1 2; do
  D=$DATA/node$n
  OPTS="-Daeron.cluster.dir=$D/cluster -Daeron.archive.dir=$D/archive" \
    start node$n cluster-node $C $HERE/node$n.properties
done
# A shared driver's agent thread is named after its Aeron directory.
pin ${pids[0]} receiver 1; pin ${pids[0]} sender 2
pin ${pids[1]} /dev/shm/kb 5; pin ${pids[2]} /dev/shm/kb 6
pin ${pids[4]} consensus-modul 3;  pin ${pids[4]} archive-conduct 4;  pin ${pids[4]} clustered-servi 12
pin ${pids[5]} consensus-modul 13; pin ${pids[5]} archive-conduct 10; pin ${pids[5]} clustered-servi 9
pin ${pids[6]} consensus-modul 14; pin ${pids[6]} archive-conduct 11; pin ${pids[6]} clustered-servi 9
sleep 5   # leader elected

# The load generator: the Rust rig (bench/rig, built with its cargo), with
# the Java LoadTestRig's method; its C client's conductor stays on HOUSE,
# the rig thread moves to 15.
RIG=${RIG_BIN:-$HERE/../rig/target/release/e3-rig}
taskset -c "$HOUSE" "$RIG" aeron --rate "$RATE" --duration "$SECS" --warmup "$WARM" \
  --rig-cpu 15 --aeron-dir /dev/shm/kb-aeron-client \
  --ingress 'aeron:udp?endpoint=localhost:20000' --egress 'aeron:udp?endpoint=localhost:24000' &
client=$!
pids+=($client)
wait $client
