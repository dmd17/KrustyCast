#!/bin/bash
# E3 after issue #1 (2026-11-24): sweep19.sh with a sync before each run, and its own results directory; Aeron and KrustyCast interleaved in one session.
# drives Aeron Cluster (its own transceiver) and KrustyCast (the KrustyCast
# transceiver, bench/aeron-rig-krusty), each client JVM pinned the same way
# (rig thread 15; Aeron's media driver / KrustyCast's I/O thread 7), same
# session, same isolation. MTU 1408 on both.
#   none:  Aeron on tmpfs            vs KrustyCast logs on tmpfs
#   async: Aeron archive sync 0      vs KrustyCast mode (a) on disk
set -u
B=/var/tmp/krusty-aeron-bench; R=$B/results-e3/issue1-full; ISO=$B/iso
K=/home/dmd/KrustyCast/bench/krusty-cluster/run.sh; A=/home/dmd/KrustyCast/bench/aeron-cluster/run.sh
mkdir -p $R
trap "$ISO/restore.sh" EXIT
trap "exit 1" TERM INT  # so that the EXIT trap runs
$ISO/isolate.sh
run() { # name env-assignments... -- cmd...
  local name=$1; shift
  local env=(--setenv=HOME=$HOME)
  while [ "$1" != "--" ]; do env+=("--setenv=$1"); shift; done; shift
  # Each run starts with a quiet disk: a previous run's dirty data (Aeron's
  # archive above all) being written back stalls the next run's writes
  # behind journal commits (2026-11-24, issue #1).
  sync
  echo "$(date +%s) $(date +%T) $name" | tee -a $R/runs.log
  sudo systemd-run --quiet --scope --slice=bench.slice -p AllowedCPUs=0-15 --uid=dmd --gid=dmd \
    "${env[@]}" timeout 400 "$@" > $R/$name.txt 2>&1
  echo "$(date +%s) $(date +%T) end" >> $R/runs.log
  mkdir -p $R/$name-gc; cp /dev/shm/kb-*-gc.log $R/$name-gc/ 2>/dev/null; rm -f /dev/shm/kb-*-gc.log
  sleep 3
}
for r in 100000 300000 500000 750000; do
  run a-none-$r TMPFS=1 -- $A $r 60 10
  run k-none-$r RIG=1 CONF=$B/cfg-batch-tmpfs -- $K $r 60 10
  run a-async-$r X=0 -- $A $r 60 10
  run k-async-$r RIG=1 CONF=$B/cfg-batch-disk -- $K $r 60 10
done
echo "$(date +%T) done"
