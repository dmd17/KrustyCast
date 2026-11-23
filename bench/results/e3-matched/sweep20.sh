#!/bin/bash
# E3 part 17: KrustyCast after write-before-ack in mode (a) (2026-11-23), same layout as sweep19.sh; Aeron unchanged (sweep19).
set -u
B=/var/tmp/krusty-aeron-bench; R=$B/results-e3/${OUT:-rust-rig-writeack}; ISO=$B/iso
K=/home/dmd/KrustyCast/bench/krusty-cluster/run.sh
mkdir -p $R
trap "$ISO/restore.sh" EXIT
trap "exit 1" TERM INT
$ISO/isolate.sh
run() {
  local name=$1; shift
  local env=(--setenv=HOME=$HOME)
  while [ "$1" != "--" ]; do env+=("--setenv=$1"); shift; done; shift
  echo "$(date +%s) $(date +%T) $name" | tee -a $R/runs.log
  sudo systemd-run --quiet --scope --slice=bench.slice -p AllowedCPUs=0-15 --uid=dmd --gid=dmd \
    "${env[@]}" timeout 400 "$@" > $R/$name.txt 2>&1
  echo "$(date +%s) $(date +%T) end" >> $R/runs.log
  sleep 3
}
for r in ${RATES:-100000 300000 500000 750000}; do
  for m in ${MODES:-none async}; do
    c=$B/cfg-batch-tmpfs; [ $m = async ] && c=$B/cfg-batch-disk
    run k-$m-$r RIG=1 CONF=$c -- $K $r 60 10
  done
done
echo "$(date +%T) done"
