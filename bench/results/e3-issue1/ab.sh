#!/bin/bash
# Same-session A/B for issue #1: base build (e5bc2f8) vs the working tree, alternating per run.
set -u
B=/var/tmp/krusty-aeron-bench; ISO=$B/iso
K=/home/dmd/KrustyCast/bench/krusty-cluster/run.sh
WBIN=/home/dmd/.claude/jobs/cb899b1e/tmp/base-wt/target/release
RA=$B/results-e3/ab-base; RB=$B/results-e3/ab-new
rm -rf $RA $RB; mkdir -p $RA $RB
trap "$ISO/restore.sh" EXIT
trap "exit 1" TERM INT
$ISO/isolate.sh
run() { # dir name env... -- cmd...
  local R=$1 name=$2; shift 2
  local env=(--setenv=HOME=$HOME)
  while [ "$1" != "--" ]; do env+=("--setenv=$1"); shift; done; shift
  echo "$(date +%T) $R $name" | tee -a $B/results-e3/ab.log
  sudo systemd-run --quiet --scope --slice=bench.slice -p AllowedCPUs=0-15 --uid=dmd --gid=dmd \
    "${env[@]}" timeout 400 "$@" > $R/$name.txt 2>&1
  sleep 3
}
for r in ${RATES:-100000 300000 500000 750000}; do
  for m in none async; do
    c=$B/cfg-batch-tmpfs; [ $m = async ] && c=$B/cfg-batch-disk
    run $RA k-$m-$r RIG=1 CONF=$c KRUSTY_BIN=$WBIN -- $K $r 60 10
    run $RB k-$m-$r RIG=1 CONF=$c -- $K $r 60 10
  done
done
echo "$(date +%T) done"
