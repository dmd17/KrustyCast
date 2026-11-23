#!/usr/bin/env bash
# Checks cluster-shell traces against Cluster.tla with TLC (TraceCluster).
#   KRUSTY_CLUSTER_TRACE_DIR=dir cargo test --release -p krusty-client \
#       --test cluster -- --ignored traces
#   spec/tla/check-cluster-traces.sh dir
# Each dir/node<id>.trace (one TLA+ record per line) is wrapped in a
# TraceClusterData module and replayed: accepted when TLC reports the
# invariant Unfinished violated (the last record was reached); rejected on
# a deadlock (a step of the code that is not the model's) or an error.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
jar=${TLA2TOOLS:-$HOME/.local/share/krusty-tools/tla/tla2tools.jar}
dir=${1:?usage: check-cluster-traces.sh <trace dir>}
fail=0
for t in "$dir"/node*.trace; do
    [ -f "$t" ] || continue
    w="$t.check"
    mkdir -p -- "$w"
    cp "$here/Cluster.tla" "$here/TraceCluster.tla" "$here/TraceCluster.cfg" "$w"
    {
        echo "---- MODULE TraceClusterData ----"
        echo "TraceClients == {$(grep -o 'c |-> "c[0-9]*i[0-9]*"' "$t" | sed 's/c |-> //' | sort -u | paste -sd, -)}"
        echo "TraceMaxSeq == $(grep -o 'c |-> "c[0-9]*i[0-9]*", seq |-> [0-9]*' "$t" | grep -o '[0-9]*$' | sort -n | tail -1)"
        echo "Trace == <<"
        sed '$!s/$/,/' "$t"
        echo ">>"
        echo "===="
    } > "$w/TraceClusterData.tla"
    # TLC prints the verdict, then the whole trace (both when the trace is
    # accepted, as a violation of Unfinished, and when it is rejected):
    # gigabytes for a long trace. Keep the first 2 MB, which hold the
    # verdict; TLC stops on the closed pipe.
    (cd "$w" && java -XX:+UseParallelGC -Xss16m -cp "$jar" tlc2.TLC -workers 1 \
        -noGenerateSpecTE -metadir "$w/states" -config TraceCluster.cfg TraceCluster.tla 2>&1 || true) \
        | head -c 2000000 > "$w/tlc.out" || true
    rm -rf -- "$w/states"
    if grep -q "Invariant Unfinished is violated" "$w/tlc.out"; then
        echo "accepted: $t ($(wc -l < "$t") records)"
        rm -f -- "$w/tlc.out"
    else
        echo "REJECTED: $t (TLC output: $w/tlc.out)"
        grep -E "Deadlock|is violated|Error" "$w/tlc.out" | head -3 || true
        fail=1
    fi
done
exit $fail
