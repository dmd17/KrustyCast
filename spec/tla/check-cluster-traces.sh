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
    out=$(cd "$w" && java -XX:+UseParallelGC -Xss16m -cp "$jar" tlc2.TLC -workers 1 \
        -metadir "$w/states" -config TraceCluster.cfg TraceCluster.tla 2>&1 || true)
    rm -rf -- "$w/states"
    if grep -q "Invariant Unfinished is violated" <<<"$out"; then
        echo "accepted: $t ($(wc -l < "$t") records)"
    else
        echo "REJECTED: $t"
        grep -E "Deadlock|is violated|Error" <<<"$out" | head -3 || true
        echo "$out" > "$w/tlc.out"
        fail=1
    fi
done
exit $fail
