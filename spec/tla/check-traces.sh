#!/usr/bin/env bash
# Checks krusty-raft traces against RaftKC with TLC (gap Q3).
#   KRUSTY_RAFT_TRACE_DIR=dir cargo test -p krusty-raft --test trace -- --ignored
#   spec/tla/check-traces.sh dir
# Each dir/<seed>/TraceData.tla is replayed by TraceRaftKC: accepted when
# TLC reports the invariant Unfinished violated (the last record matched);
# rejected on a deadlock (some step of the code is not a RaftKC step) or a
# violation of TraceSafety.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
jar=${TLA2TOOLS:-$HOME/.local/share/krusty-tools/tla/tla2tools.jar}
dir=${1:?usage: check-traces.sh <trace dir>}
fail=0
for d in "$dir"/*/; do
    [ -f "$d/TraceData.tla" ] || continue
    cp "$here/RaftKC.tla" "$here/TraceRaftKC.tla" "$here/TraceRaftKC.cfg" "$d"
    out=$(cd "$d" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers 1 \
        -metadir "$d/states" -config TraceRaftKC.cfg TraceRaftKC.tla 2>&1 || true)
    rm -rf -- "$d/states"
    if grep -q "Invariant Unfinished is violated" <<<"$out"; then
        echo "accepted: $d ($(grep -c '^\[node' "$d/TraceData.tla") records)"
    else
        echo "REJECTED: $d"
        grep -E "Deadlock|is violated|Error" <<<"$out" | head -3 || true
        echo "$out" > "$d/tlc.out"
        fail=1
    fi
done
exit $fail
