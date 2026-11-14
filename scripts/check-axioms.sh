#!/bin/sh
# Builds a Lean proof project and fails if any `#print axioms` output mentions
# an axiom outside Lean's standard three (in particular `sorryAx`), or if the
# build reports a declaration using `sorry`.
#
#   scripts/check-axioms.sh <lake project dir>
set -eu
dir=$1
log=$(mktemp)
trap 'rm -f "$log"' EXIT
(cd "$dir" && lake build) >"$log" 2>&1 || { cat "$log"; exit 1; }
if grep -q "declaration uses 'sorry'\|declaration uses \`sorry\`" "$log"; then
    grep "sorry" "$log"; echo "FAIL: sorry in proofs"; exit 1
fi
found=$(grep -c "depends on axioms" "$log" || true)
if [ "$found" -eq 0 ]; then
    echo "FAIL: no '#print axioms' output (did the proofs stop printing it?)"; exit 1
fi
bad=$(grep "depends on axioms" "$log" \
    | sed 's/.*depends on axioms: \[\(.*\)\]/\1/' | tr ',' '\n' | tr -d ' ' \
    | grep -v -x -e propext -e Classical.choice -e Quot.sound || true)
if [ -n "$bad" ]; then
    grep "depends on axioms" "$log"; echo "FAIL: unexpected axioms: $bad"; exit 1
fi
grep "depends on axioms" "$log"
echo "OK: $found theorem(s) checked"
