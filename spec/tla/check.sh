#!/bin/sh
# Model-checks the TLA+ specs with TLC.
#
#   spec/tla/check.sh            CI configurations (seconds each)
#   spec/tla/check.sh --large    adds the large configurations (hours)
#
# TLA2TOOLS points at tla2tools.jar (default: ~/.local/share/krusty-tools/tla).
set -eu
cd "$(dirname "$0")"
jar=${TLA2TOOLS:-$HOME/.local/share/krusty-tools/tla/tla2tools.jar}
meta=$(mktemp -d)
trap 'rm -rf -- "${meta:?}"' EXIT

configs="SequencerRace SequencerRestart"
[ "${1:-}" = "--large" ] && configs="$configs SequencerLarge"

status=0
for cfg in $configs; do
    spec=Sequencer
    printf '%-20s ' "$cfg"
    if java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto \
        -config "$cfg.cfg" -metadir "$meta/$cfg" "$spec.tla" >"$meta/$cfg.log" 2>&1 \
        && grep -q "No error has been found" "$meta/$cfg.log"; then
        grep -v Progress "$meta/$cfg.log" | grep "distinct states found" | sed 's/^[0-9,]* states generated, //'
    else
        echo "FAILED"
        cat "$meta/$cfg.log"
        status=1
    fi
done
exit $status
