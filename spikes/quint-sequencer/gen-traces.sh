#!/bin/sh
# Generates random traces of the Quint spec for the model-based test in mbt/.
#   ./gen-traces.sh [N]      (default 2000 traces of up to 40 steps)
set -eu
cd "$(dirname "$0")"
Q=${QUINT:-$HOME/.local/share/krusty-tools/quint/bin/quint}
mkdir -p traces
find traces -name '*.itf.json' -delete
"$Q" run Sequencer.qnt --main SequencerRace --invariant safety --max-steps 40 \
    --max-samples "${1:-2000}" --n-traces "${1:-2000}" --out-itf 'traces/t_{seq}.itf.json' \
    --verbosity 1
