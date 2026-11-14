#!/bin/sh
# Regenerates the Lean translation of the subset crate into proofs/StreamSubset.
set -e
T=${KRUSTY_TOOLS:-$HOME/.local/share/krusty-tools}
cd "$(dirname "$0")/subset"
"$T/charon/bin/charon" cargo --preset=aeneas
cd ..
rm -rf lean-subset && mkdir lean-subset
"$T/aeneas/bin/aeneas" -backend lean -split-files -dest lean-subset subset/stream_subset.llbc
cp lean-subset/Types.lean lean-subset/Funs.lean proofs/StreamSubset/
cp lean-subset/FunsExternal_Template.lean proofs/StreamSubset/FunsExternal.lean
