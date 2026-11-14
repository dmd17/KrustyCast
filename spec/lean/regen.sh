#!/bin/sh
# Regenerates the Lean translation of krusty-core into spec/lean/KrustyCore.
# Needs Charon and Aeneas (see spikes/aeneas-stream/REPORT.md for setup).
set -eu
# Snapshot code is not verified (its property test covers it) and is left out.
T=${KRUSTY_TOOLS:-$HOME/.local/share/krusty-tools}
here=$(cd "$(dirname "$0")" && pwd)
out=$(mktemp -d)
trap 'rm -rf -- "${out:?}"' EXIT
(cd "$here/../../crates/krusty-core" && "$T/charon/bin/charon" cargo --preset=aeneas --exclude "krusty_core::snapshot" --dest-file "$out/krusty_core.llbc")
"$T/aeneas/bin/aeneas" -backend lean -split-files -dest "$out" "$out/krusty_core.llbc"
cp "$out/Types.lean" "$out/Funs.lean" "$here/KrustyCore/"
cp "$out/FunsExternal_Template.lean" "$here/KrustyCore/FunsExternal.lean"
"$here/source-hash.sh" > "$here/KrustyCore/SOURCE_HASH"
