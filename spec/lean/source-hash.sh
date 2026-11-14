#!/bin/sh
# Hash of the Rust sources the Lean in KrustyCore/ was generated from.
# CI compares it with KrustyCore/SOURCE_HASH (written by regen.sh) so the
# proofs cannot silently describe stale code.
set -eu
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
find crates/krusty-core/src crates/krusty-core/Cargo.toml crates/krusty-wire/src crates/krusty-wire/Cargo.toml \
    -type f | LC_ALL=C sort | xargs sha256sum | sha256sum | cut -d' ' -f1
