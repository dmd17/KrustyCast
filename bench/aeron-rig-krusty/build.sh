#!/bin/bash
# Builds krusty-rig.jar: the KrustyCast transceiver for aeron-io/benchmarks'
# LoadTestRig, against the suite's jar (built at $AERON_BENCH).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
AB=${AERON_BENCH:-$HOME/.local/share/krusty-tools/aeron-benchmarks}
JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}
OUT=${OUT:-/var/tmp/krusty-aeron-bench/krusty-rig}
mkdir -p "$OUT/classes"
"$JAVA_HOME/bin/javac" -proc:none -Xlint:all -cp "$AB/benchmarks-all/build/libs/benchmarks.jar" \
  -d "$OUT/classes" "$HERE"/src/io/krustycast/bench/*.java
"$JAVA_HOME/bin/jar" cf "$OUT/krusty-rig.jar" -C "$OUT/classes" .
echo "$OUT/krusty-rig.jar"
