# e3-rig: one Rust load generator for Aeron Cluster and KrustyCast

E3 (docs/BASELINE.md) compares the two clusters under one load generator,
and no JVM runs on the client side of either. The rig keeps the method of
aeron-io/benchmarks' Java `LoadTestRig`:
- an open-loop schedule, one message per send, each stamped with its
  scheduled time;
- a warm-up in 10 chunks, then one measured run;
- 100 ms of grace to send, 3 s to receive;
- the same percentile output.

The two clients:
- **Aeron:** Aeron's C client, and the Cluster client protocol
  (`src/aeron.rs`). The client's media driver is Aeron's C `aeronmd`.
- **KrustyCast:** `krusty-session` on the rig thread, and an I/O thread
  that packs SUBMITs into datagrams (`src/krusty.rs`).

Build (outside the workspace, since it links Aeron's C client):

    # Aeron 1.54.0's C client and driver, from source, into
    # ~/.local/share/krusty-tools/aeron-c-src/aeron-1.54.0/build. Its
    # CMakeLists asks for CMake 3.30; 3.28 builds this subset after
    # lowering cmake_minimum_required and cmake_policy to 3.28.
    cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_AERON_DRIVER=ON -DBUILD_AERON_ARCHIVE_API=OFF \
      -DAERON_TESTS=OFF -DAERON_UNIT_TESTS=OFF -DAERON_SYSTEM_TESTS=OFF -DAERON_BUILD_SAMPLES=OFF \
      -DAERON_BUILD_DOCUMENTATION=OFF -DAERON_INSTALL_TARGETS=OFF
    make -j8 aeron_static aeronmd
    cargo build --release            # here; links libaeron_static (and libbsd)

Used by `bench/aeron-cluster/run.sh` (always) and
`bench/krusty-cluster/run.sh` (with `RIG=1`).
