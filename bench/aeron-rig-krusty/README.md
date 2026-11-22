# One load generator for both clusters

aeron-io/benchmarks' `LoadTestRig` (the client that measures Aeron Cluster in
`bench/aeron-cluster`) driving KrustyCast through `KrustyMessageTransceiver`:
same open-loop schedule, latency definition (scheduled send time to the
answer: Aeron's echo, KrustyCast's ACCEPTED), histograms and JVM. As Aeron's
client hands frames to its media driver, the transceiver hands SUBMITs to an
I/O thread (`krusty-io`) that packs them into datagrams of up to
`krusty.mtu` bytes (1408, Aeron's default). `bench/krusty-cluster/run.sh`
with `RIG=1` pins the rig thread on 15 and the I/O thread on 7, as Aeron's
client's rig and driver.

    bench/aeron-rig-krusty/build.sh      # -> /var/tmp/krusty-aeron-bench/krusty-rig/krusty-rig.jar
    RIG=1 CONF=<dir with node1-3.toml> bench/krusty-cluster/run.sh RATE SECONDS [WARMUP]

The nodes' `outcome_ring` must be at least `krusty.window` (1024).
