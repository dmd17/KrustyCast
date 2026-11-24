#!/usr/bin/env python3
"""Sample memory of the benchmark processes once a second (E3).

One CSV row per process per sample: proportional set size (PSS, so pages
shared between processes, like Aeron's /dev/shm log buffers, count once
in total), split into anonymous (heap etc.), file-backed and shmem, plus
RSS and peak RSS (VmHWM). Every 10 s also the size of the Aeron
directories in /dev/shm and of each system's on-disk storage.
Usage: memsample.py OUT.csv   (runs until killed; 2026-11-24: also e3-rig and aeronmd)
"""
import os, sys, time, glob, re

FIELDS = ["Rss", "Pss", "Pss_Anon", "Pss_File", "Pss_Shmem"]

def label(pid):
    try:
        return label_of(pid)
    except (OSError, ValueError, IndexError):
        return None  # a process starting or exiting

def label_of(pid):
    try:
        comm = open(f"/proc/{pid}/comm").read().strip()
        if comm not in ("krusty-load", "krusty-node", "java", "e3-rig", "aeronmd"):
            return None
        cmd = open(f"/proc/{pid}/cmdline").read().split("\0")
        if comm in ("e3-rig", "aeronmd"):
            return comm + ":" + (cmd[1] if comm == "e3-rig" and len(cmd) > 1 else "client")
        if comm == "krusty-load":
            return "krusty-load:" + ("client" if "--connect" in cmd else "in-process")
        if comm == "krusty-node":
            return "krusty-node:" + os.path.basename(cmd[cmd.index("--config") + 1]).removesuffix(".toml")
    except OSError:
        return None
    main = next((a.rsplit(".", 1)[-1] for a in cmd if a.startswith("io.aeron.")), "?")
    props = [os.path.basename(a).removesuffix(".properties") for a in cmd if a.endswith(".properties")
             and not a.endswith("cluster.properties")]
    return f"{main}:{props[-1] if props else '?'}" if main != "?" or props else None

def du(path):
    tot = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                tot += os.lstat(os.path.join(root, f)).st_blocks * 512
            except OSError:
                pass
    return tot

out = open(sys.argv[1], "a", buffering=1)
if out.tell() == 0:
    out.write("t,pid,proc," + ",".join(f"{f}_kB" for f in FIELDS) + ",VmHWM_kB\n")
n = 0
while True:
    t = time.time()
    for pid in filter(str.isdigit, os.listdir("/proc")):
        name = label(pid)
        if not name:
            continue
        try:
            roll = open(f"/proc/{pid}/smaps_rollup").read()
            hwm = re.search(r"VmHWM:\s+(\d+)", open(f"/proc/{pid}/status").read()).group(1)
        except (OSError, AttributeError):
            continue
        vals = [re.search(rf"^{f}:\s+(\d+)", roll, re.M) for f in FIELDS]
        out.write(f"{t:.1f},{pid},{name}," + ",".join(v.group(1) if v else "" for v in vals) + f",{hwm}\n")
    if n % 10 == 0:
        for d in glob.glob("/dev/shm/kb-*") + glob.glob("/dev/shm/aeron*") + glob.glob("/var/tmp/krusty-aeron-bench/node*") + \
                 glob.glob("/var/tmp/krusty-aeron-bench/krusty-*"):
            out.write(f"{t:.1f},0,du:{d},{du(d)//1024},,,,,\n")
    n += 1
    time.sleep(max(0, 1 - (time.time() - t)))
