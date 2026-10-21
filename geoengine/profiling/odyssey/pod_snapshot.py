#!/usr/bin/env python3
"""Sample server and worker CPU, RSS, kernel I/O, cgroup throttling, and hotpath inside a pod.

No command lines or environments are read: worker arguments contain credentials.
"""
import argparse
import json
import os
from pathlib import Path
import time
import urllib.request


def snapshot():
    ticks = os.sysconf("SC_CLK_TCK")
    page = os.sysconf("SC_PAGE_SIZE")
    processes = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            comm = (proc / "comm").read_text().strip()
            if comm not in ("geoengine-serve", "geoengine-server", "gdalsource-proc", "gdalsource-process"):
                continue
            stat = (proc / "stat").read_text().rsplit(")", 1)[1].split()
            io = dict(line.split(": ") for line in (proc / "io").read_text().splitlines())
            processes.append({"pid": int(proc.name), "comm": comm,
                              "start_ticks": int(stat[19]), "cpu_seconds": (int(stat[11]) + int(stat[12])) / ticks,
                              "rss_bytes": int(stat[21]) * page, "io": {k: int(v) for k, v in io.items()}})
        except (OSError, ValueError):
            continue
    cgroup = {}
    for name in ("cpu.stat", "cpu.max", "memory.current", "memory.events", "io.stat"):
        try:
            cgroup[name] = (Path("/sys/fs/cgroup") / name).read_text().strip()
        except OSError:
            pass
    return {"unix_us": time.time_ns() // 1000, "processes": processes, "cgroup": cgroup}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--hotpath", action="store_true", help="Query localhost live profiler endpoints once")
    parser.add_argument("--workers", type=int, default=32)
    args = parser.parse_args()
    if args.hotpath:
        for label, port in [("server", 6770)] + [(f"worker-{i}", 6800 + i) for i in range(args.workers)]:
            result = {"process": label, "unix_us": time.time_ns() // 1000}
            for endpoint in ("functions_timing", "threads"):
                try:
                    request = urllib.request.Request(f"http://127.0.0.1:{port}/{endpoint}")
                    token = os.environ.get("HOTPATH_METRICS_AUTH_TOKEN")
                    if token:
                        request.add_header("Authorization", token)
                    with urllib.request.urlopen(request, timeout=5) as response:
                        result[endpoint] = json.load(response)
                except Exception as error:
                    result[endpoint] = {"error": type(error).__name__}
            print(json.dumps(result), flush=True)
        return
    stop = time.monotonic() + args.seconds
    while time.monotonic() < stop:
        print(json.dumps(snapshot()), flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
