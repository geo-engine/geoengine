#!/usr/bin/env python3
"""Export Jaeger JSON and summarize span latency, sharing, and tile amplification."""
import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import statistics
import urllib.parse
import urllib.request


def tags(span):
    return {tag["key"]: tag["value"] for tag in span.get("tags", [])}


def summarize(traces):
    durations = defaultdict(list)
    outcomes = Counter()
    input_tiles = []
    queues = []
    read_ids = set()
    for trace in traces:
        for span in trace.get("spans", []):
            name, fields = span["operationName"], tags(span)
            durations[name].append(span["duration"] / 1000)
            if name == "Request" and "http_handler_ms" in fields:
                durations["http_handler_ms (measured)"].append(float(fields["http_handler_ms"]))
            if name == "http_attempt" and "http_status" in fields:
                outcomes[(name, str(fields["http_status"]))] += 1
            if name in ("raster_cache_get", "operator_cache_query", "stac_cache_get"):
                outcomes[(name, fields.get("outcome", "unknown"))] += 1
            if name == "gdal_read_dispatch":
                outcomes[(name, str(fields.get("shared", "unknown")))] += 1
            if name == "raster_subquery_tile" and "input_tiles" in fields:
                input_tiles.append(int(fields["input_tiles"]))
            if name == "gdal_broker_dispatch" and "queue_ms" in fields:
                queues.append(float(fields["queue_ms"]))
            if name == "gdal_worker_companion" and fields.get("read_id"):
                read_ids.add(fields["read_id"])
    print("Span latencies are inclusive and overlap; their sums are NOT request wall time or CPU time.")
    print(f"{'span':42} {'count':>7} {'sum ms':>12} {'p50 ms':>12} {'p95 ms':>12}")
    for name, values in sorted(durations.items(), key=lambda item: sum(item[1]), reverse=True):
        values.sort()
        p95 = values[max(0, math.ceil(.95 * len(values)) - 1)]
        print(f"{name:42} {len(values):7} {sum(values):12.2f} {statistics.median(values):12.2f} {p95:12.2f}")
    for (name, outcome), count in sorted(outcomes.items()):
        print(f"{name} {outcome}: {count}")
    if input_tiles:
        print(f"Subquery input tiles: total={sum(input_tiles)} outputs={len(input_tiles)} mean={statistics.mean(input_tiles):.2f} max={max(input_tiles)} (per output band)")
    if queues:
        print(f"GDAL queue ms: p50={statistics.median(queues):.2f} max={max(queues):.2f}")
    return read_ids


def summarize_processes(path, run):
    samples = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if run:
        # Only complete sample intervals inside the workload window are included.
        samples = [s for s in samples if run["started_unix_us"] <= s["unix_us"] <= run["ended_unix_us"]]
    cpu = Counter()
    wall = 0
    for before, after in zip(samples, samples[1:]):
        elapsed = (after["unix_us"] - before["unix_us"]) / 1_000_000
        previous = {(p["pid"], p["start_ticks"]): p for p in before["processes"]}
        for process in after["processes"]:
            old = previous.get((process["pid"], process["start_ticks"]))
            if old:
                kind = "GDAL workers" if process["comm"].startswith("gdalsource") else "server"
                cpu[kind] += max(0, process["cpu_seconds"] - old["cpu_seconds"])
        wall += elapsed
    if wall <= 0:
        print("Process samples: fewer than two samples inside the run; extend or shorten the sampling interval")
        return
    for kind, seconds in cpu.items():
        print(f"{kind}: CPU seconds={seconds:.2f}, average occupied cores={seconds / wall:.2f} over {wall:.2f}s")
    def counters(sample):
        return dict(line.split() for line in sample.get("cgroup", {}).get("cpu.stat", "").splitlines())
    first, last = counters(samples[0]), counters(samples[-1])
    for key in ("nr_periods", "nr_throttled", "throttled_usec"):
        if key in first and key in last:
            print(f"cgroup delta {key}: {int(last[key]) - int(first[key])}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, help="Workload run.json, used to select traces")
    parser.add_argument("--jaeger-url", default="http://127.0.0.1:16686")
    parser.add_argument("--out", type=Path, help="Export raw combined Jaeger JSON here")
    parser.add_argument("--input", type=Path, help="Analyze an existing export offline")
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--processes", type=Path, help="Optional pod sampler JSONL")
    args = parser.parse_args()
    run = json.loads(args.run.read_text()) if args.run else None
    if args.input:
        traces = json.loads(args.input.read_text())["data"]
    else:
        if not args.run or not args.out:
            parser.error("--run and --out are required for export")
        start = run["started_unix_us"] - 5_000_000
        end = run["ended_unix_us"] + 5_000_000

        def query(service, **filters):
            params = dict(service=service, start=start, end=end, limit=args.limit, **filters)
            url = args.jaeger_url.rstrip("/") + "/api/traces?" + urllib.parse.urlencode(params)
            with urllib.request.urlopen(url, timeout=120) as response:
                payload = json.load(response)
            if payload.get("errors"):
                raise RuntimeError(payload["errors"])
            data = payload.get("data") or []
            if len(data) >= args.limit:
                raise RuntimeError("Trace limit reached; shorten the run or increase --limit")
            return data

        main_traces = query("Geo Engine", tags=json.dumps({"profile_run": run["run_id"]}))
        if not main_traces:
            raise RuntimeError("No run-tagged traces found; check profiling build, filters, exporter, and wait for batch export")
        wanted = {tags(s).get("read_id") for trace in main_traces for s in trace.get("spans", [])
                  if s["operationName"] == "gdal_worker_companion"}
        workers = query("Geo Engine GDAL Worker", operation="gdal_worker_read_tile") if wanted else []
        worker_traces = [t for t in workers if any(tags(s).get("read_id") in wanted for s in t.get("spans", []))]
        traces = list({t["traceID"]: t for t in main_traces + worker_traces}.values())
        args.out.write_text(json.dumps({"data": traces}, indent=2))
        found = {tags(s).get("read_id") for t in worker_traces for s in t.get("spans", [])}
        missing = wanted - found - {None}
        if missing:
            print(f"WARNING: {len(missing)} worker reads missing; check worker telemetry/export delay/time window")
        tile_requests = sum(s["operationName"] == "ogc_tile" for t in main_traces for s in t.get("spans", []))
        expected = sum(p["requests"] for p in run["phases"])
        print(f"Tile request spans: {tile_requests}, client requests: {expected}. Check any mismatch before drawing conclusions.")
    summarize(traces)
    if args.processes:
        summarize_processes(args.processes, run)


if __name__ == "__main__":
    main()
