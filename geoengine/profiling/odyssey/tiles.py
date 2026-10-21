#!/usr/bin/env python3
"""Replay an Odyssey viewport using the server's actual tile matrix metadata (stdlib only)."""
import argparse
import concurrent.futures
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import statistics
import threading
import time
import urllib.parse
import urllib.request
import uuid

INTERNAL_PROVIDER = "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74"
PNG = b"\x89PNG\r\n\x1a\n"


def percentile(values, quantile):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * quantile) - 1)] if values else None


def viewport(matrix, lon, lat, size):
    """Coordinates are derived from CustomWebMercator metadata, never slippy assumptions."""
    x = 6378137 * math.radians(lon)
    y = 6378137 * math.asinh(math.tan(math.radians(lat)))
    origin_x, origin_y = matrix["pointOfOrigin"]
    col = math.floor((x - origin_x) / (matrix["cellSize"] * matrix["tileWidth"]))
    row = math.floor((origin_y - y) / (matrix["cellSize"] * matrix["tileHeight"]))
    width, height = matrix["matrixWidth"], matrix["matrixHeight"]
    if not (0 <= col < width and 0 <= row < height):
        raise ValueError("Center is outside this layer's tile matrix")
    if matrix.get("cornerOfOrigin", "topLeft") != "topLeft":
        raise ValueError("Only topLeft tile matrices are supported")
    tiles = [(r, c) for r in range(row - size // 2, row - size // 2 + size)
             for c in range(col - size // 2, col - size // 2 + size)
             if 0 <= r < height and 0 <= c < width]
    return sorted(tiles, key=lambda rc: (rc[0] - row) ** 2 + (rc[1] - col) ** 2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default="http://127.0.0.1:3030/api")
    parser.add_argument("--layer-id", required=True, help="Imported backend layer ID, not the STAC provider ID")
    parser.add_argument("--connector-id", default=INTERNAL_PROVIDER)
    parser.add_argument("--tms", default="CustomWebMercator", choices=["CustomWebMercator", "WebMercatorQuad"])
    parser.add_argument("--time", default="2026-04-01T00:00:00Z")
    parser.add_argument("--center", nargs=2, type=float, default=[8.77, 50.81], metavar=("LON", "LAT"))
    parser.add_argument("--zoom", type=int, help="Matrix ID reported by --list-matrices")
    parser.add_argument("--resolution", type=float, default=38.218514, help="Choose nearest cellSize in projected m/pixel if --zoom is absent")
    parser.add_argument("--size", type=int, default=3, help="Square viewport width in tiles")
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--duplicates", type=int, default=1, help="Simultaneous copies of every tile, for sharing experiments")
    parser.add_argument("--pause", type=float, default=2, help="Seconds between completed viewports")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--run-id", default=time.strftime("odyssey-%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8])
    parser.add_argument("--list-matrices", action="store_true")
    parser.add_argument("--anonymous", action="store_true", help="Create an anonymous session when GEOENGINE_TOKEN is unset")
    parser.add_argument("--save-png", action="store_true")
    args = parser.parse_args()
    if min(args.size, args.concurrency, args.repeats, args.duplicates) < 1:
        parser.error("size, concurrency, repeats, and duplicates must be positive")
    if args.timeout <= 0 or args.pause < 0 or args.resolution <= 0:
        parser.error("timeout/resolution must be positive; pause must be nonnegative")
    lon, lat = args.center
    if not (-180 <= lon <= 180 and -85.05112878 <= lat <= 85.05112878):
        parser.error("center is outside Web Mercator")
    args.out.mkdir(parents=True, exist_ok=False)
    token = os.environ.get("GEOENGINE_TOKEN", "")
    if not token and args.anonymous:
        request = urllib.request.Request(args.api_url.rstrip("/") + "/anonymous", data=b"{}", method="POST", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            token = json.load(response)["id"]
    if not token:
        parser.error("set GEOENGINE_TOKEN or pass --anonymous")
    headers = {"Authorization": "Bearer " + token, "X-GeoEngine-Profile-Run": args.run_id}
    encoded_layer = urllib.parse.quote(args.layer_id, safe="")
    ogc = f"{args.api_url.rstrip('/')}/ogc/{args.connector_id}/{encoded_layer}"
    metadata_url = f"{ogc}/tileMatrixSets/{args.tms}"
    with urllib.request.urlopen(urllib.request.Request(metadata_url, headers=headers), timeout=args.timeout) as response:
        metadata = json.load(response)
    (args.out / "tile-matrix-set.json").write_text(json.dumps(metadata, indent=2))
    matrices = metadata["tileMatrices"]
    for m in matrices:
        print(f"matrix={m['id']} cellSize={m['cellSize']:.6f} tile={m['tileWidth']}x{m['tileHeight']} grid={m['matrixWidth']}x{m['matrixHeight']}")
    if args.list_matrices:
        return
    matrix = (next((m for m in matrices if m["id"] == str(args.zoom)), None) if args.zoom is not None
              else min(matrices, key=lambda m: abs(math.log(m["cellSize"] / args.resolution))))
    if matrix is None:
        parser.error("zoom is not available; use --list-matrices")
    coords = viewport(matrix, lon, lat, args.size)
    query = urllib.parse.urlencode({"datetime": args.time})
    base = f"{ogc}/collections/{encoded_layer}/map/tiles/{args.tms}/{matrix['id']}"
    urls = [f"{base}/{r}/{c}?{query}" for r, c in coords for _ in range(args.duplicates)]
    (args.out / "urls.txt").write_text("\n".join(urls) + "\n")
    manifest = {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()}
    manifest.update(matrix=matrix, tiles=coords, started_unix_us=time.time_ns() // 1000, phases=[])
    local = threading.local()

    def fetch(job):
        repeat, index, url = job
        parsed = urllib.parse.urlsplit(url)
        # Slow tiles can leave other connections idle beyond the server's keepalive
        # timeout before the next viewport starts. Keep pooling within each viewport.
        if getattr(local, "repeat", None) != repeat:
            if getattr(local, "connection", None) is not None:
                local.connection.close()
            local.connection = None
            local.repeat = repeat
        if not hasattr(local, "connection") or local.connection is None:
            cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
            local.connection = cls(parsed.hostname, parsed.port, timeout=args.timeout)
        request_headers = dict(headers, **{"X-GeoEngine-Profile-Request": f"{repeat}-{index}"})
        start = time.perf_counter()
        record = {"repeat": repeat, "index": index, "url": url, "started_unix_us": time.time_ns() // 1000}
        try:
            local.connection.request("GET", parsed.path + "?" + parsed.query, headers=request_headers)
            response = local.connection.getresponse()
            record["ttfb_ms"] = (time.perf_counter() - start) * 1000
            body = response.read()
            valid = response.status == 200 and response.getheader("content-type", "").split(";")[0] == "image/png" and body.startswith(PNG)
            record.update(status=response.status, bytes=len(body), valid_png=valid,
                          sha256=hashlib.sha256(body).hexdigest(), cache_control=response.getheader("cache-control"),
                          request_id=response.getheader("x-request-id"), computation_id=response.getheader("x-computation-id"))
            if args.save_png and valid:
                (args.out / f"tile-{repeat}-{index}.png").write_bytes(body)
        except Exception as error:  # Preserve failures in the measurements; never retry silently.
            record.update(error=type(error).__name__, valid_png=False)
            local.connection.close()
            local.connection = None
        record["total_ms"] = (time.perf_counter() - start) * 1000
        return record

    failures = 0
    with (args.out / "requests.jsonl").open("w") as output, concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        for repeat in range(args.repeats):
            started = time.perf_counter()
            jobs = [(repeat, i, url) for i, url in enumerate(urls)]
            results = list(executor.map(fetch, jobs))
            for result in results:
                output.write(json.dumps(result) + "\n")
            output.flush()
            good = [r["total_ms"] for r in results if r["valid_png"]]
            failed = len(results) - len(good)
            failures += failed
            summary = {"repeat": repeat, "viewport_ms": (time.perf_counter() - started) * 1000,
                       "requests": len(results), "errors": failed, "p50_ms": statistics.median(good) if good else None,
                       "p95_ms": percentile(good, .95), "p99_ms": percentile(good, .99)}
            manifest["phases"].append(summary)
            manifest["ended_unix_us"] = time.time_ns() // 1000
            (args.out / "run.json").write_text(json.dumps(manifest, indent=2))
            print(json.dumps(summary), flush=True)
            if repeat + 1 < args.repeats:
                time.sleep(args.pause)
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
