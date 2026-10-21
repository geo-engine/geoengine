# Odyssey / EDV tile profiling

Measure the critical path of a Sentinel-2 viewport: operator initialization → STAC discovery → cache → serial raster subqueries → GDAL scheduling → dataset open/read → reprojection → colorization/PNG. Rank optimization candidates by **viewport completion time**, p95 tile latency, and avoided work per viewport. Instrumentation does not change scheduling or caching policy.

## Plan and measurement boundaries

1. Establish a repeatable Sentinel-2 TCI viewport and confirm it contains imagery.
2. Trace one request at concurrency 1, then a viewport at concurrency 6/12, across several resolutions.
3. Compare first versus repeated requests, explicit cache-disabled runs, and duplicate concurrent requests. Keep the database, provider mapping, resource limits, scene, and viewport fixed.
4. Use spans to locate elapsed time, hotpath to aggregate selected Rust functions, and process CPU/cgroup measurements to distinguish work from waiting.
5. Follow the largest remaining component with native GDAL/JP2 CPU sampling or an S3/local-file comparison. Change one parameter or implementation at a time and repeat.

The scripts use only Python's standard library. `tiles.py` reuses an HTTP connection per client thread, never silently retries failed tile requests, and saves exact URLs and metadata. `pod_snapshot.py` runs inside the backend container. `traces.py` exports Jaeger JSON and produces an initial latency/count summary.

### Inspected staging environment (2026-10-20)

- Kubeconfig: workspace-root `fra1-3-gen-01.yaml`; namespace: `staging-geoengine-edv`.
- Deployment/container: `geoengine-edv-staging-backend`; one replica; service port 8080.
- Inspected pod: `geoengine-edv-staging-backend-6884b8b7f-v8vp2`. Resolve the current pod after every rollout.
- Image: `registry.cloudferro.com/geoengine/edv/backend:basic-3`; `/api/info` reports commit `e22d074f8`, build 2026-10-07, version 0.9.4, no features.
- Requests: 4 CPUs / 8 GiB; limits: 8 CPUs / 32 GiB.
- ConfigMap: `geoengine-edv-staging-backend-config`; raster cache enabled, 8000 MB, new raster cache enabled; STAC cache 100 MB / 86400 s.
- Existing Jaeger: `geoengine-edv-staging-jaeger`, `jaegertracing/all-in-one:1.75.0`, ports 4317 (OTLP/gRPC), 16686 (UI/query API).
- ConfigMap uses older pool keys (`max_processes`, `global_active_worker`, `worker_per_dataset`). This checkout uses `number_of_processes`, `max_active_processes`, `max_dataset_processes`. `backend.env` sets these explicitly to 32/16/8.
- Jaeger query access and the process sampler were verified. Existing Jaeger advertised only `Geo Engine`; no recent traces were available at inspection time. An anonymous metadata probe for the local bootstrap report's layer returned 404 on `basic-3`. Verify the deployed catalogue and API against the new build before load testing; local import IDs need not match a particular deployment.

No deployment changes or raster load test were performed as part of these read-only cluster checks. The new image must be built and deployed to collect the new spans/hotpath data.

## Quick start after the profiling deployment is live

For the short GitOps/deployment guide, see the operations repository's `PROFILING.md`.
With Jaeger forwarded to local port 16686, one command now
runs the workload inside Kubernetes and downloads the artifacts:

```bash
python3 profiling/odyssey/run-cluster.py --kubeconfig ../../fra1-3-gen-01.yaml \
  --layer-id 29370057-0eb6-4226-b483-41d24b3dad0e \
  --time '2026-04-23T00:00:00Z/2026-04-24T00:00:00Z' \
  --out ./profiling/odyssey/results/run-02
```

This example runs from the backend directory. The layer was verified in staging on
2026-10-20 and STAC confirms imagery at Marburg on the selected day. Reimports can
change layer IDs; check the bootstrap import report if metadata returns 404. Use a
new output directory for each run. `--kubeconfig` is optional when `KUBECONFIG` is
already set in this terminal.

It defaults to anonymous access; set `GEOENGINE_TOKEN` for a restricted layer.
It collects CPU/cgroup samples, hotpath snapshots, PNGs/client timings, logs and
Jaeger traces, then removes its temporary Job/ConfigMap/session Secret.
The detailed manual steps below remain useful for custom experiments.

## 1. Build and validate

From the backend directory (`geoengine/geoengine` inside this workspace):

```bash
cargo build --locked --profile profiling --features geoengine-services/hotpath,geoengine-operators/hotpath \
  --bin geoengine-server --bin gdalsource-process
python3 -m unittest discover -s profiling/odyssey -p 'test_*.py' -v
```

Both binaries must come from the same build. The `profiling` profile inherits release optimization and retains line information. A build without `--features .../hotpath` keeps the annotations as no-ops. Tracing remains runtime configurable. Use an uninstrumented optimized build to measure instrumentation overhead before treating absolute throughput as production capacity.

For the existing container build, run from the **workspace root**, alongside `container/` and `geoengine/`:

```bash
export IMAGE=registry.cloudferro.com/geoengine/edv/backend:odyssey-profile-$(git -C geoengine rev-parse --short HEAD)
podman build --memory=8g --memory-swap=8g --cpu-period=100000 --cpu-quota=200000 \
  -f container/geoengine/Dockerfile \
  --build-arg BUILDER_IMAGE=quay.io/geoengine/devcontainer:extended-latest-2026-10-13 \
  --build-arg GEOENGINE_CODE_PATH=geoengine/geoengine \
  --build-arg GEOENGINE_GIT_PATH=geoengine/.git \
  --build-arg GEOENGINE_CONTAINER_PATH=container \
  --build-arg 'GEO_ENGINE_CARGO_BUILD_PARAMS=--release --locked -j 1 --features geoengine-services/hotpath,geoengine-operators/hotpath' \
  --build-arg GEO_ENGINE_RELEASE_LTO=thin \
  --build-arg GEO_ENGINE_RELEASE_DEBUG=1 \
  --build-arg GEO_ENGINE_RELEASE_STRIP=none \
  --build-arg 'GEO_ENGINE_RUST_FLAGS=-C force-frame-pointers=yes -C link-arg=-fuse-ld=lld -C link-arg=-Wl,--threads=2 -C codegen-units=8' \
  -t "${IMAGE}-backend" .
podman build -f geoengine/geoengine/profiling/odyssey/Containerfile \
  --build-arg BACKEND_IMAGE="${IMAGE}-backend" -t "$IMAGE" .
podman push "$IMAGE"
```

The image uses ThinLTO to bound release-link memory, retains debug information/frame pointers, and caches Cargo artifacts between builds. The final layer installs Python for the in-pod sampler. Use a unique image tag for each working-tree revision. The existing Dockerfile copies release/debug directories only, so its build command uses `--release`, with debug info supplied through flags. Do not pass `--profile profiling` to that Dockerfile without updating its target-directory selection. GDAL/PROJ and the runtime image must match the build environment.

## 2. Configure the staging profiling run

From the workspace root, in Bash:

```bash
export KUBECONFIG="$PWD/fra1-3-gen-01.yaml"
export NS=staging-geoengine-edv
export DEPLOY=geoengine-edv-staging-backend
export TOOLS="$PWD/geoengine/geoengine/profiling/odyssey"
export RESULTS="$PWD/odyssey-profile-results"
mkdir -p "$RESULTS"
k() { kubectl -n "$NS" "$@"; }
# If the snap wrapper fails, use /snap/kubectl/current/kubectl in this function.
```

Coordinate the image/env change with the deployment's Argo CD source of truth; otherwise reconciliation can undo it. The following prepares a reviewable Deployment file locally, without printing Secret/ConfigMap contents:

```bash
k get deployment "$DEPLOY" -o json > "$RESULTS/deployment-before.json"
k set image -f "$RESULTS/deployment-before.json" \
  "$DEPLOY=$IMAGE" --local -o yaml > "$RESULTS/deployment-image.local.yaml"
k set env -f "$RESULTS/deployment-image.local.yaml" \
  --env - --local -o yaml < "$TOOLS/backend.env" > "$RESULTS/deployment-profile.local.yaml"
# Review image, env, resources, and compatibility with the existing database/config.
# Apply the equivalent change through GitOps, or for an agreed temporary staging run:
k apply -f "$RESULTS/deployment-profile.local.yaml"
k rollout status deployment/"$DEPLOY" --timeout=300s
export POD=$(k get pods -l app.kubernetes.io/name="$DEPLOY" \
  --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
```

Use a dedicated replica/deployment when other users need staging. This checkout may include database migrations beyond the old image; validate compatibility/back up the database through the normal deployment process before upgrading or attempting rollback. Environment overrides do not make an arbitrary older database/image pair compatible.

Do not change the existing cache settings for the first baseline. Record effective runtime settings and image digest with every result. The server and worker telemetry settings are **independent**; both must be enabled. Release builds compile out `trace` level, so these measurements use `debug`. Do not rely on `RUST_LOG`; this server uses `GEOENGINE__LOGGING__LOG_SPEC` and the separate worker log spec.

Open these tunnels in separate terminals:

```bash
k port-forward service/geoengine-edv-staging-jaeger 16686:16686
k port-forward pod/"$POD" 3030:8080 6770:6770
```

Open <http://127.0.0.1:16686>. Traces appear under `Geo Engine` and `Geo Engine GDAL Worker`. A worker creates a separate trace **per read**, rather than one unbounded trace for its lifetime. IPC carries a `read_id`, not W3C context; join worker/companion spans by that attribute. Concurrent GDAL followers have their own proposed IDs; use the leader companion's `read_id` and `tile_key`/span links when following shared reads.

Hotpath is available at <http://127.0.0.1:6770/functions_timing>. For a live console, install the pinned CLI with `cargo install hotpath --version 0.28.6 --features tui --bin hotpath --locked -j 1`, then run `hotpath console`. Worker endpoints use localhost ports `6800 + worker_id` inside the pod; collection below queries them without opening 32 tunnels. Do not expose these profiling ports through public ingress.

## 3. Select a real Sentinel-2 layer and time

Use `edv/bootstrap2`'s existing imported Sentinel-2 EPSG:32632 `true_color_tci` layer. The import report identifies backend layer IDs:

```bash
python3 - <<'PY'
import json
from pathlib import Path
p = Path('edv/bootstrap2/.build/import-result.json')
for key, value in json.loads(p.read_text())['resources'].items():
    if key.startswith('layer/sentinel2/epsg32632/'):
        print(key, value['backendId'])
PY
```

The report inspected locally lists TCI `0a763c28-6e99-4246-89a4-79803675ec18` and NDVI `f50253c2-e367-4e15-aaaa-de3798ef89f3`. Confirm these against the selected cluster's catalogue or copy the actual layer ID from the browser. The **data connector** is the internal layer database (`ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74`), not the STAC provider (`c385386d-484e-5b40-9c56-0a6ca9f07243`). The imported layer already contains the STAC provider in its source definition.

Bootstrap2's TCI workflow is `BandFilter(B04,B03,B02) → MultiBandGdalSource` over the `epsg32632_u8_10` dataset, reading the JP2 TCI asset. NDVI is a separate experiment with different bands/compute. Run bootstrap import only for a fresh profiling catalogue: it does not resume a partially populated EDV catalogue. Do not re-import a working staging database just to profile it.

Set a session from the UI or use an anonymous session when permissions allow. Keep it in an environment variable; the tools never save it:

```bash
read -rsp 'Geo Engine session token: ' GEOENGINE_TOKEN; echo
export GEOENGINE_TOKEN
export LAYER_ID=0a763c28-6e99-4246-89a4-79803675ec18
export SCENE_TIME=2026-04-01T00:00:00Z
python3 "$TOOLS/tiles.py" --layer-id "$LAYER_ID" --list-matrices --out "$RESULTS/matrices"
```

`SCENE_TIME` is the bootstrap configuration's default, **not a verified cloud-free acquisition**. Start with Marburg `(8.77, 50.81)` inside UTM 32N. Inspect one small viewport's PNGs and ensure traces have STAC files and GDAL reads. A transparent PNG with HTTP 200 is not evidence of imagery. Choose a confirmed acquisition/day through Odyssey if necessary; record the exact value. Avoid an unnecessarily broad time interval that loads many acquisitions.

The UI uses `CustomWebMercator`. Its zoom IDs are layer-specific. `--resolution 38.218514` chooses the closest **projected** m/pixel cell size (roughly standard WebMercatorQuad z12); `--zoom N` chooses an exact listed matrix. At this latitude, projected meters differ from ground meters. The script computes row/column using `pointOfOrigin`, `cellSize`, and tile dimensions from the backend; it does not assume a 256-pixel global XYZ grid.

## 4. Run inside Kubernetes and collect process data

Create the workload ConfigMap and short-lived session Secret, then render the Job with the chosen layer/time. Avoid printing the session value in shell tracing:

```bash
k create configmap odyssey-profile-tools --from-file="$TOOLS/tiles.py" \
  --dry-run=client -o yaml | k apply -f -
printf '%s' "$GEOENGINE_TOKEN" | k create secret generic odyssey-profile-session \
  --from-file=token=/dev/stdin --dry-run=client -o yaml | k apply -f -
# The Job deliberately remains alive for an hour after the workload so results can be copied.
python3 - "$TOOLS/job.yaml" "$RESULTS/job.local.yaml" <<'PY'
import os, sys
from pathlib import Path
s = Path(sys.argv[1]).read_text().replace('REPLACE_WITH_IMPORTED_SENTINEL2_EPSG32632_TCI_LAYER_ID', os.environ['LAYER_ID'])
s = s.replace('2026-04-01T00:00:00Z', os.environ['SCENE_TIME'])
Path(sys.argv[2]).write_text(s)
PY
```

In one terminal, start a 3-minute process sampler on the **backend** pod before starting the Job:

```bash
k exec -i "$POD" -c "$DEPLOY" -- python3 - --seconds 180 \
  < "$TOOLS/pod_snapshot.py" > "$RESULTS/processes.jsonl"
```

In another terminal, collect initial hotpath counters and start the Job:

```bash
k exec -i "$POD" -c "$DEPLOY" -- python3 - --hotpath --workers 32 \
  < "$TOOLS/pod_snapshot.py" > "$RESULTS/hotpath-before.jsonl"
k create -f "$RESULTS/job.local.yaml"
k wait --for=jsonpath='{.status.active}'=1 job/odyssey-profile --timeout=120s
export JOB_POD=$(k get pods -l job-name=odyssey-profile -o jsonpath='{.items[0].metadata.name}')
k wait --for=condition=Ready pod/"$JOB_POD" --timeout=120s
k logs -f "$JOB_POD"
```

The log prints the selected matrix, each viewport summary, and `workload_exit=0` when complete. Stop following logs after this line; the container intentionally sleeps. If a run can exceed 180 seconds, extend the sampler's duration. To check completion separately, use `k exec "$JOB_POD" -- test -e /results/done`. The Job won't become Complete until its retention sleep ends.

After workload completion:

```bash
k cp "$JOB_POD:/results/run" "$RESULTS/run"
k exec -i "$POD" -c "$DEPLOY" -- python3 - --hotpath --workers 32 \
  < "$TOOLS/pod_snapshot.py" > "$RESULTS/hotpath-after.jsonl"
k logs "$POD" -c "$DEPLOY" --since=30m --timestamps > "$RESULTS/backend.log"
k get pod "$POD" -o json > "$RESULTS/backend-pod.json"
k top pod "$POD" --containers > "$RESULTS/top.txt"
# Allow OTLP batch export to complete before querying Jaeger (typically a few seconds).
sleep 10
python3 "$TOOLS/traces.py" --run "$RESULTS/run/run.json" \
  --processes "$RESULTS/processes.jsonl" \
  --out "$RESULTS/traces.json" > "$RESULTS/span-summary.txt"
```

Keep the sampler running through the last request. Worker/server live snapshots are the primary hotpath artifacts: workers are killed by the pool on shutdown and cannot be relied on to emit exit reports. Hotpath totals include all traffic since process startup; use before/after counts/totals or a fresh dedicated pod for an isolated experiment. Do not subtract percentiles from two snapshots. `HOTPATH_SHUTDOWN_MS` terminates processes and must not be set for a serving deployment.

For a developer-machine smoke test instead of the Job:

```bash
python3 "$TOOLS/tiles.py" --layer-id "$LAYER_ID" --time "$SCENE_TIME" \
  --center 8.77 50.81 --resolution 38.218514 --size 3 --concurrency 6 \
  --repeats 3 --save-png --out "$RESULTS/local-smoke"
```

Use `--anonymous` instead of setting a token if anonymous access is enabled. The script records actual URLs in `urls.txt`, timings in `requests.jsonl`, and summaries/run identity in `run.json`. Client times over `kubectl port-forward` include tunnel latency/bandwidth; compare in-cluster results for backend conclusions.

## 5. Controlled experiment matrix

Use a new output directory and Job name for each run, or delete the completed workload Job before recreating it. Edit the rendered Job's script arguments for each case. Record commit/image digest, backend pod UID, CPU limit, pool limits, raster tile size, cache sizes/TTLs, STAC grid target, GDAL global settings, JP2 driver, and competing traffic. Keep the backend replica count at one for controlled cache experiments; a service can otherwise route identical requests to different caches.

| Experiment       | Variables                                                                    | What it answers                                                 |
| ---------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------- |
| Single tile      | `--size 1 --concurrency 1 --repeats 3`                                       | Request critical path and cache effect                          |
| Viewport         | `--size 3 --concurrency 1`, then 6, then 12                                  | Serial adapter versus useful cross-request overlap              |
| Zoom/resolution  | projected cell sizes about 153, 38, 9.55 m/pixel                             | Input amplification, overview selection, decode cost            |
| Exact duplicates | `--size 1 --duplicates 6 --concurrency 6 --repeats 1`                        | Which layers coalesce, versus duplicated reprojection           |
| Warm repeat      | same URLs/time, three repetitions                                            | Completed raster cache versus STAC/GDAL state reuse             |
| Pan              | center moved one tile width at the chosen matrix                             | Neighboring input reuse and worker affinity                     |
| Cache variants   | raster cache enabled/disabled; STAC cache separately controlled              | Which cache avoids which cost                                   |
| Worker limits    | active 4/8/16, then per-dataset 2/4/8                                        | Queue relief versus CPU/S3 saturation                           |
| CRS boundary     | same WebMercator viewport, one UTM workflow then both adjacent UTM workflows | UI overlapping-layer fan-out; quantify each workflow separately |
| NDVI             | use the imported NDVI layer, same viewport/time                              | Band-specific JP2 reads versus TCI RGB                          |

A pod restart clears process-local raster/STAC/worker caches; it does not clear S3/server-side caches, shared OS caches, or PostgreSQL buffers. Call this **process-cold**, not globally cold. Fresh timestamps/URLs do not necessarily bypass the operator cache. `Cache-Control: no-cache` on an HTTP request does not disable Geo Engine's internal caches. Disable raster caching via `GEOENGINE__CACHE__ENABLED=false` for the relevant experiment; STAC admission via `GEOENGINE__STAC_CACHE__SIZE_IN_MB=0`/TTL 0 is independent and can still coalesce concurrent searches. Be deliberate about STAC rate limits when doing this. Restore settings between cases.

Repeated adjacent queries can still do real duplicate work even with the existing GDAL and STAC coalescers. Count `shared=true/false`, STAC initializer events, repeated raster-cache misses for the same key, and subquery amplification together before proposing another cache or request-sharing layer.

## 6. Interpret the evidence

`traces.py` prints inclusive span distributions and counters. The root `Request.http_handler_ms` records handler response latency; span lifetimes can extend while detached work retains a parent span. Use client/viewport timings and this field to check request wall time. Inspect the slowest complete tile traces in Jaeger, filtering `profile_run=<run_id>`; use `profile_request=<repeat>-<index>` to join a successful client request even when the existing middleware omits its `x-request-id` response header. Check exported tile-span count against client request count, OTLP failures, and Jaeger retention before comparing runs.

| Measurement                                                                | Meaning and optimization signal                                                                                                                                           |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ogc_tile`, `ogc_load_layer`, `ogc_initialize`, `ogc_reproject_initialize` | Request/DB/operator setup; large setup suggests metadata/initialization reuse opportunities                                                                               |
| `raster_cache_get` outcome and key                                         | Actual per-operator tile hit/miss/error; concurrent same-key misses reveal work not coalesced at this cache                                                               |
| `raster_cache_compress/decompress`, `blocking_work`                        | Warm cache CPU and compression queueing; compare cache savings with cache maintenance cost                                                                                |
| `operator_cache_query`                                                     | Legacy cache wrapper including source-stream setup; not a pure lookup timer                                                                                               |
| `stac_cache_get`, `stac_cell_lookup`, STAC events                          | Hit versus shared lookup; only `fetch_started` is a real initializer. A shared lookup may find a just-completed result; don't label every non-hit a distinct network miss |
| `stac_search`, `stac_page`, `items`                                        | Searches/pages and returned STAC item count per request/cell                                                                                                              |
| `http_attempt` errors/status, `http_retry_backoff`                         | HTTP failures including 429; backoff is intentional waiting, not CPU                                                                                                      |
| `stac_http_headers`, `stac_body_and_json`                                  | Header wait, then body transfer plus JSON decoding. Neither is a pure CPU timer                                                                                           |
| `raster_subquery_tile.input_tiles`                                         | Input tiles consumed per output **band**. Compare across zoom/CRS and include empty tiles. Spans contain the input query and aggregator type                              |
| `gdal_read_dispatch.shared`, `tile_key`                                    | Logical reads that join existing work; distinguish callers from physical reads                                                                                            |
| `gdal_broker_dispatch.queue_ms`                                            | Submission-to-dispatch, including broker-channel backpressure. `gdal_broker_send` isolates channel send wait; queue_ms is a field, not the dispatch span's duration       |
| `active_global`, `active_dataset`, `pending_dataset`, affinity             | Scheduling constraints at dispatch. These are samples, not a continuous queue-depth gauge                                                                                 |
| `gdal_worker_companion`                                                    | IPC send → worker response, with parent request context; includes worker read/open and serialization                                                                      |
| `gdal_worker_read_tile`, `gdal_dataset_open/cache_hit`                     | Actual physical reads and per-worker open-dataset reuse; join via read_id                                                                                                 |
| `gdal_rasterband_read`                                                     | GDAL RasterIO, buffer/mask handling, network and decode combined; source/output dimensions show resampling/read amplification                                             |
| `reprojection_coordinate_grid`, `reprojection_fold`                        | Coordinate transformation and input-to-output copying; hotpath aggregates these functions                                                                                 |
| `spawn_blocking` → `blocking_work`                                         | Start gap estimates blocking pool queueing; durations include scheduler/Rayon waiting and are not CPU accounting                                                          |
| `compute_rgb_tile`, `png_encode`                                           | Colorization versus PNG encoding after stream draining                                                                                                                    |

The subquery adapter computes one output tile/band at a time and folds its input stream serially. Its long span can be dominated by nested source work. A serial waterfall with **idle CPU, available workers and pending independent reads** supports prefetch/interleaving experiments. A busy 8-vCPU container decoding JP2 will not benefit automatically from more concurrency. The new raster cache also requests missing tiles sequentially inside its stream; inspect that layer as well.

For `processes.jsonl`, match PID **and start_ticks** across samples. `delta(cpu_seconds)/delta(wall_seconds)` gives occupied cores for each process, including its native threads. Compare server versus aggregate GDAL worker CPU; sum neither RSS nor shared pages as an exact pod memory value. `memory.current` is the pod/container cgroup view. Use **deltas** of `nr_throttled`, `nr_periods`, and `throttled_usec`, not lifetime counters, to identify CPU quota contention. High elapsed read time + low worker CPU suggests waiting; high worker CPU points toward GDAL decoding/resampling, to be confirmed by native stacks. Read latency minus process CPU is not an exact I/O duration, particularly with native threads and shared CPU quotas.

`/proc/<pid>/io` and cgroup `io.stat` report kernel file I/O, **not S3 payload bytes**. They must not be used to calculate S3 throughput. Hotpath's Rust `io!` wrappers cannot see GDAL's native libcurl traffic. Obtain actual remote request count/bytes/latency from an S3 access log or a controlled GDAL-specific diagnostic run, rather than adding a second Rust HTTP client on the tile path.

### Separate JP2 CPU from remote I/O when GDAL dominates

1. Select a slow physical read from its worker trace. Record dataset path/asset, band, source x/y/width/height, output dimensions, driver, and options. Raster read fields include the source window and output size.
2. Repeat that same read in a diagnostic pod on remote JP2, a downloaded local copy of that JP2, and (optionally) a COG with equivalent pixels/overviews. Keep source window, output size, resampling, codec thread settings and native cache state fixed. Use `gdal_translate -b BAND -srcwin X Y WIDTH HEIGHT -outsize OUT_WIDTH OUT_HEIGHT -of GTiff INPUT /tmp/read.tif` under `/usr/bin/time -v`; record output writes separately. This isolates a native read and does not reproduce the complete Geo Engine pipeline.
3. Collect native CPU stacks in the worker process(es), using `perf`/`samply` in an appropriately permitted diagnostic environment. Include GDAL, OpenJPEG and libcurl symbols when possible. Attach to the GDAL **workers**, as server-only sampling misses codec CPU. Worker process IDs are in the sampler; avoid dumping command lines/environments, which contain configuration credentials.
4. Match high CPU to codec/resampling stacks, or low CPU to remote request latency/bytes. A local-file improvement is evidence of a remote/caching contribution, not a precise transfer-time subtraction: it also changes cache behavior.

GDAL's curl debug output can include authentication headers or signed URLs. If used, enable it only on an isolated diagnostic workload, redact it before copying/sharing, and keep it out of routine `backend.env`. Native-library thread counts and overview availability must be recorded before judging worker-pool tuning.

The first ranking should report, for each hypothesis: affected code path, fraction of slow request critical path, calls/input amplification per viewport, CPU/queue/network evidence, expected saved work, and an A/B experiment. Do not rank by summed inclusive span times: nested spans double-count, and overlapping requests can sum to much more than wall time. Hotpath is scoped to instrumented Rust functions/processes, not a distributed/native-stack profiler.

Use these code paths when the corresponding measurements dominate:

| Evidence                                        | Code to investigate                                                                                                                                                                                             |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Serial subqueries / large amplification         | [RasterSubQueryAdapter](../../operators/src/adapters/raster_subquery/raster_subquery_adapter.rs), [reprojection accumulator/fold](../../operators/src/adapters/raster_subquery/raster_subquery_reprojection.rs) |
| Raster misses, duplicate computation, cache CPU | [new raster cache](../../operators/src/cache/new_raster_cache/mod.rs), [legacy cache wrapper](../../operators/src/cache/cache_operator.rs)                                                                      |
| STAC pages, shared searches, retries            | [STAC loading info](../../services/src/datasets/external/stac/loading_info.rs), [STAC cell cache](../../services/src/datasets/external/stac/cache.rs), [HTTP retry](../../services/src/util/retry.rs)           |
| GDAL queue saturation or poor locality          | [pool broker and sharing](../../operators/src/source/gdal_worker_process/process_pool.rs)                                                                                                                       |
| Open/read/JP2 costs                             | [GDAL dataset holder and RasterIO](../../operators/src/source/gdal_worker_process/process_impl.rs)                                                                                                              |
| RGB / PNG costs                                 | [raster stream rendering](../../operators/src/util/raster_stream_to_png.rs)                                                                                                                                     |
| UI fan-out or matrix selection                  | [map layer](../../../ui/projects/core/src/lib/map/map-layer.component.ts), [Odyssey viewport](../../../ui/projects/eodyssey/src/app/main/main.component.html)                                                   |

## 7. Cleanup and rerun

```bash
k delete job odyssey-profile
k delete configmap odyssey-profile-tools
k delete secret odyssey-profile-session
```

Restore the image/env change through the same GitOps process, preserving unrelated changes. `rollout undo` is suitable only if no intervening changes or incompatible migrations occurred. Stop the port-forwards and sampler, unset `GEOENGINE_TOKEN`, and retain raw traces/JSONL, exact URLs, settings and image digest beside the comparison report. Export traces before restarting Jaeger if its storage is ephemeral; live hotpath snapshots must be collected before killing the backend/workers.

References: [hotpath 0.28.6](https://docs.rs/hotpath/0.28.6/hotpath/), [hotpath configuration](https://hotpath.rs/configuration), [GDAL virtual filesystems](https://gdal.org/en/stable/user/virtual_file_systems.html). The live snapshot routes in this implementation were verified against the pinned crate source (`/functions_timing`, `/threads`).
