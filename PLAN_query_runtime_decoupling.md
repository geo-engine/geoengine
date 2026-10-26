# Plan: decouple query execution from the actix HTTP workers

Worktree `/home/droenner/git/geoengine.worktrees/query-runtime-decoupling`, branch `feat/query-runtime-decoupling` from `origin/main` (`d3d04d872`, release 0.10.0). Nothing committed or pushed.

This is step WP2 of _Geo Engine query execution: performance plan_ (shared doc), reduced to one processing runtime. Section 7 says what comes from that plan and what is deferred.

## 1. Context and goal

- actix-server builds every HTTP worker with `Builder::new_current_thread()` (actix-server 2.9.4, `src/worker.rs:348` and `:417`). Each of the 32 workers is a single-threaded runtime.
- Nothing in the query path names a runtime. `crate::util::spawn` / `spawn_blocking` (`operators/src/util/async_util.rs`) use the _current_ runtime. So a request's whole processing graph, including the GDAL dedup leader tasks (`tokio::spawn` in `process_pool.rs`), runs on the one actix worker thread that accepted the request. Only rayon work (`spawn_blocking_with_thread_pool`) and the `gdalsource-process` workers leave that thread.
- **The consumers also do CPU work on that thread.** Moving only the query stream leaves it there:
  - PNG: `blit_tile` per tile in the fold and `to_png` (colorize + encode) in `raster_stream_to_png.rs`
  - WFS: `to_geo_json` plus a `serde_json` re-parse per collection in `vector_stream_to_geojson` (`wfs.rs:614`)
  - GeoTIFF: the write loop, with GDAL writes in `spawn_blocking`, which uses the actix worker's own blocking pool
  - websocket: Arrow IPC encoding (`websocket_stream.rs:50/103`, `spawn_blocking` on the actix worker)
  - dataset tasks: `run_task` spawns with `crate::util::spawn` (`tasks/in_memory.rs:282`), so the whole export runs on the actix worker that scheduled it
- **Queries do async I/O besides GDAL:** `MultiBandGdalSource` loading info queries Postgres at query time (`datasets/postgres.rs:988`), and STAC loading info sends HTTP searches. This I/O moves with the query; the clients behind it (bb8 Postgres pool, STAC HTTP client) were created on the main runtime in `start_server`.
- Consequence: **head-of-line blocking.** A CPU-heavy query or export delays every other connection on the same actix worker, including small API calls and health checks.
- Goal: actix workers only do HTTP I/O. Queries **and the CPU work of their consumers** run on one shared multi-thread "processing runtime"; only finished results or encoded items come back.
- Model: DataFusion's `RecordBatchReceiverStream`. A spawned producer task owns the work, a bounded channel provides backpressure, and the consumer reads a stream. Dropping the consumer cancels the producer, as DataFusion's `SpawnedTask` does with abort-on-drop.
- **Expected effect, from the measurements in section 5:** isolation, not single-query speed. One tokio worker already gets the same per-query speed-up as 32 (5c), so the gain shows up in the mixed-load test, not in criterion benches.

## 2. Design

**Processing runtime.** For this PR: the existing `#[tokio::main]` runtime in `services/src/bin/geoengine-server.rs` (multi-thread, one worker per core; it already runs the GDAL pool broker). `PostgresContext` is built inside it (`start_server` → `PostgresContext::new_with_data`), so `tokio::runtime::Handle::current()` there is that runtime. Store the handle in `PostgresContext` and pass it into `QueryContextImpl`. Reusing this runtime has a second benefit: the Postgres pool and the GDAL broker already live there, so query I/O stays on the runtime its clients were created on.

**QueryContext.** New default method `fn processing_runtime(&self) -> Option<&tokio::runtime::Handle> { None }`, following the existing `new_raster_cache()` default. Only `QueryContextImpl` returns `Some`. Mock contexts and tests return `None`, which spawns on the current runtime, so the same code path runs everywhere.

**Two helpers, by consumer shape** (`operators/src/util/async_util.rs`, re-exported from `crate::util`):

```rust
/// Streaming consumers (websocket, GeoTIFF writer of dataset tasks): items come back through a channel.
pub fn spawn_query<Q, F, O>(processor: Q, query: QueryRectangle<Q::SpatialBounds, Q::Selection>,
                            query_ctx: Box<dyn QueryContext>, capacity: usize,
                            map_item: F) -> BoxStream<'static, Result<O>>
where Q: QueryProcessor + 'static, F: FnMut(Q::Output) -> BoxFuture<'static, Result<O>> + Send + 'static,
      O: Send + 'static;

/// Single-result consumers (PNG, WCS bytes, WFS GeoJSON, plots): query, fold and encode run in one task.
pub async fn spawn_query_task<T: Send + 'static>(
    runtime: Option<&tokio::runtime::Handle>,
    work: impl Future<Output = Result<T>> + Send + 'static) -> Result<T>;
```

- **Why two:** for PNG, WFS and plots the CPU work is in the consumer (fold, encode). Spawning the whole future moves it off actix and costs one hop instead of one channel send per tile. `spawn_plot_query` becomes a use of `spawn_query_task`.
- **`map_item`** runs inside the spawned task, so per-item encoding (Arrow IPC for the websocket) leaves actix too. Identity for GeoTIFF.
- **Ownership (`'static`):** the task owns processor and context. It builds the stream itself with the existing `QueryProcessorExt::query_into_owned_stream`, so operators stay unchanged.
- **Bounded `tokio::sync::mpsc`,** capacity per call site, default `QUERY_TASK_CHANNEL_CAPACITY = 2`. The consumer reads it through `futures::stream::poll_fn(|cx| receiver.poll_recv(cx))`, so no `tokio-stream` dependency. The capacity is also the producer's compute-ahead: a pull-based websocket consumer gets `capacity` tiles prepared while the client decodes the last one.
- **Cancellation:**
  1. The task is wrapped in `tokio_util::task::AbortOnDropHandle`, so dropping the stream or the future aborts it immediately. A failing `send` alone would only notice on the next item.
  2. The existing `QueryAbortRegistration` still works. Callers take `query_ctx.abort_trigger()` _before_ moving the context into the task, and `abortable_query_execution(…, conn_closed, trigger)` stays as it is, with `conn_closed` on the actix side.
- **Errors:** forwarded as stream items. An error while starting the query, e.g. `validate_query`, becomes the first item instead of an early `?`. The consumers (PNG fold, GeoTIFF loop, WFS merge) already propagate `Err` items.
- **Panics:** after the channel closes, the stream awaits the task's handle. A `JoinError` is mapped to `Error::TokioJoin` and yielded as the last item, so a panicked task produces an error instead of a silently truncated result. Log the panic payload at error level before mapping it; otherwise the cause is lost.
- **Tracing:** spawn with `.instrument(tracing::Span::current())`, so spans of the query stay under the request span. The existing `spawn` in `async_util.rs` holds `span.enter()` across `.await`, which `tracing` documents as wrong for async code; don't copy that pattern.

**Call sites to move:**

| site                                                                                                                          | change                                                                                                                                                  |
| ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `operators/src/util/raster_stream_to_png.rs` (single- and multi-band, 2×)                                                     | done with `spawn_query`; **change to `spawn_query_task` around query + fold + `to_png`** so blit and encode leave actix                                 |
| `operators/src/util/raster_stream_to_geotiff.rs` `consume_stream_into_vec` / `raster_stream_to_multiband_geotiff_bytes` (WCS) | done with `spawn_query`; **change to `spawn_query_task`** around query + write + read back bytes                                                        |
| `operators/src/util/raster_stream_to_geotiff.rs` `raster_stream_to_geotiff` (dataset task)                                    | `spawn_query` (done); GDAL writes stay in `spawn_blocking`, which now uses the processing runtime's blocking pool once the task itself moves (next row) |
| `services/src/tasks/in_memory.rs:282` `run_task` (and the cleanup spawn at `:403`)                                            | spawn on the processing runtime; the task manager's locks are tokio locks and work across runtimes                                                      |
| → covers WMS, WCS, OGC API tiles/coverages (`api/handlers/ogc/tiles.rs`, `ogc/common.rs`) and the dataset-from-workflow task  | indirectly                                                                                                                                              |
| `services/src/api/handlers/wfs.rs:614` `vector_stream_to_geojson`                                                             | `spawn_query_task` around query + GeoJSON merge                                                                                                         |
| `services/src/api/handlers/plots.rs:205/209/216` `plot_query`                                                                 | `spawn_query_task` inside the existing `abortable_query_execution`                                                                                      |
| `services/src/workflows/websocket_stream.rs:46/99` `query_into_owned_stream` (raster + vector stream)                         | `spawn_query(p, query_rectangle, Box::new(query_ctx), capacity, encode)` with the Arrow IPC encoding as `map_item`                                      |
| `services/src/contexts/mod.rs` `QueryContextImpl`                                                                             | field `processing_runtime: Option<Handle>`, new parameter in `new_with_extensions`, override `processing_runtime()`                                     |
| `services/src/contexts/postgres.rs`                                                                                           | field `processing_runtime: Handle` from `Handle::current()` in all three constructors, passed in `query_context()`                                      |

**Sites not moved:**

- **Test-only queries,** in `datasets.rs` tests, `layers.rs:2800`, `processing_graphs.rs:1645`, `wms.rs:739` and `operators/src/util/test.rs`. They run on the test runtime anyway.
- **Websocket session loop:** the `actix_ws` session and message stream stay on actix (`processing_graphs.rs:607`); `WebsocketStreamTask` only forwards finished, already encoded items.
- **Workflow load and `initialize`** stay in the handler. They are async I/O and little CPU; move them only if the per-stage traces of the performance plan's WP0 show otherwise.

## 3. Current state (uncommitted)

| file                                                       | change                                                                                 |
| ---------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `geoengine/operators/Cargo.toml`                           | `tokio-util` gets feature `rt` (for `AbortOnDropHandle`; no new dependency)            |
| `geoengine/operators/src/engine/query.rs`                  | `QueryContext::processing_runtime()` default method returning `None`                   |
| `geoengine/operators/src/util/async_util.rs`               | `spawn_query`, `spawn_plot_query`, `QUERY_TASK_CHANNEL_CAPACITY`, plus 3 tests (below) |
| `geoengine/operators/src/util/mod.rs`                      | re-exports `spawn_query`, `spawn_plot_query`                                           |
| `geoengine/operators/src/util/raster_stream_to_png.rs`     | both query sites use `spawn_query`; unused `QueryProcessor` import removed             |
| `geoengine/operators/src/util/raster_stream_to_geotiff.rs` | both query sites use `spawn_query`                                                     |

- **Build:** `cargo check -p geoengine-operators --all-targets` compiles. Services is untouched and not yet checked, so `QueryContextImpl` doesn't return a runtime yet, and everything still spawns on the current runtime.
- **Tests:** `cargo test -p geoengine-operators --lib -- util::async_util` passes 3/3:
  - `it_streams_items_in_order_and_forwards_errors`
  - `it_stops_the_task_when_the_stream_is_dropped`
  - `it_ends_with_an_error_when_the_task_panics`
- **QueryCanceled finding:** the first version of the error test failed. The abort wrapper that `QueryProcessor::query()` puts around every stream ends the stream after an `Error::QueryCanceled` item (by design; see `it_ends_when_inner_yields_query_canceled` in `engine/query.rs`). The test now uses the neutral `Error::QueryProcessor`.
- **Clippy:** strict clippy hasn't run yet.

## 4. Remaining steps

- [ ] `async_util.rs`: add `spawn_query_task`; give `spawn_query` the `capacity` and `map_item` parameters; replace `spawn_plot_query` with `spawn_query_task`; instrument spawned tasks with the current span; log panic payloads before mapping to `Error::TokioJoin`.
- [ ] `raster_stream_to_png.rs`: wrap query + fold + `to_png` in `spawn_query_task` (both variants).
- [ ] `raster_stream_to_geotiff.rs`: wrap the bytes variants (WCS) in `spawn_query_task`.
- [ ] `services/src/contexts/postgres.rs` + `contexts/mod.rs`: store `Handle::current()`, pass it to `QueryContextImpl`, override `processing_runtime()`.
- [ ] `services/src/tasks/in_memory.rs`: `run_task` and the cleanup spawn on the processing runtime (the task manager gets the handle from the context).
- [ ] `services/src/api/handlers/wfs.rs`: `spawn_query_task` around `vector_stream_to_geojson`'s work.
- [ ] `services/src/api/handlers/plots.rs`: `spawn_query_task` (3 variants).
- [ ] `services/src/workflows/websocket_stream.rs`: `spawn_query` for raster and vector, with the IPC encoding as `map_item`.
- [ ] Check that the GDAL dedup leaders now run on the processing runtime. They are spawned from inside the query task, so the "current" runtime there is the processing runtime.
- [ ] Check that no query-path code calls `actix_web::rt` or `actix_rt` APIs, which need an actix runtime (`grep -rn "actix_rt\|actix_web::rt" operators/ services/src/` outside handlers and server setup).
- [ ] `cargo fmt` and `cargo clippy --all-targets -- -D warnings` (in `geoengine/`).
- [ ] Tests:
  - `cargo build --bin gdalsource-process` (this worktree's own target dir), then `export GDAL_SOURCE_PROCESS_PATH=$PWD/target/debug/gdalsource-process`
  - `cargo test -p geoengine-operators --lib` (everything; the PNG/GeoTIFF utils are used widely)
  - `cargo test -p geoengine-services --lib -- api::handlers::wms api::handlers::wcs api::handlers::wfs api::handlers::plots api::handlers::ogc processing_graphs workflows tasks` (needs the local Postgres `geoengine`/`geoengine`)
- [ ] Test: the server context returns `Some(handle)` and a query's task, including its PNG encode, runs on a thread of that runtime (assert on `std::thread::current().name()` inside a test operator).
- [ ] Test: dropping a WMS request future while its query runs ends the task (the existing abort test, run against the server context instead of the mock).
- [ ] Commit `feat(backend): run queries on a shared processing runtime instead of the HTTP worker`, without a Co-Authored-By trailer. Do not push without asking.

**Measurement** (the point of the change):

- **Setup:**
  - release build of both `origin/main` and this branch (`cargo build --release --bin geoengine-server --bin gdalsource-process`)
  - per run: `GEOENGINE__POSTGRES__SCHEMA=agent_decoupling` (unique, **never `agent`**, which other sessions wipe), `GEOENGINE__POSTGRES__CLEAR_DATABASE_ON_START=true`, `GEOENGINE__WEB__BIND_ADDRESS=127.0.0.1:<free port>`
  - port 3030 is the user's server, and 3031/3032 have been used by other sessions. Check with `ss -ltnp`, and **verify the listener pid is your own server** before sending load.
- **Load:**
  - 16 concurrent clients requesting 256 px WMS tiles of `GdalSource(ndvi)` in EPSG:3857 (zoom 4, `time=2014-04-01T12:00:00Z`); a second run with OGC API Tiles of the same layer
  - at the same time, in separate runs: (a) one large WCS GetCoverage, (b) one `processingGraphs/{id}/rasterStream` over the whole NDVI extent, (c) one dataset-from-workflow task, each in a loop
  - a health-check client (`/available`) at 10 req/s, to show head-of-line blocking on small calls directly
  - metrics: WMS/OGC throughput, p50, p95, p99; health-check p99; runtime of the large request
  - CPU time per thread group from `/proc/<pid>/task/*/stat`, grouped by thread name and sampled before and after each run. `/proc` cuts names to 15 characters: actix workers show as `actix-rt|system`, tokio workers and tokio blocking threads both as `tokio-runtime-w`. Rayon threads are unnamed and show the process name, so give the pool a `thread_name` (`ge-rayon-{i}`) in `create_rayon_thread_pool` for this test.
- **Compare:** `origin/main` vs this branch, each with and without the large request.
- **Expected:** on `main`, WMS p95/p99 and health-check p99 rise sharply while the large request runs (head-of-line blocking on its actix worker); on the branch, much less, and actix threads carry a small share of the CPU time. Single-tile WMS without load: no regression beyond noise.
- **Noise:** earlier runs varied about ±30% between runs on the shared machine (section 5d). Run each case 3× and report the median; treat differences below that range as no change.
- **Script:** goes in the session scratchpad `/tmp/claude-1000/-home-droenner-git-geoengine/20193657-c999-45fd-9890-0ab5e14b09ac/scratchpad/decoupling/` (Python `aiohttp`/`httpx` are installed; run with `python3 -I`).
- **Afterwards:** stop every server and process you started.

## 5. Performance data so far (from the sub-query work)

**a. criterion `subquery_gdal`**

- Setup: branch `feat/parallel-subqueries` (stacked on `feat/cheap-nodata-tiles`), NDVI whole extent, 32-core machine, mean of 100 runs, GDAL pool 8, 32 tokio workers.

| per-query parallelism     | 1       | 2             | 4             | 8             | 16            |
| ------------------------- | ------- | ------------- | ------------- | ------------- | ------------- |
| reprojection              | 340 ms  | 219 ms (1.6×) | 181 ms (1.9×) | 157 ms (2.2×) | 163 ms (2.1×) |
| two chained reprojections | 1153 ms | 870 ms (1.3×) | 723 ms (1.6×) | 636 ms (1.8×) | 633 ms (1.8×) |

**b. GDAL pool size makes no difference** (at parallelism 8):

| GDAL processes | 8      | 16     | 32     |
| -------------- | ------ | ------ | ------ |
| reprojection   | 157 ms | 160 ms | 157 ms |
| chained        | 636 ms | 661 ms | 634 ms |

**c. Tokio workers make no difference** (parallelism 1 → 8):

| tokio workers                | reprojection        | chained                                                    |
| ---------------------------- | ------------------- | ---------------------------------------------------------- |
| 1 (close to an actix worker) | 340 → 173 ms (2.0×) | 1165 → 645 ms (1.8×)                                       |
| 32                           | 344 → 169 ms        | → 661 ms (one 1649 ms outlier at fan-out 1, under load 30) |

So inline async work per query is not the bottleneck. **Decoupling mainly helps head-of-line blocking and isolation, not single-query speed.**

**d. WMS load tests**

- Setup: 21 Oct, release server, 256 px tiles, before the nodata rebase.
- Plain NDVI, EPSG:3857: identical for sequential, per-query 4, and per-query 4 + server 16:

| clients | 1    | 4    | 16    | 32    | 64    |
| ------- | ---- | ---- | ----- | ----- | ----- |
| req/s   | ≈ 11 | ≈ 46 | 49–52 | 47–53 | 48–50 |

- Workflow reprojected to 3857, requested in 3857: same picture.
- Chained (workflow 3857, requested in 4326): 5.0 vs 4.9 req/s at 1 client, 11.9 vs 12.0 at 16 (sequential vs parallel). An earlier +23–31% was run-to-run noise: sequential measured 3.9 vs 5.0 req/s between runs, about ±30% on the shared machine.
- The server saturates at about 50 req/s with 4–5 concurrent tile requests. Going from 1 to 4 clients scales about 4× through parallel requests, not sub-queries.

**e. Earlier numbers and a benchmark pitfall**

- Before the nodata rebase, at parallelism 8: reprojection 455 → 156 ms, chained 1143 → 619 ms. Sequential is faster now because of the cheap-nodata-tiles commit.
- Pitfall: the bench created a GDAL pool per case, and pools are never shut down. That leaked workers (up to 340 processes at pool 32) and invalidated those results. Fixed with one shared pool per run. The mock execution context still starts an idle 2-worker default pool per `setup()`.

**What the saturation at 50 req/s means for this PR:** 4–5 concurrent tiles saturate the server on a 32-core machine, so the limit is not the number of actix workers. This PR does not move that number; its per-tile cost (reprojection, resampling, PNG encode) is WP9 of the performance plan. The decoupling test therefore measures latency under mixed load, not peak WMS throughput.

## 6. Open questions, with recommendations

1. **Channel capacity.** Recommended: a parameter of `spawn_query`, not a global constant. 2 for consumers that read as fast as items arrive; for the websocket stream it is the compute-ahead while the client decodes, so make it a setting (`[query_context] stream_read_ahead`, default 2) and let the performance plan's WP8 tune it with the bulk-throughput bench. Single-result consumers need no channel (`spawn_query_task`).
2. **Processing runtime size.** Recommended for this PR: keep the `#[tokio::main]` runtime. 5c shows the worker count does not change per-query speed, and the Postgres pool and GDAL broker already live there. Follow-up (performance plan WP1): build the runtime with `tokio::runtime::Builder` so `worker_threads` and `max_blocking_threads` come from settings; today they are one per core plus Rayon one per core plus 32 actix workers. A second, separate runtime for bulk work only if the mixed-load test shows exports still delay WMS after this PR.
3. **Websocket raster stream.** Recommended: move the query **and** the Arrow IPC encoding (as `map_item`); keep only the session loop on actix. The encoding is CPU work per tile and today runs in the actix worker's blocking pool.
4. **Panics.** Yes: an error for the request, not a crashed worker, and a log line with the panic payload. Rayon work is separate: a panic inside `ThreadPool::spawn` (if the performance plan's WP3 `compute` helper lands) must be caught there too.
5. **Scope.** Separate PRs, yes. This one does not depend on #1361 (`feat/parallel-subqueries`) or #1360 (`feat/cheap-nodata-tiles`). #1361's `subquery_gdal` numbers (5a–c) stay valid after it; rebase whichever lands second.

## 7. Alignment with the performance plan

| Performance plan                                              | This PR                                                                                                                  | Deferred                                                          |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------- |
| WP2: queries off actix, abort on drop, tracing across the hop | yes, with one processing runtime                                                                                         | interactive/bulk runtimes, only if the mixed-load test needs them |
| WP2: a `QueryClass` on every query                            | no                                                                                                                       | add when the first shared queue needs a priority (below)          |
| WP3: CPU work off async threads                               | partly: PNG/GeoTIFF/WFS/IPC encoding leave actix, but still run on the processing runtime's threads or its blocking pool | one `compute` helper on Rayon (`rayon::spawn` + oneshot)          |
| WP8: bulk outputs compute ahead                               | websocket via channel capacity                                                                                           | GeoTIFF writer producer/consumer split                            |
| WP6: admission and class priority                             | no                                                                                                                       | shared queues stay first come, first served                       |

**Limits to keep in mind when reading the measurements:** threads are separated after this PR, but three queues are still shared by all requests in arrival order:

- the GDAL process pool (dataset scheduling with worker affinity, `process_pool.rs`)
- #1361's server-wide sub-query permits, once merged
- #1343's STAC request coordinator, once merged (FIFO per server, no priority)

If WMS p99 still rises during an export after this PR, the per-stage traces should show which of these adds the wait; that queue then gets a class priority (performance plan WP6), and the `QueryClass` above is introduced for it.

**Relation to the tile-overlap stack** (`feature/tile-types` → `feature/retile-and-dataset-origins` → `feature/tile-overlap`, based on `22121e3`): this branch starts from `origin/main` and changes 7 files the stack also changes (`raster_stream_to_png.rs`, `raster_stream_to_geotiff.rs`, `wrap_with_projection_and_resample.rs`, `wms.rs`, `wfs.rs`, `plots.rs`, `contexts/postgres.rs`). The changes here are thin wrappers around the query calls, so landing this PR first and rebasing the stack onto it is the cheaper order; if the stack lands first, rebase this branch onto it. Nothing in the design depends on tile size or overlap.
