# 1. Determine how to collect query data

Parent: [whole PR](README.md). Next: [collector](02-collector.md). Consumers: [API](03-api.md), [UI](04-manager-ui.md). Status: initial repository investigation complete; integration audit and proofs remain.

## Evidence from the current code

Paths below are relative to the repository root; they are implementation anchors, not proposed new APIs.

| Existing source                                                                                                                    | What it provides                                                              | Gap for the dashboard                                                                                     |
| ---------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| [`QueryContext`](../../../geoengine/operators/src/engine/query.rs)                                                                 | Per-query execution facilities and abort support                              | No retained admin query lifecycle registry.                                                               |
| [`PostgresSessionContext::query_context`](../../../geoengine/services/src/contexts/postgres.rs)                                    | Processing graph ID, computation ID, session and quota handle                 | Context construction alone does not prove execution started or completed.                                 |
| [`QueryProcessorWrapper`](../../../geoengine/operators/src/meta/wrapper.rs)                                                        | Operator path/name, source data label, repeated query count and tracing spans | Root tracing UUID is separate from computation identity; spans are not a live state store.                |
| [`StreamStatisticsAdapter`](../../../geoengine/operators/src/adapters/stream_statistics_adapter.rs)                                | Stream polling and element counts; optional debug timing/bytes                | Current element/quota counting uses `Some(_)`, including errors. Debug metrics are log-level dependent.   |
| [`QuotaTracking`](../../../geoengine/operators/src/meta/quota.rs) and [quota worker](../../../geoengine/services/src/quota/mod.rs) | User/graph/computation identity and per-operator work events                  | Unbounded event channel, buffered persistence, no complete lifecycle and no successful-tile semantics.    |
| [`ExecutionContextImpl`](../../../geoengine/services/src/contexts/mod.rs)                                                          | Raster/vector wrapper construction and named-data resolution                  | Cache wraps statistics, so cache hits may bypass them. Plot wrapping currently adds no stream statistics. |
| [WebSocket stream](../../../geoengine/services/src/processing_graphs/websocket_stream.rs)                                          | Streaming ownership, abort and delivery boundary                              | Disconnect/drop must terminate observation without declaring successful completion.                       |

## Collection contract

Allocate an execution UUID before initialization; reuse the computation UUID only where the audit proves it is unique per top-level execution. Keep processing graph ID separately because it hashes graph content. Never infer query identity from a graph ID or use tracing-generated UUIDs as the registry key. Sub-queries inherit execution identity and aggregate by operator path; identical operators at different paths remain separate.

Capture start at admission to execution, UTC timestamp plus monotonic clock. Terminal outcomes are completed, failed, cancelled, or dropped (reason unknown). Initialization failure must produce a visible failed execution even if no stream existed. Abort signals identify cancellation; disconnect without reliable abort attribution remains dropped. Non-stream plot execution gets a top-level guard too.

Successful raster output is one emitted `RasterTile2D` item, including no-data tiles, for one band. Count only `Ok`; errors and pending polls produce no tile increment. Vector feature-collection chunks and plot results are separate metrics. Sum of all operator tile outputs measures pipeline work and can exceed final output. Overview headline tiles means root-output tiles; expose operator-output tiles separately with a precise label. No total-to-complete estimate is available.

Place observation outside each operator's chosen cache wrapper so cached output is visible once. Keep quota semantics unchanged. Track cache provenance only if the cache boundary can supply it reliably; otherwise leave cache-hit counts unavailable. Record returned output, not speculative work discarded before emission. HTTP image responses served without engine execution do not count as executed queries or produced tiles.

Graph initialization/optimization can insert, remove or replace operators. Retain a bounded requested-graph snapshot and the executable graph metadata with explicit path mapping. Display runtime nodes for counters, with requested-graph correspondence where known. A pruned source is declared but not read. Resolve dataset IDs/provider IDs while initializing, then mark actually accessed sources on successful source output or explicit read hooks; zero emitted tiles alone cannot prove no read happened. Retain declared, resolved and accessed distinctions. Never derive complete dataset involvement solely from tile events or quota strings.

## Entry-point audit to finish

- [ ] Inventory production callers of `SessionContext::query_context` and graph initialization, excluding tests, with `rg`.
- [ ] Trace WMS/WFS/WCS/OGC tiles through `services/src/api/handlers/ogc/common.rs` and WMS helpers.
- [ ] Trace raster/vector WebSocket execution in processing-graph handlers and stream ownership through disconnect.
- [ ] Trace plot handlers, dataset materialization/export, ML jobs and task-owned execution; record parent task ID without counting both task and child query as the same execution.
- [ ] For every path, record the identity creator, admission point, graph transformation, query-context lifetime, output consumer and all termination paths.
- [ ] Prove that queries starting before stream construction, cache-only queries, repeated sub-queries and plot roots are observable.

Exit artifact: commit the completed entry-point table and tests alongside implementation. Any unsupported production path must appear in API coverage metadata and UI copy until instrumented. The experiment cannot claim complete server coverage while that table is incomplete.
