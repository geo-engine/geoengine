# 2. Design the collector

Parent: [whole PR](README.md). Requires: [collection contract](01-data-collection.md). Next: [API](03-api.md). Verify with [validation](05-validation-rollout.md).

## Proposed ownership and interfaces

Add `services/src/query_tracking/` for registry, lifecycle guards, snapshot worker and retention. Own one shared registry in the application context, not one per session or actix worker. Add an optional observer abstraction to operators, accessible through query context and a query-scoped execution context. Operators must not depend on services. Disabled tracking uses a no-op handle and must not require quota or tracing.

A service creates `begin_execution(metadata) -> QueryGuard`. Its handle registers executable operators and resolved source references during initialization, then is propagated through all query contexts and sub-queries. Stream observation adds successful outputs to per-operator atomics; no database writes, allocation or channel message per tile. A root-consumer guard reports explicit completion/error/abort and survives until output exhaustion or drop, not merely until the handler returns a streaming response. Guard drop provides a nonblocking exactly-once fallback.

## Records

| Record            | Fields                                                                                                                                                                                                 |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Execution         | query UUID, optional computation UUID, graph ID, owner user ID, optional parent task ID, entry point, instance ID, start/end timestamps, monotonic duration, state/reason, graph revision, output kind |
| Graph             | bounded requested definition, executable nodes/edges, runtime path → requested path mapping, operator type, output kind, declared/resolved/accessed source references                                  |
| Operator counters | successful raster tiles, vector chunks, optional feature count, errors, invocation count, first activity, optional last activity                                                                       |
| Snapshot          | instance ID, sequence, sampled UTC time, active count, cumulative root/operator outputs, history coverage, capacity/retention diagnostics                                                              |

Use 64-bit internal counters; serialize counters as decimal strings in the API to avoid JavaScript integer rounding. Source references contain authorized dataset/provider identifiers and display names, never credentials, raw provider configuration, filesystem paths or signed URLs. Keep only an allowlisted graph representation suitable for admin display; do not return arbitrary source parameters. Dataset name resolution failure must not fail a query or hide its source ID.

## Lifecycle

`initializing → running → completed | failed | cancelled | dropped`; initialization can transition directly to failed/cancelled/dropped. Terminal transitions use an atomic compare-and-set and decrement active exactly once. Mark running when processing begins. Keep elapsed time based on a monotonic clock; use UTC only for ordering and chart axes. A completed stream means execution output exhausted, not confirmed receipt by the remote client.

Finalize after terminal outcome and all tracked child activities quiesce, or expose a `finalizing` flag until counters freeze. Prevent late child increments after a record is frozen. An operator sub-stream drop is not by itself a top-level failure: early stopping can be intentional. Root output must never be counted a second time by both transport and operator wrappers.

## Consistency and limits

Use atomic hot counters and a sharded registry for admission/lookup. Sampling takes short metadata locks, never across an await. Publish an immutable snapshot with a sequence; APIs read that snapshot rather than walking executing streams. Terminal counters are copied into retained records before active state is released. State and gauges are consistent within a published snapshot; counters may reflect sampling skew across operators and do not claim transactional per-tile consistency.

Sample every 2 seconds into a fixed ring (43,200 buckets for 24 hours). A bucket contains active count at sample time, root/operator output deltas since the prior sample, and interval boundaries. This is a sampled gauge, so a query shorter than the cadence can be absent from active samples while still contributing output deltas and retained history. Do not label it peak concurrency. Maintain process-wide totals independent of terminal-record eviction; never compute history deltas by subtracting sums over retained queries.

Retain terminal records by age and count (proposed 24 hours / 10,000), evicting oldest first. Never evict active queries silently. Configure active-record, per-query operator/source, and graph-byte limits. On active capacity overflow, preserve aggregate active/output tracking with lightweight guards, omit expensive details and expose untracked-detail counts. Truncate metadata with explicit flags; do not block or fail geospatial work. Export dropped-observation and sampler-lag diagnostics. Bound the aggregate-only fallback by sharing counters rather than retaining overflow records.

Memory budgeting must include active metadata and counters, terminal graphs, immutable snapshots and history. Cap graph bytes independently of operator count and deduplicate immutable graph metadata where safe. Settings proposed under `[query_tracking]`: enabled (false), snapshot interval, history duration, terminal capacity, active-detail capacity, maximum nodes/sources and graph bytes. Validate ranges at startup.

Restart creates a new instance ID and empty history. No database migration is needed. A request distributed to another replica cannot retrieve this instance's detail; deployment must pin monitoring to a selected instance until a future aggregator exists.

## Implementation checklist

- [ ] Add no-op observer and query-scoped execution observation without breaking crate layering.
- [ ] Add lifecycle guards covering initialization, buffered responses and lazy transports.
- [ ] Wrap operator output outside caches; implement plot-root outcome handling.
- [ ] Capture graph mapping and resolved/accessed data references.
- [ ] Add atomic counters, immutable snapshots, bounded history and diagnostics.
- [ ] Add validated configuration and fake-clock tests for termination races and retention.
