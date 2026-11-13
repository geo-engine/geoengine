# 5. Validation and experimental rollout

Parent: [whole PR](README.md). Validates [collection](01-data-collection.md), [collector](02-collector.md), [API](03-api.md) and [manager](04-manager-ui.md).

## Required behavior matrix

| Scenario                                     | Required evidence                                                                                         |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Two simultaneous executions of one graph     | Different query IDs, active=2, independent counters and durations.                                        |
| Repeated operator sub-queries                | One top-level query, summed counters per path, no graph-ID collision.                                     |
| Raster multiband/no-data outputs             | Exact successful emitted-item counts; root totals distinct from pipeline work.                            |
| Stream error / initialization failure        | Failed terminal record, errors excluded from tile count, active decremented once.                         |
| Cancellation / disconnect / early child stop | Root outcome correct; dropped child does not falsely fail root; no leaked active record.                  |
| Root/subtree cache hits and misses           | Consumer-visible outputs counted once outside cache; bypassed children correctly show no observed output. |
| Vector and plot executions                   | Correct lifecycle and separately labeled chunk/result metrics.                                            |
| Graph optimization/reprojection              | Runtime paths and dataset references agree with executable graph; mapping gaps explicit.                  |
| Materialization and task-owned execution     | Included in entry-point coverage with parent task identity and no duplicate top-level registration.       |
| Quotas disabled / tracing disabled           | Dashboard still records queries and successful outputs.                                                   |
| Retention, saturation, graph truncation      | Bounded memory, reliable aggregate metrics and explicit detail gaps.                                      |
| Restart / replica switch                     | New instance ID, history gap, invalid cursors, no cross-instance data merge.                              |
| Permissions                                  | Admin allowed; normal/anonymous rejected; graph and source credentials never exposed.                     |
| Pagination during state changes              | Stable snapshot pages; expiry and changed filters handled predictably.                                    |

## Automated checks

Use fake clocks for sampling, duration and retention; deterministic stream fixtures for success/error/drop; concurrent tests for exactly-once terminal transitions and snapshot races. Verify history deltas survive terminal eviction. Include an integration test for each audited production entry-point category, especially streaming transport ownership and plots. Add API tests for limits, counter serialization, disabled state and secret redaction. Add UI service/utility tests plus browser tests covering overview → detail → filtered return, keyboard navigation and stale/restart states.

Run backend formatting/lints and the appropriate operator/services/server tests, then `just ci backend` before implementation is committed as required by backend instructions. Regenerate spec/clients and run generated freshness checks after API changes. Run manager lint/build/tests using repository recipes and the live local backend. At the current planning stage, this PR changes only Markdown; validation so far covers Markdown formatting and local link integrity. Implementation tests remain required before the dashboard PR is ready for review.

## Performance and rollout gate

Measure disabled baseline versus enabled tracking using identical data, cache settings and concurrency. Include fast cache-only tiles, expensive GDAL work, deep graphs, many short queries and slow consumers. Record throughput, p50/p95 latency, CPU, allocation rate, peak memory, sampler lag and time to visible terminal state. Proposed target is under 5% throughput/latency overhead; treat it as a decision gate to measure, not a demonstrated result. Validate memory ceilings at retention and metadata limits and verify overload never blocks query processing.

Ship disabled by default behind `[query_tracking]`. Enable on one development instance, show an Experimental label and instance scope in manager, then review evidence before wider use. Rollback is disabling tracking and restarting; no persisted schema or migration rollback is required. Document that disabled/restarted collectors cannot reconstruct past executions.

## Completion evidence to fill during implementation

- [ ] Completed entry-point audit with test names and known coverage gaps.
- [ ] Backend tests, generated-code check and manager checks with exact results.
- [ ] Live screenshots/recording of overview, active filtering, node metrics and datasets.
- [ ] Performance table and observed retention/saturation behavior.
- [ ] Confirmed configuration defaults and documented single-instance deployment procedure.
- [ ] Updated high-level checklist and removal or explicit acceptance of unresolved design assumptions.
