# Experimental query dashboard

Status: design proposal; implementation has not started. This experimental PR establishes the design and implementation checklist, rather than claiming a working dashboard.

## Intended result

Add a manager dashboard under `ui/projects/manager` showing all tracked executions on the selected server: current active count, active queries over time, successful raster tiles produced, and a start-date-sorted list filterable by active state. Selecting an execution shows its processing graph, per-operator output counts, elapsed duration, and involved internal datasets and external provider data.

A query is one top-level execution, not a registered processing graph, an operator sub-query, an HTTP connection, or a background task. Repeated executions of the same graph get different query IDs. Active includes initializing and running, including time waiting for input or a consumer. Finished executions remain available within explicit retention limits. “All” means all instrumented entry points on this server instance within retention, not historical executions before tracking was enabled.

## Ordered subgoals

| Order | Plan                                               | Deliverable and exit condition                                                                                                   |
| ----- | -------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| 1     | [Collection feasibility](01-data-collection.md)    | Verify entry points, identities, counter semantics, caches, graph transformations and data resolution before implementing hooks. |
| 2     | [Collector design](02-collector.md)                | Bounded live registry, exact lifecycle transitions, successful-output counters, retained history and deterministic tests.        |
| 3     | [API design](03-api.md)                            | Admin-only snapshot, history, list and detail contracts, OpenAPI and regenerated clients.                                        |
| 4     | [Manager UI design](04-manager-ui.md)              | Accessible overview and query detail, using the generated client and existing manager navigation.                                |
| 5     | [Validation and rollout](05-validation-rollout.md) | Cross-entry-point evidence, measured overhead and explicit experimental limitations.                                             |

Implementation order is 1 → 2 → 3 → 4 → 5. Subgoal 1 is investigated below and in its plan; remaining checkboxes describe future implementation work. Each detailed plan links back here and to its dependencies. Keep these documents updated in the implementation PR as decisions are validated.

## Architecture decision

Use an opt-in, process-local collector in services, with a small observer interface in operators. Count successful output where the consumer sees it, outside cache wrappers; keep quota accounting separate. Create a top-level lifecycle guard before graph initialization, and propagate its observation handle through execution and query contexts. Poll a read-only admin API from manager. Store bounded recent history in memory; do not introduce a database migration in the experiment.

This makes live visibility independent of quota configuration and tracing log levels. It also means restarts reset history and multiple replicas do not automatically yield a cluster-wide view. Every response identifies its instance and tracking start. Cluster aggregation and durable storage are deferred, not silently approximated.

## Completion checklist

- [x] Inspect existing execution, statistics, quota, cache and manager integration points.
- [x] Record proposed collection, collector, API and UI contracts in linked documents.
- [ ] Finish entry-point audit and prove graph-to-runtime mapping with tests.
- [ ] Implement collector and instrumentation.
- [ ] Implement API and regenerate all clients.
- [ ] Implement manager dashboard and detail view.
- [ ] Complete the validation matrix and publish measured results.

## Decisions to verify during implementation

Start with a 2-second registry snapshot cadence, 24-hour history, 10,000 retained terminal query records, and an explicit active-query capacity. These are proposed defaults, not measured safe limits. Benchmark representative workloads before finalizing them. Do not add a cancellation control or percentage-complete indicator: output cardinality is generally unknown, and cancellation is outside this dashboard's scope.
