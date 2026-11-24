# 4. Design the manager dashboard

Parent: [whole PR](README.md). Requires: [API](03-api.md) and [counter semantics](01-data-collection.md). Acceptance: [validation](05-validation-rollout.md).

## Repository integration

Implement under `ui/projects/manager/src/app/query-monitoring/`. Existing manager navigation uses `NavigationType` and an inline switch in `navigation/navigation.component.{ts,html}`, rather than routed child pages. Add a Queries section there and introduce authenticated admin detail navigation deliberately in `app-routing.module.ts`; preserve the existing datasets/layers/providers navigation. A direct detail URL must load inside the manager shell and retain a return link with list filters. Persist active filter/order/time window in URL query parameters; use a query-ID parameter for shareable details. Ensure configured deployment subdirectories work.

Inspect shared common/core services before adding a wrapper around the generated API. The existing `core/src/lib/provenance/lineage-graph/lineage-graph.component.ts` uses d3/dagre but depends on project service, layer and dialog context. Extract reusable graph layout/rendering only if it can accept a pure graph model without those dependencies. Add counters via an explicit node view model, not edits to generated operator types. Prefer existing chart facilities; do not add a chart dependency without checking current packages.

## Overview layout

```text
Queries                     Server instance …   Updated …   Auto-refresh [on]
[Active queries: 3] [Root raster tiles: 240] [Operator raster outputs: 960]
[Time window: 15m / 1h / 24h]
[Active queries over time — sampled step line]
[Root raster tiles per interval — bars, explicit interval units]
State [Active / Finished / All]    Started [Newest / Oldest]
Started (UTC) | Status | Duration | Output tiles/chunks | Graph | Query ID
… clickable query links …                                   [Next page]
```

Default Active and newest first so the primary page shows current work; Finished and All include retained executions. The active KPI counts initializing and running. Label root tiles as output and all-operator counts as processing work. Vector-only executions show chunks, with raster tiles marked not applicable. Display count scope (“since tracking started”) and retention range. Do not suggest a percentage complete or expected total.

Use independent, vertically aligned charts with the same time axis; distinguish a sampled active gauge from tile interval counts. Label gaps and tracking start, provide a textual/table equivalent and avoid color-only state encoding. A refresh error keeps the last successful data with a prominent stale timestamp. Empty running state differs from disabled monitoring, missing history, capacity-limited detail and permission denied.

## Detail layout

Header: query ID, graph ID, owner ID where allowed, entry point, state, started time, current elapsed or final duration, instance and snapshot time. Duration ticks locally between polls using the server elapsed duration; terminal duration freezes. Do not use browser wall-clock subtraction as the sole source.

Main panel: executable processing graph with type, stable runtime path, successful raster tiles or vector chunks on each node. Selecting a node opens its metrics and source references. Aggregate repeated invocations by path and show invocation count. Offer requested graph mapping; label inserted, removed and unmapped nodes instead of assigning counts to the wrong node. Unknown counters show unavailable, not zero. Provide pan/zoom, fit, keyboard selection and an accessible operator table for large graphs.

Dataset panel: deduplicated internal dataset IDs/names and external provider/layer references, links to manager dataset details when supported, and the operators using each reference. Show declared, resolved and actually accessed status separately. Failed initialization can have incomplete resolution. Deleted datasets and unavailable providers retain identity without breaking the detail page.

## Refresh and state ownership

One injectable monitoring service manages typed API calls and polling. Proposed cadence: 2 seconds for overview/list and selected active detail, history only when its time window changes or a new bucket is available. Cancel obsolete requests on filter, query or route changes; prevent overlapping polls. Pause polling while the tab is hidden and on destruction; refresh immediately when visible. Back off failures, resume on success, and stop terminal detail polling after counters finalize. List refresh resets pagination to a new snapshot; provide a visible refresh action when browsing older pages to avoid jumping rows.

Reject responses from a previous selected query/filter generation. Detect changed instance ID, clear cursors/history and explain restart or replica change. Never merge different instance histories. Build decimal-string formatting that preserves exact counters; chart conversions must be bounded and label any approximation.

## Implementation and accessibility checklist

- [ ] Add Queries navigation and backend-authorized admin visibility, with deep-link/return behavior.
- [ ] Add generated-client service and tests for polling teardown, response races, failures and instance changes.
- [ ] Add KPIs, charts, filterable/sorted paginated list and distinct loading/empty/disabled/stale states.
- [ ] Add graph metrics, accessible node table, durations and dataset references.
- [ ] Verify keyboard links, filter labels, focus after navigation, narrow screens and non-color chart interpretation.
- [ ] Verify live manager behavior against the locally instrumented backend.
