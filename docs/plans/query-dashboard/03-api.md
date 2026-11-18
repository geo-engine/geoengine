# 3. Design the monitoring API

Parent: [whole PR](README.md). Requires: [collector](02-collector.md). Next: [manager UI](04-manager-ui.md). Acceptance: [validation](05-validation-rollout.md).

## Authorization and integration

Propose a read-only `/queryMonitoring` resource under the existing configured API prefix. Implement handlers in `services/src/api/handlers/query_monitoring.rs`, API-facing DTOs in `services/src/api/model/`, and register routes and utoipa schemas in `api/apidoc.rs`. Require an authenticated admin on every endpoint using existing server admin checks; the manager login guard alone is insufficient. Anonymous and ordinary registered users cannot inspect other executions. Use existing error envelopes and auth status conventions.

Return `Cache-Control: no-store`. Only allowlisted graph labels and dataset/provider identities are exposed. Authenticate and authorize before looking up query IDs. Do not add mutation or cancellation endpoints.

## Proposed endpoints

| Method/path                              | Parameters                    | Result                                                                                                                                     |
| ---------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `GET /queryMonitoring/overview`          | none                          | Sampled active count; root and operator raster totals since tracking start; vector counts separately; coverage and saturation diagnostics. |
| `GET /queryMonitoring/history`           | `from`, `to`, `bucketSeconds` | Bounded buckets with active-at-sample gauge and root/operator output deltas; requested/available ranges and effective resolution.          |
| `GET /queryMonitoring/queries`           | `active=all                   | true                                                                                                                                       | false`, optional `state`, `order=asc | desc`, `limit`, `cursor` | Query summaries ordered by `(startedAt, queryId)`; default descending, limit 50, maximum 200. |
| `GET /queryMonitoring/queries/{queryId}` | query UUID                    | Summary, executable graph, requested/runtime mapping, per-node outputs and source references.                                              |

Every response includes `instanceId`, `trackingStartedAt`, `sampledAt`, `snapshotSequence`, `enabled`, retention coverage and incomplete-detail counts where applicable. Counts are decimal strings; `durationMs` is a bounded numeric elapsed duration. IDs are UUID strings. Dates are UTC RFC 3339. State enum is initializing/running/completed/failed/cancelled/dropped, plus a separate finalizing flag. Active is initializing or running. Output counters explicitly distinguish root raster tiles from all operator raster outputs and vector chunks.

Example overview payload (illustrative contract, not generated code):

```json
{
  "instanceId": "a348f6fb-5685-4daf-88c5-43a41167ac14",
  "enabled": true,
  "trackingStartedAt": "2026-11-13T10:00:00Z",
  "sampledAt": "2026-11-13T10:05:00Z",
  "snapshotSequence": "150",
  "activeQueries": "3",
  "rootRasterTiles": "240",
  "operatorRasterTiles": "960",
  "activeQueriesWithoutDetails": "0"
}
```

Overview and history read the same published generation when requested together through an optional overview `includeHistory` parameter; otherwise the UI uses instance/sequence/time to identify sampling differences. Never promise independent polling requests form an atomic transaction.

## Pagination and history rules

Cursor pins an immutable list snapshot, filters, ordering, instance and final `(startedAt, queryId)` key. Keep a bounded short-lived snapshot cache with explicit expiry; reject expired/mismatched cursors using existing error conventions and a typed reason. Do not paginate live active-state filters without snapshot pinning: state changes would skip records. Snapshot cache has byte/count/TTL limits and cannot retain arbitrary graphs; list summaries only. Restart invalidates cursors. UI refresh starts a new first-page snapshot.

Validate date order, maximum history window, bucket size and returned bucket count (proposed maximum 1,000). Return effective aligned bucket boundaries. Aggregate larger buckets using the last active gauge sample and summed tile deltas, documenting this gauge rule. Missing time is a gap, never an invented zero. Reject malformed filters, invalid UUIDs, conflicting `active`/`state` combinations and unsupported resolutions with typed validation errors.

Unknown or evicted query returns 404 with a retention-aware message; do not promise 410 without tombstones. Disabled monitoring returns an enabled=false envelope with no recorded metrics, not fabricated zeros. Tracking coverage lists supported entry-point categories; incomplete instrumentation is visible. Wrong-instance detail lookup is distinguishable only when caller supplies the expected instance ID, otherwise it is unknown.

## Delivery checklist

- [ ] Implement DTO conversions separate from operator/service internals.
- [ ] Test admin/ordinary/anonymous access, no credential leakage, limits, cursor expiry and disabled mode.
- [ ] Register every path/schema and document counter, lifecycle and retention semantics.
- [ ] Run `just backend generate-openapi-spec`, then `just api-clients build`; commit generated OpenAPI and Python/Rust/TypeScript clients together.
- [ ] Run generated-code freshness checks; never hand-edit generated clients.
