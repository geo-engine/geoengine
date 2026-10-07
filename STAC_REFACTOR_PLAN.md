# STAC cleanup review and implementation plan

Review the current uncommitted backend changes and preserve the projection-aware
grid, Moka request sharing, cancellation survival, retries, pagination, regular
time axes, extraction, API schema, and database behavior.

1. Inline `cache/tests.rs` into `cache.rs` and remove the empty `cache/` directory.
   Move `grid_request_tests.rs` HTTP/loading tests into `loading_info.rs`; move
   its provider configuration/database test into `mod.rs`. Keep tests beside
   production logic. Introduce a `#[cfg(test)]` helper module only if fixtures
   are actually shared by multiple modules.
2. Replace the STAC positional constructor chain with a definition-based
   constructor using the existing `StacDataProviderDefinition`. Update callers
   and remove all STAC `clippy::too_many_arguments` allowances. Keep validation
   and asynchronous authentication initialization explicit.
3. Share regular-step validation/traversal without eagerly allocating steps on
   tile queries. Remove the redundant `fill_regular_steps` wrapper and avoid
   unused time-step accumulation in the new cell search. Simplify the new tile
   comparator/deduplication with named helpers and preserve its identity rules.
4. Add Rust documentation to complex new helpers only. Describe grid rounding,
   edge/point membership, lazy traversal, full-cell searches, cache admission,
   shared errors, and cancellation where appropriate. Correct stale WGS84 grid
   comments. Narrow visibility exposed solely for separate test modules. Add
   empty lines between validation, setup, traversal, and result assembly.
5. Review the implementation against the pre-refactor snapshot. Run all STAC
   tests, relevant migration checks, `cargo fmt --all -- --check`,
   `git diff --check`, and services library/test Clippy with `-D warnings`.
   Coordinate Cargo builds with `CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2`.

GPT-6 Luna performs the actual refactoring; the root agent reviews and verifies.
Keep unrelated working-tree changes and generated API/client artifacts intact.

## Completed

GPT-6 Luna implemented the cleanup; root reviewed the result and requested the
final import, allocation, documentation, and spacing fixes. Cached cell results
now contain tile files only; the provider-defined regular time axis is assembled
once per query. All 93 original STAC tests remain present and pass.

Validation passed:

- All 93 STAC tests.
- Full migration chain and migration/current-schema equivalence checks.
- Services library/test Clippy with `-D warnings`.
- Workspace formatting and whitespace checks.

Database checks used an isolated temporary PostgreSQL instance with the required
extensions; that instance has been stopped. Existing tracked changes outside
the STAC modules remain exactly as they were at the start of this review.

## Follow-up: redundant wrappers and comparison traits

GPT-6 Luna removed the single-field `StacQueryResult` wrapper and the separate
normalization, subset, and comparison helpers. Cached results now use
`Arc<Vec<TileFile>>`; each query filters its own tile vector with `retain` before
sorting and deduplicating. A private borrowed `StacTileIdentity` derives `Eq`
and `Ord`, using the same fields for both operations and treating signed zero
consistently. Shared geometry and time-interval trait semantics stay unchanged.

Regular time-step callers now construct the iterator directly, and WGS84 bounds
use the existing bounding-box intersection operation. Cache cancellation,
shared errors, and admission behavior remain covered by the existing tests.

Validation passed: all 95 STAC tests (the original 93 plus two identity
regressions), services library/test Clippy with `-D warnings`, workspace
formatting, and whitespace checks.
