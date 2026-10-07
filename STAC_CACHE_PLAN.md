# STAC cache: Moka implementation

The custom/Moka comparison and all preceding working-tree changes are saved in
`checkpoint/stac-cache-backends-2026-10-06`, commit
`d8843ee99e0ddf3c41fbb7736643d23c05488bae`. Development continues on
`feat/stac-request-grid` with one concrete `StacQueryCache` using Moka.

## Refactoring plan

1. Move the Moka implementation into `cache.rs` and name its concrete struct
   `StacQueryCache`. Remove the custom implementation, backend aliases,
   reexports, selection annotations, and comparison benchmark. Preserve the
   provider-facing constructor and `get_or_fetch` interface.
2. Preserve exact request keys, coalescing, cancellation survival, shared terminal
   errors, panic recovery, successful non-retained results, and TTL behavior.
   Remove the byte estimate's dependency on the custom entry layout and estimate
   the retained Moka key/value and result payload instead.
3. Keep one direct test suite without backend traits, boxed fetch adapters, or
   duplicate backend contracts. Update the grid design to reflect Moka.
4. Review the change and run the full STAC tests, formatting check, and services
   library/test Clippy with warnings denied.

## Request handling

Each provider owns its cache. Keys comprise the full dataset identity, integer
STAC cell index, and exact layer time-step start/end. A short-held dataset registry
assigns compact identifiers using full `PartialEq`, including fields containing
floating-point values. The provider's fixed grid configuration and each dataset's
CRS extent determine cell geometry; see `STAC_GRID_PLAN.md`.

Warm requests return the cached `Arc<StacQueryResult>`. On a miss, a detached Tokio
task calls Moka's `try_get_with`. Moka owns same-key initializer matching and
publishes its result to attached callers. Different keys progress independently.
Dropping a caller does not stop the initializer or its HTTP pagination/retries.

Fetch failures and synchronous/asynchronous initializer panics become shared
errors and are not retained. Later requests can retry. Successful results that
cannot be retained (zero budget, zero TTL, oversized result, or a weight exceeding
Moka's `u32` range) travel through an internal error variant so current waiters
share the same successful result without leaving a reusable cache entry.

## Retention

Use the existing STAC TTL and byte-budget settings. Moka's default admission and
eviction policy applies to weighted entries. The weight estimates the key, value,
and result payload; it excludes Moka internals, allocator overhead, dataset
registry, and active requests. Capacity enforcement is best effort and concurrent
insertion can temporarily exceed the configured weight.

## Validation coverage

Retain checks for warm reuse, full dataset identity, cell/time key separation,
independent requests, cancellation, coalescing without retention, shared errors,
synchronous/asynchronous panics and retry, TTL expiration, and weighted capacity
after Moka maintenance. Provider tests continue to exercise grid requests,
pagination, extraction, and request sharing.

The current implementation passes all 93 STAC tests, `cargo fmt --all -- --check`,
`git diff --check`, and services library/test Clippy with `-D warnings`.
