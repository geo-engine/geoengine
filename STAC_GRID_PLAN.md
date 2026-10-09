# STAC projection-aware square grid

Continue on `feat/stac-request-grid`, preserving the Moka cache, STAC parsing,
HTTP retries/authentication/pagination, regular time steps, extraction, and
deduplication. Replace global longitude/latitude subdivisions with square cells
in each dataset's CRS. GPT-6 Luna implements; root reviews and validates.

## Provider configuration

```json
{"stacGrid": {"targetNumberOfCells": 512}}
```

The optional setting applies to all datasets. Omission uses 512; explicit counts
must be positive signed 32-bit integers. The count is a target: square cells and
complete coverage require rounding row/column counts up. No per-dataset settings,
rotations, distortion compensation, or new concurrency options are added.

## Geometry

1. Obtain the finite projected bounding extent of the dataset CRS using existing
   metadata APIs. Use the CRS extent, not the raster footprint. Reject missing,
   nonfinite, or degenerate metadata clearly.
2. For width W, height H, target N, derive one side length
   `sqrt(W) * sqrt(H) / sqrt(N)`. Validate finite positive arithmetic,
   representable boundaries and checked row/column/index ranges. Counts are
   approximately `ceil(W / side)` and `ceil(H / side)`.
3. Anchor at the extent's lower-left corner and use the same canonical boundary
   formula everywhere. Interior cells are square in CRS units; boundary cells
   keep square geometry but have search footprints clipped to the fixed extent.
4. Enumerate native query intersections lazily, without allocating the grid.
   Handle negative coordinates, exact edges, points and maximum endpoints. A
   positive-area query ending on a shared edge does not add the next cell.
5. Resolve geometry once per loading-info call, outside cell/time loops. Fixed
   CRS/configuration identity makes request keys independent of query arrival.

EPSG:4326 cells are square in degrees, not ground distance. Projected Sentinel
cells follow native coordinates. CRS bounding extents remove global padding but
are not exact polygons of curved valid domains: conversion must handle genuinely
empty intersections and propagate unexpected failures.

## Search and cache integration

Select cells in native coordinates first. Reproject each complete cell's fixed
clipped footprint to WGS84 with the existing clipped/edge-sampled helpers.
Outward-round STAC bboxes to five decimals and clamp geographic limits. Never
crop the cached search to caller bounds. Outside-domain queries stay empty;
invalid/nonfinite queries and invalid CRS definitions remain errors.

Keep full dataset identity + integer cell indices + exact layer-step time keys.
Dataset identity includes projection, so distinct CRSs cannot share filtered
results. Moka still owns coalescing, cancellation survival, errors, retries, TTL
and weighted retention; see `STAC_CACHE_PLAN.md`. Preserve time-only early return
before URL/CRS/grid work, complete pagination, lazy cells/steps, deduplication,
z-index ordering, and extraction to the original native/time subset.

## API and storage

Replace both axis counts with `targetNumberOfCells` throughout domain/API types,
conversions, OpenAPI, Rust/Python/TypeScript clients and product docs. Change the
PostgreSQL `StacGrid` composite in current schema and unfinished migration
`0034_stac_grid` to `target_number_of_cells`. Keep its version/registration;
this change is unreleased. Pre-0034 provider definitions retain NULL/defaults.

## Validation

- Test square/tall/narrow extents, target-count rounding, bounds coverage, exact
  edges/endpoints/points, negative coordinates, invalid inputs and integer limits.
- Test actual UTM geometry, different projections, complete native-cell searches,
  repeated native queries sharing one search, and projection isolation.
- Preserve HTTP/cache/pagination/cancellation/error tests. One-degree geographic
  test fixtures use explicit N=64800; keep precise search-bbox assertions.
- Test configuration/API/database round trips and invalid updates; run STAC and
  migration tests, formatting, services library/test Clippy with warnings denied,
  and generated schema/client consistency checks.
- Root coordinates builds using CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2; avoid
  all-targets/examples and concurrent Cargo compilation.

## Completed verification

GPT-6 Luna implemented the change and root reviewed and finished boundary and
lint cleanup. All 93 STAC tests and both migration checks pass. Formatting,
whitespace checks, SQL lint, and services library/test Clippy with warnings denied
pass. Stored OpenAPI matches application-generated output exactly; all six
affected client models match fresh generator output. TypeScript type checking
and Python model syntax checks pass.
