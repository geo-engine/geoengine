# MdGdalSource: rebase onto ge/main + multi-band parity

## Decisions (settled)

| Question | Decision |
|---|---|
| Rebase target | `ge/main` |
| Data management | Full DB parity with `MultiBandGdalSource` |
| MD read batching | Keep `GdalReadKind::MdArray` + `IpcChannelResult::MdBatch` (gdal 0.19 Rust has no `as_raster_band`, so an MD z-slice cannot go through the 2D path) |
| API reachability | Close the gap: `RasterOperator` variant + `source_operator_from_dataset` arm + `it_deserializes` |
| Per-file times | `time_steps "TimeInterval"[]` **always** populated, plus the `TimeDescriptor` it collapses to. `MdFileTimes::from_columns` rejects a row whose descriptor disagrees with its steps |
| Where MD rows live | New `dataset_md_tiles` table |

## Rebase

2 commits ahead, 8 behind `ge/main`. Merge base `d1e13ab1f`.

| File | Cause | Resolution |
|---|---|---|
| `services/src/contexts/migrations/mod.rs` | both branches claim `0030` | keep ge/main's `0030_stac_provider_band_name`; ours becomes a number above ge/main's latest (see *Rebase onto the real ge/main*) |
| `services/src/datasets/postgres.rs` | same `use geoengine_operators::source::{…}` line | merge both import sets; keep ge/main's `extend_spatial_bounds`/`extend_time_bounds` |
| `openapi.json` | both regenerated | discard, regenerate at the end |

Auto-merges: `operators/src/error.rs`, `api/handlers/datasets.rs`, `api/model/operators.rs`,
`api/model/services.rs`, `contexts/mod.rs`, `current_schema.sql`.

## Migration 0034 (renumbered from 0031; see *Rebase onto the real ge/main*)

`current_schema.sql` is hand-maintained; `migrations_lead_to_ground_truth_schema` compares it
against the migrated schema including `ordinal_position`, so `.sql` and `current_schema.sql`
move in lockstep.

```sql
CREATE TYPE "ZRole" AS ENUM ('Band', 'Variable');
CREATE TYPE "GdalMdMetaData" AS (
    result_descriptor "RasterResultDescriptor",
    z_role "ZRole",
    wrap boolean,
    max_z_batch_size bigint,
    cache_ttl int
);
ALTER TYPE "MetaDataDefinition" ADD ATTRIBUTE gdal_md_meta_data "GdalMdMetaData";

CREATE TABLE dataset_md_tiles (
    id uuid NOT NULL PRIMARY KEY,
    dataset_id uuid NOT NULL,
    time "TimeInterval" NOT NULL,
    bbox "SpatialPartition2D" NOT NULL,
    band oid NOT NULL,
    z_index bigint NOT NULL,
    array_name text NOT NULL,
    array_group text,
    time_descriptor "TimeDescriptor" NOT NULL,
    time_steps "TimeInterval"[] NOT NULL,  -- always populated
    gdal_params "GdalDatasetParameters" NOT NULL
);
CREATE UNIQUE INDEX dataset_md_tiles_unique_idx ON dataset_md_tiles
    (dataset_id, time, bbox, band, z_index, array_name);

CREATE TYPE "MdTileKey" AS (time "TimeInterval", bbox "SpatialPartition2D", band oid,
                            z_index bigint, array_name text);
CREATE TYPE "MdTileEntry" AS (
    id uuid, dataset_id uuid, time "TimeInterval", bbox "SpatialPartition2D",
    band oid, z_index bigint, array_name text, array_group text,
    time_descriptor "TimeDescriptor", time_steps "TimeInterval"[],
    gdal_params "GdalDatasetParameters"
);
```

`z_index` = position in the concatenated z axis within `band` (not multi-band's stacking
precedence — MD files are disjoint, so the ordering is what matters).
`time` = the file's overall bounds, so `time_interval_intersects` still drives row filtering.
`bbox` = the *presented* footprint (wrap-aware), so `extend_spatial_bounds` keeps 0..360 datasets
correct without touching `origin_x`.

`GdalMdMetaData` becomes a real composite instead of `jsonb`, which removes the broken round trip
at `db_types.rs:549` (`serde_json::from_value::<StaticMetaData<…>>` reads the inner struct, not the
tagged form) and the `.ok()` at `db_types.rs:462` that silently writes NULL.

## Per-file times

`TimeDescriptor` is `(bounds, dimension)`; `RegularTimeDimension` is `(origin, step)` with **no
count** — count is implied by bounds. That is why both forms are stored: `steps` says which
slices exist, `descriptor` says it compactly to the API.

- `MdFileTimes { descriptor: TimeDescriptor, steps: Vec<TimeInterval> }`
- `steps` is **always** populated (changed in review round 1); `intervals()` returns it as is
- `is_consistent()` re-derives the descriptor and compares; `from_columns` applies it when
  reading a row back, which is the trust boundary

`MdDatasetFile.time: TimeInterval` is replaced by `times: MdFileTimes`; `z_start`/`z_end` stay.
`probe.rs` keeps each file's own times instead of concatenating and discarding them.

## Postgres provider

- `MdGdalLoadingInfoProvider<Tls>`, same shape as `MultiBandGdalLoadingInfoProvider` (`:838-1165`)
- query `dataset_md_tiles` filtered by bbox/time/band, `ORDER BY band, z_index`,
  same `data_path.resolve_base_path()` join
- five MD helpers mirroring `:852-1046`: `create_md_gap_free_time_steps`, `resolve_md_time_dim`,
  `collect_md_timesteps_in_query`, `resolve_md_first_time_end_before`,
  `resolve_md_first_time_start_after`. `resolve_time_dim`'s SQL is copy-pasted inside
  `validate_time` already — route all three through one function, don't make a third copy.
- `DatasetStore::add_md_dataset_tiles` → `validate_md_time` (skips multi-band's non-overlap rule;
  MD rows span many slices so adjacent files' bounds nest), `batch_insert_md_tiles`,
  `update_md_dataset_extents`
- `AddDatasetMdTile { spatial_partition, band, z_index, array_name, array_group,
  time_descriptor, time_steps, params }` (no `time` - derived from `time_steps.bounds()`
  at row-build time), route `POST /dataset/{dataset}/md-tiles`, handler gated on
  `MdGdalSource::TYPE_NAME`, reusing `validate_tile`'s trust-boundary checks plus a z-count check
- errors in `api/model/responses/datasets/errors.rs` (snafu, not thiserror)

## Operator crate

- keep `GdalReadKind::MdArray` / `IpcChannelResult::MdBatch`; factor the duplicated preamble in
  `gdal_worker_process/reader.rs` (read-id suffix + span, FileNotFound mapping) into two private
  helpers shared by `read_tile_data` and `read_md_batch_data`
- `md_gdal_source/reader.rs`: `GdalPoolReader` wrapper like `multi_band_gdal_source/reader.rs:21-33`;
  replace the 9 positional args (and both `#[allow(clippy::too_many_arguments)]`) with the loading
  info + a small `MdTileRequest { file_idx, local_z, band, tile }`
- `mod.rs::_query`: fold `selected_band_groups` + `select_all` into the same
  `iproduct!(...).buffered(16)` shape as `multi_band_gdal_source/mod.rs:244-263`
- `MdGdalLoadingInfoQueryRectangle { query_rectangle, fetch_tiles }`, copied from
  `multi_band_gdal_source/mod.rs:95-138`, so `_time_query` skips the file query. Changes the `Q`
  parameter of `MetaDataProvider<MdLoadingInfo, …>` in `execution_context.rs` and
  `services/src/contexts/mod.rs`
- `adjust_meta_data_path` MD arm becomes a no-op — paths live in the table and stay relative

## API gap

- `api/model/processing_graphs/source.rs`: `MdGdalSource { params: MdGdalSourceParameters }` with
  `#[api_operator]` + `TryFrom`, mirroring `:71-99`
- `api/model/processing_graphs/mod.rs`: `RasterOperator::MdGdalSource` variant + `TryFrom` arm
- `util/operators.rs:39-44`: `MdGdalSource::TYPE_NAME` arm
- `it_deserializes` asserting `{"type":"MdGdalSource","params":{"data":…}}` round-trips

## Verification

```
just backend build-gdalsource-process
cargo test -p geoengine-operators md_gdal
cargo test -p geoengine-operators --lib gdal_worker
just backend lint-rustfmt && just backend lint-clippy --deny-warnings && just backend lint-sql
cargo run --bin geoengine-cli -- openapi > ../openapi.json
```

`migrations_lead_to_ground_truth_schema` and `lint-sql` need a live Postgres
(`service postgresql start`). Everything else is offline. The 31 existing md unit tests read
committed fixtures from `geoengine/test_data/md/` and must stay green unchanged.

## Where this stands

Branch `feature/md-gdal-source`, rebased onto `ge/main` (0 behind, 2 ahead). All work so far
is **uncommitted** on top of the two rebased commits. Backup: `backup/pre-md-full-parity`.

### Part 1 - rebase + multi-band parity (committed in the rebase)

Done and verified. See the sections above for the design; the summary is the progress log
entry further down.

### Part 2 - CMIP6/NEX-GDDP ingest work (uncommitted)

Triggered by `../BioIS/notebooks/nexgddp_cmip6_ingest.ipynb`, which builds GDAL VRTs
concatenating 65 yearly NEX-GDDP-CMIP6 files x 365 daily bands = 23,743 VRTRasterBand
entries per model/scenario/variable. That is what breaks, not per-file 2D access.

A real file was downloaded and probed (`ACCESS-CM2/historical/1950`, 244 MB) to check the
data shape against this implementation. Copy at `/tmp/opencode/nex.nc`, source URL:

```
https://nex-gddp-cmip6.s3.us-west-2.amazonaws.com/NEX-GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/tasmax/tasmax_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950.nc
```

| Property | Value | Support |
|---|---|---|
| `tasmax` | **3D MD array `(365, 600, 1440)`**, no subdatasets | probe's `(z,y,x)` requirement |
| arrays | `time, tasmax, lat, lon` - 4 arrays with >=3 dims | `array_name` must be passed explicitly |
| `time.units` | `days since 1850-01-01`, values `36500.0, 36501.0` | `ZRole::Time`, uniform 1-day |
| `lon` | `0.125 .. 359.875` step `0.25` -> edges exactly `0..360`, width 1440 even | `wrap = true`, seam split |
| `lat` | `-59.875 .. 89.875`, **ascending** (south-up storage) | `flip_y` on the read advise |
| datatype / nodata | `Float32` / `1.0000000200408773e20` | matches the notebook's `1e20` |
| `lon.units` | `degrees_east` | EPSG:4326 hardcode is right here |
| `srs` | `None` (netCDF driver sets none on MD arrays) | already documented as a `ponytail:` note |
| block size | `[1, 600, 1440]` | one daily slice = one 3.4 MB chunk |
| classic 2D mode | `RasterCount 365`, geotransform `(0,0.25,0,90,0,-0.25)` | per-file works; the VRT concat is what dies |

Remote `/vsicurl` timings (GDAL 3.8.4): MD open **1.7 s**; 4-slice batched read **6.0-6.8 s**;
single slice ~1.4 s cold / **0.0 s warm**; a 512x512 tile costs the same as a full slice
(whole daily slice is one chunk). 65 rows replace 23,743 bands.

Note: `MultiBandGdalSource` cannot rescue this either - a `dataset_tiles` row is one `time` +
one `rasterband_channel`, so day-as-band would still need 23,743 rows. MD is the right tool.

### Part 2 progress

| Step | State | Notes |
|---|---|---|
| 1a probe time descriptor | **done** | `probe.rs` used `TimeDescriptor::new_irregular(first_interval)` - bounds covered only the *first slice* and a uniform daily axis was declared Irregular. Now `MdLoadingInfo::time_descriptor()` -> `time_descriptor_for()`, full-range bounds + `Regular` when uniform. New test `probe_advertises_a_regular_daily_time_axis_over_the_whole_range`. |
| 1b provider time axis | **done** | Was calling `create_gap_free_time_steps`, which assumes one row per time step (true for multi-band, false for MD where a row spans a whole file). For CMIP6 that collapsed 23,743 daily steps to 65 yearly ones, so any date but Jan 1st read the wrong slice - silently, since `MdLoadingInfo::new`'s asserts could not see it. Now `md_loading_info_from_rows` concatenates each row's per-slice intervals, runs them through the existing `try_time_irregular_range_fill`, and derives `MdDatasetFile::z_start` from `time.start()` via `partition_point` - so a missing file yields an empty tile instead of mis-stamped data. |
| - `TileTable` revert | **done** | The `TileTable` enum added earlier to share the time helpers between `dataset_tiles` and `dataset_md_tiles` became dead once 1b stopped using them. Reverted; the four helpers are plain SQL again. `resolve_time_dim` keeps `&impl GenericClient` (a genuine win: `validate_time` now reuses it instead of repeating its SQL). |
| 1c `max_z_batch_size` default 4 | **done** | Moved from operator parameter to dataset metadata (`GdalMdMetaData.max_z_batch_size`). `MdGdalSourceParameters` no longer has it. Default 4: 16 slices at 1440x600 f32 = 55 MB / ~22 s cold; 4 = 14 MB / ~6 s. |
| 1d gap fixtures + regression test | **done** | `generate_md_fixtures.py` gained `time_series_gap_a.nc` (t 0..4) / `time_series_gap_b.nc` (t 8..12), so t 4..8 is a hole. `it_reads_md_tiles_across_a_gap_in_the_file_sequence` creates the dataset, POSTs the rows, queries 13 days and asserts 9 steps: 4 from a, one empty gap, 4 from b - with index 5 = 8000.0 (t=8), **not** a repeat of t=3. Verified it fails with the 1b fix reverted. |
| probe debt marker | **done** | `ponytail:` note on `ProbedMdGdalMetaData` and on `md_array_z_size`: probing is the last GDAL entry point outside the worker pool. Goal is no GDAL calls outside the pool; upgrade path is an IPC `ProbeMdArray` message alongside `GdalReadKind::MdArray` reads, once the pool can return arrays + dimension metadata instead of just raster payloads. Until then it cannot reuse the pool's dataset cache, VSI-cache clearing or retry/backoff. |
| 2a spec attributes | **done** | `add_md_dataset_tiles_handler` had no `#[utoipa::path]`, which was the only reason `/dataset/{dataset}/md-tiles` was missing. Both paths now registered in `apidoc.rs`. (Correction: `AddDatasetTile` was already in the spec - utoipa collects schemas transitively from `request_body`.) |
| 2b probe endpoint | **done** | `POST /dataset/probe-md` -> `MdProbeRequest { data_path, files, array_name, group, variables_as_bands }` -> `MdProbeResponse { meta_data, tiles }`. Delegates to the existing `probe_md_loading_info` / `probe_md_variables_loading_info`; no re-implementation. New `MdProbeError`. No `example` block on the path, because `it_can_run_examples` issues real HTTP for every example. |
| 2b conversion fn | **done** | `probed_md_dataset()` in `handlers/datasets.rs` turns `ProbedMdGdalMetaData` into `(MetaDataDefinition, Vec<AddDatasetMdTile>)` - the inverse of the provider, shared by the endpoint and the 1d test. `spatial_partition` per row is the probe's own presented footprint, so wrap-around rows are correct. |
| 2c regen spec + clients | **done** | `openapi.json` regenerated (317 new lines): paths `/dataset/probe-md` + `/dataset/{dataset}/md-tiles`, schemas `AddDatasetMdTile`, `MdGdalMetaData`, `MdGdalSource`, `MdGdalSourceParameters`, `MdProbeRequest`, `MdProbeResponse`, `ZRole`. `just api-clients lint-openapi-spec` + `just api-clients build` ran clean; all three clients regenerated and committed to the tree. |
| 2d python wrappers | **done** | `probe_md_metadata`, `add_md_dataset_tiles`, `add_md_gdal_source` in `python/geoengine/datasets.py`; all three exported from `python/geoengine/__init__.py`. `ruff check` + `ruff format` clean. |
| 2d notebook rewrite | **done** | `../BioIS/notebooks/nexgddp_cmip6_ingest.ipynb` rewritten: 14 cells, `create_vrt` gone, `data_store="external"`, `array_name=<variable>` (mandatory - the files hold 4 arrays with >=3 dims), rows from the probe, `CPL_VSIL_CURL_ALLOWED_EXTENSIONS` set per row, `Z_BATCH_SIZE = 4` documented as the consumer-side `zBatchSize`. All code cells `ast.parse` clean. |
| full services suite | **done** | **680 passed, 1 failed** - the failure is the pre-existing `netcdfcf::tests::test_irregular_time_series` that also fails on a clean `ge/main`. Was 679 before the new MD test. |

### Bugs the new test found along the way

- `md_array_z_size` did `"".split('/')` for an empty `array_group`, walking to a group named
  `""` and rejecting every root-group MD file with `CannotOpenMdTileFile`. Now filters empty
  segments, matching `probe::into_group`.
- `MdLoadingInfo::new`'s "files of a band must be contiguous along the z axis" `debug_assert`
  is wrong once gaps are allowed. Relaxed to ordered + non-overlapping, with a comment that a
  gap-filling step is a `missing` index that becomes an empty tile.
- The irregular fill does **not** emit a trailing fill when the data ends before the query
  end, so a 13-day query over 8 covered days yields 9 steps, not 10. Worth remembering: the
  gap *between* files is filled, the uncovered *tail* is not.

### Wire format note (verified, not a bug)

`AddDatasetMdTile` deliberately has **no** `#[serde(rename_all = "camelCase")]`, matching
`AddDatasetTile`. So its properties are snake_case on the wire, openapi.json advertises
snake_case, and the generated python client correctly omits aliases for them - `array_name`,
`z_index`, `time_descriptor` go out as-is and the server accepts them. `MdProbeRequest` and
`MdProbeResponse` *do* use camelCase (the response wraps the camelCase API `MdGdalMetaData`),
so `dataPath` / `arrayName` / `variablesAsBands` are aliased in the client.

This looks wrong at a glance - openapi-generator omits `alias=` whenever the snake_case
attribute name already matches the spec property name, and mixing aliased and unaliased
sub-objects is confusing - but both halves were checked against the real generator output and
a hand-built server-shaped payload parses correctly. Do not "fix" it by adding rename_all
without changing the spec and the clients together.

Also verified: `MetaDataDefinition` is a pydantic oneOf wrapper, so a consumer reads the
concrete type from `.actual_instance` (the notebook's debug print does this); generated models
are mutable with `validate_assignment=True`, so setting `tile.params.gdal_config_options`
after the probe works; `MdGdalMetaData.type` is a *required* field, but that only matters when
constructing one - the notebook only ever reads what the endpoint returns.

### Review round 1 - applied

All review decisions are in; nothing from round 1 is outstanding.

| Finding | Resolution |
|---|---|
| Input-side `band` is not an output raster band | `MdDatasetFile::band` -> `output_band`. Genuine output bands (`RasterTile2D::band`, `RasterResultDescriptor.bands`) untouched |
| Separate DB method for all time steps | `MetaData::time_axis(&self, TimeInterval) -> Result<Option<Vec<TimeInterval>>>`, defaulting to `Ok(None)`; `MdGdalLoadingInfoProvider` implements it with a bbox-free, band-free query on `time_steps` only. `_time_query` gets a `QueryContext`, not an `ExecutionContext`, so the seam had to be `MetaData` |
| `time_steps` only for irregular axes | Always populated. `MdFileTimes::is_consistent()` plus `from_columns` as the trust boundary |
| z batch size as an operator parameter | Moved to dataset metadata as `max_z_batch_size` (`bigint` column, `i64` in the DB struct, `usize` in-process). `MdGdalSourceParameters` no longer has it; `MdProbeRequest` accepts it |
| `ZRole::Variable` comment wrong | Rewritten: each selected variable becomes an output band, reads every file, rows are per `(file, variable)` on a shared timeline |
| netCDF-CF not reusable | Confirmed: `datasets/external/netcdfcf` is EBV-specific (hardcoded `ebv_cube`, 4D index layout, hardcoded date, never reads lon/lat) |
| `MdGdalPoolReader` newtype | Deleted; a free function is enough |
| magic `16` for concurrency | `TILE_READ_CONCURRENCY` in `gdal_worker_process/mod.rs`, shared by the MD and multi-band sources. `GdalSource` still has its own `try_buffered(8)` |
| `optimize()` missing | Inserts `Downsampling`/`NearestNeighbor` when the target resolution is compatible; GDAL MD arrays have no native overviews |
| `md_gdal_meta_data` / `MdGdalMetaData` | Renamed to `gdal_md_meta_data` / `GdalMdMetaData` |
| probe location | Kept in the operators crate; pooling it is deferred debt, see the `ponytail:` notes |

One thing the review surfaced that was not in the finding list: dropping
`MdGdalLoadingInfoQueryRectangle` means the MD loading info query now holds a *grid* bbox while
`dataset_md_tiles.bbox` is stored in *spatial* coordinates. The conversion moved into
`MdGdalLoadingInfoProvider::loading_info` via the dataset's own geo transform.

### Code review round - applied

A review of the resulting diff turned up three bugs and two doc mismatches. All are fixed
and covered.

| Finding | Severity | Fix |
|---|---|---|
| `ZRole::Variable` duplicated the time axis: `md_loading_info_from_rows` concatenated *every* band's intervals, and `time_axis` had no band filter at all, so an N-variable dataset got N copies of the timeline and a non-monotonic `time_steps` | high | build the axis from one band's rows only, in both places; `it_builds_one_md_time_axis_per_variable_not_one_per_band` locks it down |
| `ZRole::Band` panicked on an out-of-range band selection: `empty_tile` indexed `time_steps[global_z]` with no bounds check and `output_band_groups` only validated for `Variable` | medium/high | validate the band count for `Band` too, and index `time_steps` with `get`; `out_of_range_band_is_rejected_for_band_role` |
| `AddDatasetMdTile.time_steps` docs said "only set for an irregular axis" while the column is `NOT NULL` and the read path derives the file's slice count from it. External paths skipped validation and died on a raw DB error | medium | required field, validated before the `DataPath::External` early-return with `MdTileMissingTimeSteps`, `MdTileEntry.time_steps` no longer `Option` |
| `time_axis` checked descriptor/steps agreement via `from_columns`, `md_loading_info_from_rows` did not | minor | both use `from_columns` |
| `validate_md_tiles` claimed hole detection it does not do | minor | doc corrected; the relaxed `debug_assert` now only requires the row ordering the code actually depends on |

One behavioural change came out of the second finding: gap tiles and data tiles in a
`ZRole::Band` query carried different timestamps (`empty_tile` used `time_steps[global_z]`,
the reader used `file.times.first_interval()`). Band-role intervals are synthetic
`[k, k+1)` unit steps, so the reader now stamps `time_steps[global_z]` as well.

### Code review round 2 - applied

A second review of the diff found four bugs; all fixed and covered.

| Finding | Severity | Fix |
|---|---|---|
| `ZRole::Band` rows all store `band = 0` (the z index *is* the band) and a synthetic `[k, k+1)` time, but `loading_info` filtered `band = ANY($4)` and `time_interval_intersects(time, $3)`. Any band selection not containing 0 matched zero rows and came back as empty tiles instead of data | high | skip both predicates when `z_role == ZRole::Band`; `it_reads_md_band_role_rows_for_any_requested_band` |
| `time_axis` ignored the query window: `try_time_irregular_range_fill` *extends* coverage to the query but never clips, so a one-day window on a 65-year daily series returned all ~23k slices | medium | filter the result by `intersects(query)` in the provider method (not in `md_time_axis`, whose output feeds `loading_info`'s z realignment); covered by a one-day-window assertion |
| `/dataset/probe-md` passed `root.join(file)` straight to GDAL. `Path::join` lets an absolute path or `../` escape the root, so any authenticated user could make the server open arbitrary local files and report their metadata | high | `validate_file_path` per file before GDAL sees it; `it_rejects_probe_md_paths_that_escape_the_data_path` |
| the time axis concatenated rows in `ORDER BY band, z_index`, so a client-supplied `z_index` disagreed with the rows' time order the axis came out unsorted (only a `debug_assert` caught it) | medium | `ORDER BY band, (time).start, z_index` in both row queries; `z_index` is now only a tiebreak |

The `validate_file_path` External arm also gained `/vsi`-prefixed paths alongside bare
URLs. `file_path_for_open()` wraps bare URLs at open time and passes already-prefixed paths
through, so both forms are remote reads; a plain path is not external data and is still
rejected. `ponytail:` note names the ceiling (`/vsizip/` can reach inside a local archive)
and the upgrade path (remote handlers only).

One behavioural change from this round: `AddDatasetMdTile.time` is now derived from the
declared `time_steps` bounds in the handler, so a client-supplied `time` can no longer hide
a row from queries it should answer.

### Code review round 3 - integration, accuracy, contracts

A pass over how the feature fits the existing source/operator/DB stack, looking for
behaviour that differs from what reviewers expect and for GIS accuracy.

**Decisions taken:**

| Question | Decision |
|---|---|
| `AddDatasetMdTile.time` accepted a client-supplied value and was silently overwritten | **removed from the request type.** The file's bounds are derived from `time_steps` where the row is built (`AddDatasetMdTile::file_time_bounds()`), so a request cannot describe a row as covering a window it does not. `AddDatasetTile` (the non-MD one) keeps its `time`. |
| Which paths count as external data | **bare URLs and `/vsi*` both stay accepted.** `file_path_for_open()` wraps bare URLs at open time and passes already-prefixed paths through, so both are remote reads. |

**Bug found and fixed while tracing the derivation:**

`validate_md_tile` checked `MdFileTimes::is_consistent()` *after* the
`if matches!(data_path, DataPath::External) { return Ok(()); }` early return. External rows
therefore skipped the one check that needs no file at all and were stored with a descriptor
that disagreed with their steps - a row the read path uses via `steps` and the API
advertises via `descriptor`. The axis checks now run before anything path-specific.

That gap mattered more once `time` became derived: `MdFileTimes::bounds()` takes the extent
from the first and last step, so an axis that is not in slice order derives an inverted file
extent. Added `MdFileTimes::is_ordered()` (non-decreasing starts; gaps fine, disorder not),
enforced in `from_columns` alongside `is_consistent`, and checked in `validate_md_tile`.
Covered by `it_rejects_a_bad_axis_on_external_rows`, which drives both cases through the
external path that used to short-circuit.

**Reviewed and left as is** (documented, no change): `MetaData::time_axis` is query-scoped
and clips to the window; `ZRole::Band` skips the band/time row predicates because those
columns are synthetic there; MD `optimize` inserts `Downsampling` because GDAL MD arrays
have no native overviews; probe stays in the operators crate outside the pool.

### Remaining / possible follow-ups

1. **End-to-end run against the real file.** The probe endpoint has no integration test over
   `/vsicurl`; it was validated by hand in python against GDAL 3.8.4 (open 1.7 s, 4-slice batch
   6.0-6.8 s). Running the rewritten notebook against a live GeoEngine with `DEV_ONLY = True`
   would close this.
2. **Move probing into the gdal worker pool** (deferred debt, see the `ponytail:` notes). The
   goal of *no GDAL calls outside the pool* also applies to the pre-existing
   `suggest_meta_data_handler` and `auto_create_dataset_handler`.
3. An MD CLI importer - add when someone needs a bulk import outside the API.
4. `GdalSource` still carries its own `try_buffered(8)` instead of the shared
   `TILE_READ_CONCURRENCY`. Unify when someone touches that file for another reason.

### Verification state

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --all-features --all-targets -- -D warnings` | clean |
| `pipx run sqlfluff==4.2.2 lint` | clean (unchanged since last run) |
| `cargo test -p geoengine-operators` | **631 passed, 0 failed** |
| `cargo test -p geoengine-services --lib` (apidoc + new MD test + migrations) | **16 passed, 0 failed** |
| `cargo test -p geoengine-services` (whole suite) | **685 passed, 1 failed** (pre-existing) |
| `ruff check` + `ruff format --check` (python) | clean before review round 1; ruff is not installed in the current shell, so re-run after installing it |
| python client round-trip vs. real server JSON | verified both directions, see below |
| `cargo run --bin geoengine-cli -- openapi` | regenerated (`AddDatasetMdTile` no longer has `time`) |
| `just api-clients lint-openapi-spec` | 1 pre-existing `nullable` deprecation notice |
| `just api-clients build` | clean, all three clients |

Known pre-existing failure on `ge/main` (verified on a clean checkout, unrelated):
`datasets::external::netcdfcf::tests::test_irregular_time_series`.

### Conventions this work established

- `MdGdalSource` read tests need `cargo build --bin gdalsource-process` first, else the pool
  reports `WorkerPanic`. CI already does this.
- The migration/schema tests need a live Postgres. A suitable one was reachable at
  `localhost:5432` (db/user/pass all `geoengine`) via the `postgis_cleaner` container.
- `it_can_run_examples` issues real HTTP for every `request_body` example in the spec, so
  never add an example that points at a nonexistent path or volume.
- Service tests must build `CreateDataset` inline; `util::tests::add_file_definition_to_datasets`
  has a `todo!()` for meta data types other than the ones it knows and panics on `GdalMdMetaData`.
- `z_index` (row ordering key within a band, a running file count) and `MdDatasetFile::z_start`
  (position on the time axis) are *different numbers*. Do not merge them.

## Out of scope

An MD CLI importer, and probing inside the gdal worker
pool - the last one is recorded debt rather than a rejection, see the `ponytail:` notes on
`ProbedMdGdalMetaData` and `md_array_z_size`.

## Review round 3 - applied

| Finding | Fix |
|---|---|
| `loading_info` built the full dataset time axis for every query | `md_loading_info_from_rows` clips `time_steps` to `intersects(query_time)` and stores `local_offset` on `MdDatasetFile` so `local_z = (gz - z_start) + local_offset` stays correct |
| Tile stream was band-major | Requests are now merged by `first_z` across bands so the stream is time-major (time, space, band) |
| CF time units only accepted plural | `z_role_from_units` now accepts singular forms (`"second"`, `"minute"`, `"hour"`, `"day"`) alongside plurals. `"years since"` / `"months since"` remain unsupported (variable-length, CF calendar semantics) |
| Migration 0031 renamed in-place | Never deployed; no follow-up needed |

## ZRole::Time removal

`ZRole::Time` was a degenerate case of `ZRole::Variable` with one variable (band 0).
The probe already enforced this (`probe_md_variables_loading_info` rejects arrays without
CF time units, and `z_role_and_intervals` returns Time for CF units). Collapsed into
`Variable`: single-variable datasets are now `ZRole::Variable` with `output_band = 0`.
`Band` remains distinct (z→band, synthetic unit intervals, no time axis).
`ZRole` enum in both crates + API model + migration SQL + `current_schema.sql` updated;
OpenAPI and all three API clients regenerated.

## Rebase onto the real ge/main

The first rebase ran against a stale `ge/main`. After `git fetch`, upstream had moved 6
commits and claimed migration numbers **0031-0033** (`stac_provider_authentication`,
`stac_provider_cache_ttl`, `gdal_multiband_cache_ttl`), so ours was renumbered.

| Item | Before | After |
|---|---|---|
| migration | `0031_md_dataset_tiles` | `0034_md_dataset_tiles` |
| `prev_version()` | `Migration0030StacProviderBandName` | `Migration0033GdalMultibandCacheTtl` |
| `all_migrations()` order | after 0030 | after `0033_gdal_multiband_cache_ttl` |

The three upstream migrations touch disjoint objects (`StacProviderAuthentication`, STAC
cache TTL, `GdalMultiBand`), so renumbering was mechanical. The `.sql` body needed no
change. `current_schema.sql` was re-based on upstream's version with our two additions
re-applied on top, so upstream's `GdalMultiBand.cache_ttl`, `StacProviderAuthentication` and
the `StacProviderS3Config` encryption columns all survive.

Restore point: `backup/pre-rebase-real-ge-main` (before the rebase, 3 commits).

### Dataset-level cache_ttl for MD

Upstream's 0033 added `GdalMultiBand.cache_ttl`, so MD got the same. `GdalMdMetaData`
gained `cache_ttl int`, `MdLoadingInfo` carries `Option<CacheTtlSeconds>` instead of a
frozen `CacheHint`, and `MdGdalSourceProcessor` holds `default_cache_ttl` from
`ExecutionContext::default_cache_ttl()` — the exact chain multi-band uses, including
`cache_hint(default)` resolving dataset TTL then context default. `MdProbeRequest` accepts
it so the notebook/CLI can set it at registration.

## Review round 4 - "can this replace the old netCDF handling?"

**No, and the premise needs correcting.** `datasets/external/netcdfcf` (7756 lines) is an
EBV-portal adapter, not a general netCDF reader. It hardcodes the EBV convention throughout:

```rust
let gdal_path = format!("NETCDF:{path}:/{group_path}/ebv_cube");  // array name fixed
const LON_DIMENSION_INDEX: usize = 3;                              // (entity,time,lat,lon)
const LAT_DIMENSION_INDEX: usize = 2;
const TIME_DIMENSION_INDEX: usize = 1;
let unix_offset_millis = TimeInstance::from(DateTime::new_utc(1860, 1, 1, ...));
```

A 3D `(time, lat, lon)` array is **not supported** there: dimension 3 does not exist, so
`width = unwrap_or_default() = 0` and it silently degrades. `MustBe4DDataset` is declared
but never constructed anywhere. There is no `depth` string in the module.

Where the two genuinely overlap is the data axis, and the new code is strictly better:

```rust
// old: flatten (entity, time) into a 2D band index on a NETCDF: subdataset string
channel: dataset_id.entity * dimensions_time + i + 1
```

That is the 2D raster API, and it *assumes* how GDAL's netCDF classic view flattens leading
dims. Nothing validates `band_count() == entities x time`, so a wrong assumption surfaces as
a silently wrong band. It is exactly the assumption that breaks on NEX-GDDP-CMIP6.
`MdGdalSource` reads a per-dimension index window instead, so the mapping is read, not assumed.

Blockers to full replacement: no overviews (old has per-slice COG via `multi_dim_translate`
plus `OverviewLevel` and four HTTP task endpoints; `mod.rs:560` records that GDAL cannot
build overviews for MD arrays), no 4D/entity, no dataset discovery, no metadata harvesting
(exactly `long_name` + `standard_name` are read), no `calendar` support. The reverse reuse is
the interesting one: netcdfcf's COG overview pipeline could give `MdGdalSource` its
overviews back.

Gaps `MdGdalSource` has itself, to be closed by the plan below: CRS forced to EPSG:4326 and
never read, a non-time z axis silently degrading into bands, `group` ignored when
`variables_as_bands` is false, no one-call creation, and the deepest layout rejections
untested.

## Plan A - closing the review gaps

Ordered by "will bite next". Items A1 and A2 are the same failure class as the date-only CF
origin bug: silently wrong instead of erroring.

### A1. A non-time z axis must error, not become bands

`z_role_from_units` returning `None` currently means `ZRole::Band` with synthetic `[k,k+1)`
**millisecond** intervals. That is how a daily field became 365 bands named "band 0".."band
364". `years since` / `months since` still return `None` by design, so the trap is armed.

- Return a decision (`Time` / `Band` / `NotTime(units)`) instead of `Option`.
- Probe: `NotTime` is a **probe error** naming the units and the two ways forward.
- `MdProbeRequest.force_band_role: bool` is the escape hatch, so genuinely band-like data can
  still be registered deliberately rather than by accident.
- Gate: a fixture with `years since 2000-01-01` must error, not produce bands.

### A2. Report the real CRS instead of assuming EPSG:4326

`is_geographic` carries a `ponytail:` note that `spatial_reference()` panics in gdal 0.19 for
netCDF MD arrays, so the descriptor hardcodes `epsg_4326()`. A projected MD array is
therefore silently mislabelled.

- Read the CRS the safe way: `crs` / `spatial_ref` / `grid_mapping` attributes via the
  existing `string_attr`, preferring a non-panicking path.
- Use the real CRS when found. When genuinely unavailable, keep 4326 but say so: the probe
  response gains `crs_source: "attribute" | "assumed_epsg4326"`.
- Gate: a fixture with a `crs` attribute reports that CRS.

### A3. Honour `group` when `variables_as_bands` is false

`group` is passed only on the variables branch (`datasets.rs:742-757`);
`probe_md_loading_info` hardcodes the root group and `group: None`. A client setting `group`
alone gets a confusing "cannot open MD array" or silently reads a same-named root array.

- Pass `group` on both branches; error with the searched path if it resolves to nothing.

### A4. Cover the untested rejections

The `<3 dims` and non-singleton-leading-dims rejections have **no test**, so the deepest
layout restrictions are unenforced by CI. Add fixtures for a 2D array (and, once B lands, a
4D array), plus `auto_array_name` with 0 and >1 candidates, descending-x rejection, and a
non-numeric datatype.

### A5. One-call server-side creation - deferred

probe -> `POST /dataset` -> md-tiles is three round trips; only the Python helper bundles
them. Convenience, not correctness, and strictly less valuable than A1-A4.

---

## Split: what this branch carries, and what the probe branch will

The probe - the part that *derives* metadata by opening the files - is deliberately **not**
in this branch. Deriving is a convenience; stating is the contract. What ships here is the
half that has to be right either way: the reader, the storage layout, the trust boundary, and
a runnable example that declares every field by hand
(`python/examples/md_gdal_source_dataset.ipynb`).

The probe code is not lost: it lives in this branch's history (`cc9aa2e87`, `c93560ac9`) and
can be restored onto a stacked branch with `git show`, so the reviewers can decide separately
whether a raster importer needs an HTTP probe.

### Stays here, because production uses it

| item | why |
|---|---|
| `presented_geo_transform` | the tile trust boundary presents a file's stored transform and compares it with the dataset's; not a probe concern, and it was moved to `loading_info.rs` so that deleting the probe cannot take it |
| `validate_md_tile` + `check_md_tile_against_array` | the only thing that can catch a wrong `arrayName`, `leadingPrefix`, grid, slice count or CRS - now on every data path, see below |
| `AddDatasetMdTilesError` | the failure modes of the above |
| `dataset_md_tiles` incl. `leading_prefix` | per-row leading prefix, so one `(time, depth, y, x)` file becomes one dataset with depth bands |
| the read path | unchanged in behaviour; 3D takes an empty prefix and the same window as before |

### Moves to the probe branch

`probe.rs` in full, the `MdProbeRequest`/`MdProbeResponse` endpoint and `probed_md_dataset`
splitter, `MdArraySelection`, the Python `probe_md_metadata` + `add_md_gdal_source` wrapper,
the generated client models, and the 22 probe tests.

### Consequences accepted

- The probe's *guard rails* travel with it: the non-time-z-axis error, real CRS resolution,
  group auto-detection, and `force_band_role`. A hand-declared `zRole` is trusted, so the
  example's absolute-time read-back is the check that a wrong axis does not pass unnoticed.
- **A dataset-level leading prefix would have been wrong.** `(time, depth, y, x)` needs one
  prefix per *band* to become depth bands; a single dataset-level prefix can only express "one
  dataset = one depth", i.e. one dataset per depth plus a `RasterStacker`.
- External registration used to trust the caller completely. `validate_md_tile` returned
  before opening the array for `DataPath::External`, so for a `/vsicurl` dataset nothing
  verified the array name, grid, slice count, prefix length or CRS - and a wrong CRS is
  silent forever. It now checks **one file per distinct `(array_name, array_group)`** for
  external data (one `/vsicurl` round trip per array instead of one per file) and every file
  for local data. A per-file divergence still fails loudly at read time, because the read
  window then exceeds the array.

### Registering by hand

`AddDatasetMdTile` per file (all of a file's z slices), `GdalMdMetaData` for the dataset.
Two traps the example shows: a tile's `geoTransform` is the grid **as stored** while the
descriptor's `spatialGrid` is the grid **as presented** (north-up, `-180..180` when `wrap`),
and `timeSteps` is one interval **per z slice**, not one for the file.
