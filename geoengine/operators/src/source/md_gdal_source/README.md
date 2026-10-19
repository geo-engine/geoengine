# `MdGdalSource` — scope and `MdAxisMapping`

A raster source for **multidimensional GDAL arrays** (NetCDF / Zarr / HDF5): climate-model and
satellite products whose data is an n-dimensional array, usually `(time, y, x)` or
`(time, <other>, y, x)`, in **one file with a large number of time steps**.

## Why not the classic `GdalSource`

The classic source addresses time as **bands**: one `GdalLoadingInfoTemporalSlice` per time step,
each naming a `rasterbandChannel`. Climate model data breaks that model:

- **One file holds many steps.** A CORDEX EUR-11 daily file holds **1800** steps (5 years); CMIP6
  daily files hold 10⁴–10⁵. The classic path needs **one loading-info slice per step**, so the
  loading info grows to hundreds of thousands of entries per dataset, stored and validated one by
  one. That does not scale.
- **More than one non-spatial dimension cannot be mapped.** A classic raster folds only one
  leading dimension into band numbers. An array like `(time, level, y, x)` (ERA5 pressure levels)
  or `(time, band, y, x)` (multi-variable products, MTG channels) cannot be addressed by time at
  all, because time is no longer the only band axis.

So the time axis has to be **read dimension-wise** instead of through band numbers.

## Scope of this branch

In scope:

- The `MdGdalSource` raster operator, with dataset metadata `GdalMdMetaData` and one
  `AddDatasetMdTile` row per file.
- An explicit, per-variable **`MdAxisMapping`**: one entry per source dimension, saying which
  dimension is x, which is y, which is time, and which is pinned.
- Explicit **per-file time steps** (`time_steps` plus a compact `time_descriptor`), so one file
  contributes its whole time span as a single row.
- Reads through the existing in-pool `gdalsource-process` worker
  (`GdalReadKind::MdArray`), so GDAL stays out of the server process.
- The raster contract of the engine: a gap-free time axis (missing steps become empty tiles),
  north-up presentation (a south-up, ascending-`lat` array is flipped), and 0..360 → −180..180
  longitude wrapping.
- Validation at the registration trust boundary (see below).

Out of scope for now:

- **No probe.** The metadata is stated by the author — constants and knowledge of the data's
  layout, written in an example or a script. Nothing derives it from a file at registration time.
- No rotated-pole CRS. A CORDEX EUR-11 file in its native rotated grid
  (`rlat`/`rlon`, `standard_name=grid_latitude`) has no affine EPSG:4326 transform, so it must be
  reprojected to a rectilinear grid first (the `cordex_4326_zarr` store is such a version). The
  raw `.nc` is openable by GDAL but not mappable to a raster.
- No vector/Arrow output; MD is raster only.
- One time axis per dataset; all variables and files are expected to share it.

## How `MdAxisMapping` works

Each variable carries **one entry per source dimension, in array order**. The role of a dimension
is one of four:

| Role            | JSON                         | Meaning                                                       |
| --------------- | ---------------------------- | ------------------------------------------------------------- |
| `X`             | `{"role":"x"}`               | the horizontal x dimension                                    |
| `Y`             | `{"role":"y"}`               | the horizontal y dimension                                    |
| `Time`          | `{"role":"time"}`            | the z axis: a read takes the slice at the requested time step |
| `Fixed {index}` | `{"role":"fixed","index":0}` | a dimension that is **not** sliced; every read takes `index`  |

Rules:

- `validate_md_axis_roles`: **exactly one `X`, exactly one `Y`, at most one `Time`** — no sizes
  needed, so it also runs where the array is not opened (external rows);
- `validate_md_axis_mapping`: the mapping has exactly as many entries as the array has dimensions,
  and every `Fixed` index must be within that dimension's size.

`Time` and `Fixed` split the "not spatial" dimensions between _sliced_ (time) and _pinned_
(band, level, ensemble member, …). Examples:

| Array                 | Mapping                                               | Result                                             |
| --------------------- | ----------------------------------------------------- | -------------------------------------------------- |
| `(time, y, x)`        | `[Time, Y, X]`                                        | one output band, one tile per time step            |
| `(band, y, x)`        | `[Fixed{0}, Y, X]`, `[Fixed{1}, Y, X]`, …             | one output band per fixed index (one per variable) |
| `(time, level, y, x)` | `[Time, Fixed{level}, Y, X]`                          | one output band per level                          |
| `(time, band, y, x)`  | `[Time, Fixed{0}, Y, X]`, `[Time, Fixed{1}, Y, X]`, … | one output band per band of the array              |

A dimension with role `Time` is the z axis a read slices: the read sets GDAL's `start` for that
dimension to the file-local slice index and `count` to 1. A `Fixed` dimension sets `start` to its
index and `count` to 1. X and Y set `start`/`count` from the read window. `md_read_window` builds
this per-dimension `start`/`count` pair; nothing is inferred from the dimension's position.

## Time steps from a CF calendar

The server does not read the file's `time` coordinate: `time_steps` is stated by hand, one
`TimeInterval` per slice. Convert the coordinate with the file's own calendar and epoch, not with a
fixed one — CORDEX EUR-11 uses `360_day`, CMIP6 `standard`/`gregorian`, and both count from
different epochs. `xarray` + `cftime` does this correctly:

```python
import cftime, numpy as np, xarray as xr

ds = xr.open_dataset(path, decode_times=False)
time = ds["time"]
dates = cftime.num2date(
    time.values,
    time.attrs["units"],                      # "days since 1949-12-01 00:00:00"
    time.attrs.get("calendar", "standard"),   # "360_day", "standard", ...
)

# one interval per slice; the end is the next step (or +1 day for the last one)
starts = np.array([d.isoformat() for d in dates], dtype="datetime64[ns]")
ends = starts[1:] + [starts[-1] + np.timedelta64(1, "D")]
```

The engine's time axis is UTC nanoseconds; `datetime64[...]` converts directly. A `fixed` time step
(a monthly mean, a decade) is stated the same way — nothing is inferred from the coordinate.

## Dataset shape

`GdalMdMetaData` is the dataset-level metadata:

- `result_descriptor` — data type, spatial reference, time descriptor, band descriptors. Its
  spatial grid is the **raw** grid (the file's own transform, south-up and 0..360° included); a
  wrapped dataset is shown shifted only when the descriptor is read back (see `MdPresentation`).
- `variables: Vec<MdVariable>` — `{ name, axis_mapping }`. **The index of a variable is its Geo
  Engine output band.**
- `wrap` — whether stored 0..360° longitudes are presented as −180..180 (see `MdPresentation`).
- `cache_ttl` — dataset-level fallback.

One `AddDatasetMdTile` **per file**:

- `spatial_partition` — the file's **raw** footprint (the stored grid, no wrap shift).
- `band` — which variable this file belongs to.
- `z_index` — the file's position in that band's concatenated z axis. Files of a band must be
  ordered and non-overlapping; a gap **between** files is fine (the dataset axis fills it with an
  empty step), a gap **inside** one file is rejected.
- `array_name`, `array_group` — the GDAL MD array to open (`/pr`, `/tasmin`, …) and the group
  below the root (`null` = root).
- `time_descriptor` + `time_steps` — the file's time span, both always present. `time_steps` is
  one interval per slice and is what the read path slices by; `time_descriptor` is the compact
  form the API advertises and must be consistent with `time_steps`.
- `params` — the usual `GdalDatasetParameters` (path, geo transform, width, height,
  `no_data_value`, open/config options, …).

Stored in `dataset_md_tiles`, keyed by
`(dataset_id, time, bbox, band, z_index, array_name)`. Registered through
`POST /dataset` (`sourceOperator: "MdGdalSource"`, `metaData.type: "GdalMdMetaData"`) followed by
`POST /dataset/{name}/md-tiles` with the array of rows.

## Validation (registration is the trust boundary)

A row is rejected unless:

- the axis mapping has one `X`, one `Y`, ≤ one `Time`, and every `Fixed` index is in range;
- the file opens, the named array exists, and its **`Time` dimension size equals the number of
  declared `time_steps`** (or the mapping has no `Time` and the variable is a single slice);
- its **X/Y sizes equal `params.width`/`params.height`**;
- `time_steps` are non-empty, ordered, consistent with `time_descriptor`, and **contiguous within
  the file** (`MdTileTimeAxisHoles`);
- the file's transform is compatible with the dataset's raw grid after the south-up flip only — no
  longitude shift (`MdTileGeoTransformMismatch`), so the stored footprint is the file's own; and a
  CRS the file declares is not a _different_ one than the dataset's (`MdTileCrsMismatch`) — a file
  that declares no CRS is allowed.
- a wrapped dataset needs an **even** width, a span of exactly 360°, and an origin on the pixel
  lattice of −180° (`MdTileUnpresentable`); otherwise the shift has no whole-pixel answer.

## Reading

A query (spatial bounds, time interval, band selection) becomes, per selected output band, the
global z slices covering the interval. Files are located by their z range, and for each slice the
worker opens the file's array and reads one slice with the `start`/`count` built from the axis
mapping. The result keeps the engine's fixed raster order: **time, then tile, then band**. A slice
no file covers is emitted as an empty tile, so the axis stays gap-free.

## Worked example: CORDEX EUR-11 daily, reprojected to 4326 Zarr

Measured layout of `4326_pr_..._day_20060101-20101230.zarr` (Zarr v3, produced by
`cdo -remapbil,r3600x1800`):

- array `pr`: `(time=1800, lat=506, lon=1095)`, `float32`,
  fill value `1.0000000200408773e+20`;
- `lat` 22.05 → 72.55, uniform 0.1 (ascending → south-up); `lon` −44.5 → 64.9, uniform 0.1;
- `time`: `days since 1949-12-01 00:00:00`, `calendar=360_day`, 1800 uniform daily steps;
- GDAL's classic view exposes 1800 bands — which is exactly why the classic path would need 1800
  loading-info slices, while the MD path registers **one row per file**.

Mapping: `axis_mapping = [Time, Y, X]`. Grid: pixel 0.1°, stored origin `(−44.55, 22.0)`
(south-up corner), presented north edge `22.0 + 506 · 0.1 = 72.6`, EPSG:4326, `F32`,
`no_data_value = 1e20`.

## Fixtures

`geoengine/test_data/md/` holds small generated fixtures (`generate_md_fixtures.py`): time series,
split and holed files, a band-dimension array, a 0..360 wrap case, a fixed-axis variable. Real
products used for manual validation live outside the repo, e.g.
`/mnt/peter/geo_data/esgf/cordex_4326_zarr`, `esgf/cordex/rcp26/*.nc`, `nexgddp-cmip6`,
`ecmwf_era5_land`.
