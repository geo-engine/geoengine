#!/usr/bin/env python3
"""Generates the MD GDAL test fixtures under `test_data/md/`.

Requires: python3 with the `osgeo` GDAL Python bindings and numpy.
Idempotent: overwrites existing fixtures.

Fixtures
--------
- time_series.nc          ZRole::Time, single file, edges lon 0..240 (step 30), lat 0..-8, 8 time steps
- time_series.zarr        same array via the ZARR driver
- time_series_split_a.nc  first half of a two-file time series (global t 0..4)
- time_series_split_b.nc  second half (global t 4..8), grid identical to part a
- time_series_gap_a.nc    first 4 slices (t 0..4) of a series with a hole
- time_series_gap_b.nc    last 4 slices (t 8..12) of a series with a hole; t 4..8 is absent
- wrap_0_360.nc           ZRole::Time, edges lon 0..360 (step 1), lat 45..35 (wrap-around)
- wrap_0_360_multitile.nc same but 0.25 deg lon (1440 cols, spans 3 tile columns)
- bands.nc                ZRole::Band, dims (band, y, x), no CF time units
- cf_time_units_minutes.nc ZRole::Time, CF units `minutes since 1900-01-01`, ASCENDING lat
- variables.nc            ZRole::Variable: 3 CF data variables (temperature/precipitation/
                          cloud_area_fraction) sharing (time, lat, lon), with long_name/units
- grouped_variables.nc    same arrays as variables.nc but inside the `analysis` subgroup
- projected_crs.nc        (time, y, x) in a projected CRS, declaring it via the CF `crs`
                          attribute (metre x units, so EPSG:4326 would be a mislabel)

It also writes one copy of `time_series.nc` to `python/examples/data/md_example.nc`, which is
the fixture `python/examples/md_gdal_source_dataset.ipynb` registers. Same bytes, so the
example's documented values and the tests' expected values cannot drift apart.

netCDF/CF convention: coordinate variables are cell *centers* (offset half a
pixel from the grid edges), so the probes can derive a correct geo transform.
Cell values follow deterministic formulas so tests can assert exact values:
- (time, y, x) arrays:  value = t*1000 + y*10 + x     (x in stored column index)
- (band, y, x) arrays:   value = b*100  + y*10 + x
- wrap_0_360:            value = t*100  + y*10 + x     (x in stored column index)
- variables (.nc):       value = (var_index + 1) * (t*100 + y*10 + x)
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import numpy as np
from osgeo import gdal

gdal.UseExceptions()

OUT_DIR = Path(__file__).resolve().parent

F64 = gdal.ExtendedDataType.Create(gdal.GDT_Float64)
F32 = gdal.ExtendedDataType.Create(gdal.GDT_Float32)

FIXTURES = [
    "time_series.nc",
    "time_series.zarr",
    "time_series_split_a.nc",
    "time_series_split_b.nc",
    "time_series_gap_a.nc",
    "time_series_gap_b.nc",
    "wrap_0_360.nc",
    "wrap_0_360_multitile.nc",
    "bands.nc",
    "cf_time_units_minutes.nc",
    "cf_time_units_date_only.nc",
    "cf_time_units_years.nc",
    "too_few_dims.nc",
    "projected_crs.nc",
    "projected_no_crs.nc",
    "time_depth_4d.nc",
    "variables.nc",
    "grouped_variables.nc",
]


def mk_grid(
    width: int,
    height: int,
    lon_left: float,
    lat_edge: float,
    lon_step: float = 1.0,
    lat_step: float = 1.0,
    lat_ascending: bool = False,
) -> tuple[np.ndarray, np.ndarray]:
    """Grid edges at lon_left .. lon_left+width*lon_step; for descending lat,
    lat_edge is the TOP edge, for ascending lat it is the BOTTOM edge. Returned
    coordinate values are cell centers (CF convention)."""
    x = lon_left + (np.arange(width, dtype=np.float64) + 0.5) * lon_step
    idx = np.arange(height, dtype=np.float64) + 0.5
    y = lat_edge + idx * lat_step if lat_ascending else lat_edge - idx * lat_step
    return x, y


def add_coord_var(rg, name: str, dim_type: str, direction: str, values: np.ndarray):
    """Creates a dimension plus its same-named coordinate variable (netCDF/CF convention)."""
    dim = rg.CreateDimension(name, dim_type, direction, values.size)
    arr = rg.CreateMDArray(name, [dim], F64, [])
    arr.Write(values)
    return dim, arr


def add_xy_dims(
    rg,
    width: int,
    height: int,
    lon_left: float,
    lat_edge: float,
    lon_step: float = 1.0,
    lat_step: float = 1.0,
    lat_ascending: bool = False,
):
    x, y = mk_grid(width, height, lon_left, lat_edge, lon_step, lat_step, lat_ascending)
    xdim, xa = add_coord_var(rg, "lon", "HORIZONTAL_X", "", x)
    xa.CreateAttribute("units", [1], gdal.ExtendedDataType.CreateString(), []).WriteString(
        "degrees_east"
    )
    ydim, ya = add_coord_var(rg, "lat", "HORIZONTAL_Y", "", y)
    ya.CreateAttribute("units", [1], gdal.ExtendedDataType.CreateString(), []).WriteString(
        "degrees_north"
    )
    return xdim, ydim


def add_time_dim(rg, n_time: int, units: str, t_start: int = 0):
    tdim, ta = add_coord_var(
        rg, "time", "TEMPORAL", "", t_start + np.arange(n_time, dtype=np.float64)
    )
    ta.CreateAttribute("units", [1], gdal.ExtendedDataType.CreateString(), []).WriteString(units)
    return tdim


def new_netcdf(path: Path):
    ds = gdal.GetDriverByName("netCDF").CreateMultiDimensional(str(path))
    return ds


def write_array(rg, name: str, dims, vals: np.ndarray, no_data: float, long_name: str = "", units: str = ""):
    arr = rg.CreateMDArray(name, dims, F32, [])
    arr.SetNoDataValueDouble(no_data)
    if long_name:
        arr.CreateAttribute("long_name", [1], gdal.ExtendedDataType.CreateString(), []).WriteString(
            long_name
        )
    if units:
        arr.CreateAttribute("units", [1], gdal.ExtendedDataType.CreateString(), []).WriteString(
            units
        )
    arr.Write(vals)
    return arr


def time_series(nt: int, ntime: int, path: Path, t_start: int = 0):
    """(time, y, x) array, edges lon 0..240 step 30 (8 cols, anchored at world 0° so the
    global tiling grid's tile (0,0) covers it), edges lat 0..-8, value = t*1000 + y*10 + x."""
    width, height = 8, 8
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 0.0, lon_step=30.0)
    tdim = add_time_dim(rg, ntime, "days since 2000-01-01 00:00:00", t_start)
    ts = t_start + np.arange(ntime, dtype=np.int64)
    vals = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 1000 + vals.astype("float32")
    write_array(rg, "temperature", [tdim, ydim, xdim], vals, -9999)
    ds = None


def wrap_0_360_multitile(path: Path):
    """0..360 deg longitude at 0.25 deg, so the world spans several 512 px tile columns.

    `wrap_0_360.nc` is 360 columns at 1 deg, i.e. the whole world fits inside one tile
    column, so it cannot exercise the per-column stored<->presented mapping or the clip of a
    tile column against the presented extent. At 0.25 deg the 1440 columns need three tile
    columns, which is the layout NEX-GDDP-CMIP6 actually has.

    Latitude is kept to 4 rows and time to 2 slices so the fixture stays small (~46 kB);
    only the longitude axis needs to be wide.
    """
    width, height, ntime = 1440, 4, 2
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    # 0.25 deg cells centred, first cell edge on lon 0 and last edge on lon 360, so
    # `is_0_360_wrap` accepts it
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 1.0, lon_step=0.25, lat_step=0.25)
    tdim = add_time_dim(rg, ntime, "days since 2000-01-01 00:00:00")
    ts = np.arange(ntime, dtype=np.int64)
    rows = np.arange(height) * 10
    cols = np.arange(width)
    vals = ts[:, None, None] * 100_000 + rows[None, :, None] + cols[None, None, :]
    write_array(rg, "temperature", [tdim, ydim, xdim], vals.astype("float32"), -9999)
    ds = None


def cf_time_units_date_only(path: Path):
    """(time, y, x) array whose CF units carry a *date-only* origin.

    NEX-GDDP-CMIP6 writes `days since 1850-01-01`. The probe used to keep `"%Y-%m-%d"` in its
    origin format list, but `NaiveDateTime::parse_from_str` needs a time component, so that
    entry could never match: the origin failed to parse, the time axis was discarded, and the
    array was registered as `ZRole::Band` - one output band per time slice with synthetic
    `[k, k+1)` ms steps instead of real dates.
    """
    width, height, ntime = 8, 8, 8
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 0.0, lon_step=30.0)
    # no time component in the origin - this is the whole point
    tdim = add_time_dim(rg, ntime, "days since 1850-01-01")
    ts = np.arange(ntime, dtype=np.int64)
    vals = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 1000 + vals.astype("float32")
    write_array(rg, "temperature", [tdim, ydim, xdim], vals, -9999)
    ds = None


def zarr_time_series(path: Path):
    """(time, y, x) array via the ZARR driver, same formula as time_series.nc."""
    width, height, ntime = 8, 8, 8
    ds = gdal.GetDriverByName("ZARR").CreateMultiDimensional(str(path))
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 0.0, lon_step=30.0)
    tdim = add_time_dim(rg, ntime, "days since 2000-01-01 00:00:00")
    ts = np.arange(ntime, dtype=np.int64)
    vals = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 1000 + vals.astype("float32")
    write_array(rg, "temperature", [tdim, ydim, xdim], vals, -9999)
    ds = None


def wrap_0_360(path: Path):
    """(time, y, x) array stored on a 0..360 longitude grid, value = t*100 + y*10 + x."""
    width, height, ntime = 360, 10, 4
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 45.0)
    tdim = add_time_dim(rg, ntime, "days since 2000-01-01 00:00:00")
    ts = np.arange(ntime, dtype=np.int64)
    vals = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 100 + vals.astype("float32")
    write_array(rg, "temperature", [tdim, ydim, xdim], vals, -9999)
    ds = None


def bands(path: Path):
    """(band, y, x) array without CF time units on the leading dimension (ZRole::Band)."""
    width, height, nbands = 8, 8, 4
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 0.0, lon_step=30.0)
    bdim, _ba = add_coord_var(rg, "band", "BAND", "", np.arange(nbands, dtype=np.float64))
    vals = np.arange(height, dtype=np.float32)[:, None] * 10 + np.arange(width)[None, :]
    vals = np.arange(nbands, dtype=np.float32)[:, None, None] * 100 + vals[None, :, :]
    write_array(rg, "reflectance", [bdim, ydim, xdim], vals, -9999)
    ds = None


def cf_time_units_years(path: Path):
    """(time, y, x) array whose CF units are a variable-length `years since`.

    The probe cannot convert a calendar year to a fixed millisecond step, so it must reject
    this rather than silently fall back to bands. It used to: the array was registered as
    `ZRole::Band`, one band per slice with synthetic `[k, k+1)` ms steps.
    """
    width, height, ntime = 8, 8, 4
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 0.0, lon_step=30.0)
    tdim = add_time_dim(rg, ntime, "years since 2000-01-01 00:00:00")
    ts = np.arange(ntime, dtype=np.int64)
    vals = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 1000 + vals.astype("float32")
    write_array(rg, "temperature", [tdim, ydim, xdim], vals, -9999)
    ds = None


def projected_crs(path: Path):
    """(time, y, x) array in a projected CRS, declaring it via the CF `crs` attribute.

    The probe used to hardcode EPSG:4326 for every array, so this was silently mislabelled
    as geographic. It also carries no degrees x units, which is what forces the probe to
    actually resolve the CRS instead of inferring it.
    """
    width, height, ntime = 8, 8, 4
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    # metres, not degrees: a projected grid
    xdim, xa = add_coord_var(
        rg, "x", "HORIZONTAL_X", "", 500_000.0 + np.arange(width, dtype=np.float64) * 1000.0
    )
    ydim, ya = add_coord_var(
        rg, "y", "HORIZONTAL_Y", "", 5_000_000.0 - np.arange(height, dtype=np.float64) * 1000.0
    )
    xa.CreateAttribute("units", [1], gdal.ExtendedDataType.CreateString(), []).WriteString("m")
    ya.CreateAttribute("units", [1], gdal.ExtendedDataType.CreateString(), []).WriteString("m")
    tdim = add_time_dim(rg, ntime, "days since 2000-01-01 00:00:00")
    ts = np.arange(ntime, dtype=np.int64)
    vals = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 1000 + vals.astype("float32")
    arr = write_array(rg, "elevation", [tdim, ydim, xdim], vals, -9999)
    arr.CreateAttribute("crs", [1], gdal.ExtendedDataType.CreateString(), []).WriteString(
        "EPSG:32633"
    )
    ds = None


def time_depth_4d(path: Path):
    """(time, depth, y, x): z is the outermost dimension, `depth` is a leading prefix.

    Depth is the leading dimension a `leadingPrefix` selects: the probe reads `time` as the
    z axis and holds `depth` at a fixed index, so one dataset is one depth. Value at
    `(t, d, row, col)` is `t * 10_000 + d * 1000 + row * 10 + col`, which makes a wrong
    prefix or a wrong z mapping obvious.
    """
    width, height, ntime, ndepth = 8, 8, 6, 3
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 0.0, lon_step=30.0)
    # z first, as the design requires
    tdim = add_time_dim(rg, ntime, "days since 2000-01-01 00:00:00")
    ddim, _da = add_coord_var(
        rg, "depth", "DEPTH", "", np.arange(ndepth, dtype=np.float64) * 10.0
    )
    t = np.arange(ntime, dtype=np.int64)[:, None, None, None]
    d = np.arange(ndepth, dtype=np.int64)[None, :, None, None]
    r = np.arange(height, dtype=np.int64)[None, None, :, None]
    c = np.arange(width, dtype=np.int64)[None, None, None, :]
    vals = (t * 10_000 + d * 1000 + r * 10 + c).astype("float32")
    write_array(rg, "temperature", [tdim, ddim, ydim, xdim], vals, -9999)
    ds = None


def projected_no_crs(path: Path):
    """Projected grid (metre x units) with **no** CRS attribute.

    There is nothing left to infer a coordinate reference system from, so the probe has to
    refuse rather than fall back to EPSG:4326.
    """
    width, height, ntime = 8, 8, 4
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, xa = add_coord_var(
        rg, "x", "HORIZONTAL_X", "", 500_000.0 + np.arange(width, dtype=np.float64) * 1000.0
    )
    ydim, ya = add_coord_var(
        rg, "y", "HORIZONTAL_Y", "", 5_000_000.0 - np.arange(height, dtype=np.float64) * 1000.0
    )
    xa.CreateAttribute("units", [1], gdal.ExtendedDataType.CreateString(), []).WriteString("m")
    ya.CreateAttribute("units", [1], gdal.ExtendedDataType.CreateString(), []).WriteString("m")
    tdim = add_time_dim(rg, ntime, "days since 2000-01-01 00:00:00")
    ts = np.arange(ntime, dtype=np.int64)
    vals = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 1000 + vals.astype("float32")
    write_array(rg, "elevation", [tdim, ydim, xdim], vals, -9999)
    ds = None


def too_few_dims(path: Path):
    """(y, x) array -- one dimension short of a raster, so the probe must reject it.

    Rejection coverage for the *non-numeric datatype* path is intentionally absent: writing a
    GDAL string MD array through the python bindings is not worth the fight for a plain
    `try_into()` on the numeric datatype.
    """
    width, height = 8, 8
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, 0.0, lon_step=30.0)
    vals = np.arange(height, dtype="float32")[:, None] * 10 + np.arange(width)[None, :]
    write_array(rg, "grid_2d", [ydim, xdim], vals, -9999)
    ds = None


def cf_time_units_minutes(path: Path):
    """Same as time_series but time units `minutes since 1900-01-01 00:00:00Z` and
    ASCENDING latitudes (row 0 = south), so the read path must flip y."""
    width, height, ntime = 8, 8, 8
    ds = new_netcdf(path)
    rg = ds.GetRootGroup()
    xdim, ydim = add_xy_dims(rg, width, height, 0.0, -8.0, lon_step=30.0, lat_ascending=True)
    tdim = add_time_dim(rg, ntime, "minutes since 1900-01-01 00:00:00")
    ts = np.arange(ntime, dtype=np.int64)
    vals = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 1000 + vals.astype("float32")
    write_array(rg, "temperature", [tdim, ydim, xdim], vals, -9999)
    ds = None


def _fill_variables(group, width: int, height: int, ntime: int):
    """Three (time, y, x) data variables sharing one grid + CF attrs; the 1D coordinate
    variables (lon/lat/time) must be ignored by variable auto-selection."""
    xdim, ydim = add_xy_dims(group, width, height, 0.0, 0.0, lon_step=30.0)
    tdim = add_time_dim(group, ntime, "days since 2000-01-01 00:00:00")
    ts = np.arange(ntime, dtype=np.int64)
    base = (np.arange(height)[:, None] * 10 + np.arange(width)[None, :])[None, :, :]
    vals = ts[:, None, None] * 100 + base.astype("float32")
    write_array(group, "temperature", [tdim, ydim, xdim], vals, -9999, "air temperature", "K")
    write_array(group, "precipitation", [tdim, ydim, xdim], vals * 2, -9999, "precipitation", "mm")
    # no CF attrs -> band name falls back to the variable name
    write_array(group, "cloud_area_fraction", [tdim, ydim, xdim], vals * 3, -9999)


def variables(path: Path):
    """Root group with three CF data variables sharing (time, lat, lon) -> Geo Engine bands."""
    _fill_variables(new_netcdf(path).GetRootGroup(), 8, 8, 8)


def grouped_variables(path: Path):
    """Same as `variables` but the data arrays live in the `analysis` subgroup."""
    ds = new_netcdf(path)
    _fill_variables(ds.GetRootGroup().CreateGroup("analysis"), 8, 8, 8)


# where the example notebook's copy of `time_series.nc` goes; the notebook reads it from
# next to itself, so the example does not depend on a configured `test_data` volume
EXAMPLE_COPY = OUT_DIR.parents[2] / "python" / "examples" / "data" / "md_example.nc"


def main() -> None:
    # remove only the fixture files, keep this script itself (zarr is a directory)
    zarr_dir = OUT_DIR / "time_series.zarr"
    if zarr_dir.is_dir():
        shutil.move(str(zarr_dir), str(zarr_dir) + ".trash")
        shutil.rmtree(str(zarr_dir) + ".trash")
    for name in FIXTURES:
        p = OUT_DIR / name
        if p.is_dir():
            p.rmdir()
        p.unlink(missing_ok=True)
    time_series(8, 8, OUT_DIR / "time_series.nc")
    zarr_time_series(OUT_DIR / "time_series.zarr")
    time_series(2, 4, OUT_DIR / "time_series_split_a.nc", t_start=0)
    time_series(2, 4, OUT_DIR / "time_series_split_b.nc", t_start=4)
    # Same as the split pair but with a deliberate 4-step hole (t 4..8 is missing), so the
    # loading info's time axis has a gap-filling step in the middle. A file whose z_start
    # came from a running slice count instead of its position on the axis would be off by
    # one for every date after the gap.
    time_series(2, 4, OUT_DIR / "time_series_gap_a.nc", t_start=0)
    time_series(2, 4, OUT_DIR / "time_series_gap_b.nc", t_start=8)
    wrap_0_360(OUT_DIR / "wrap_0_360.nc")
    bands(OUT_DIR / "bands.nc")
    cf_time_units_minutes(OUT_DIR / "cf_time_units_minutes.nc")
    wrap_0_360_multitile(OUT_DIR / "wrap_0_360_multitile.nc")
    cf_time_units_date_only(OUT_DIR / "cf_time_units_date_only.nc")
    cf_time_units_years(OUT_DIR / "cf_time_units_years.nc")
    too_few_dims(OUT_DIR / "too_few_dims.nc")
    projected_crs(OUT_DIR / "projected_crs.nc")
    projected_no_crs(OUT_DIR / "projected_no_crs.nc")
    time_depth_4d(OUT_DIR / "time_depth_4d.nc")
    variables(OUT_DIR / "variables.nc")
    grouped_variables(OUT_DIR / "grouped_variables.nc")

    EXAMPLE_COPY.parent.mkdir(parents=True, exist_ok=True)
    time_series(8, 8, EXAMPLE_COPY)
    print(f"generated fixtures in {OUT_DIR}")


if __name__ == "__main__":
    sys.exit(main())