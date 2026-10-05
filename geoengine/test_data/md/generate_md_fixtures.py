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
- bands.nc                ZRole::Band, dims (band, y, x), no CF time units
- cf_time_units_minutes.nc ZRole::Time, CF units `minutes since 1900-01-01`, ASCENDING lat
- variables.nc            ZRole::Variable: 3 CF data variables (temperature/precipitation/
                          cloud_area_fraction) sharing (time, lat, lon), with long_name/units
- grouped_variables.nc    same arrays as variables.nc but inside the `analysis` subgroup

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
    "bands.nc",
    "cf_time_units_minutes.nc",
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
    variables(OUT_DIR / "variables.nc")
    grouped_variables(OUT_DIR / "grouped_variables.nc")
    print(f"generated fixtures in {OUT_DIR}")


if __name__ == "__main__":
    sys.exit(main())