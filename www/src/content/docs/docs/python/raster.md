---
sidebar_label: raster
title: raster
---

Raster data types

## RasterTile2D Objects

```python
class RasterTile2D()
```

A 2D raster tile as produced by the Geo Engine

#### \_\_init\_\_

```python
def __init__(shape: tuple[int, int], data: pa.Array,
             geo_transform: gety.GeoTransform, crs: str,
             time: gety.TimeInterval, band: int)
```

Create a RasterTile2D object

#### shape

```python
@property
def shape() -> tuple[int, int]
```

Return the shape of the raster tile in numpy order (y_size, x_size)

#### data_type

```python
@property
def data_type() -> pa.DataType
```

Return the arrow data type of the raster tile

#### numpy_data_type

```python
@property
def numpy_data_type() -> np.dtype
```

Return the numpy dtype of the raster tile

#### has_null_values

```python
@property
def has_null_values() -> bool
```

Return whether the raster tile has null values

#### to_numpy_data_array

```python
def to_numpy_data_array(fill_null_value=0) -> np.ndarray
```

Return the raster tile as a numpy array.
Caution: this will not mask nodata values but replace them with the provided value !

#### to_numpy_mask_array

```python
def to_numpy_mask_array(nan_is_null=False) -> np.ndarray | None
```

Return the raster tiles mask as a numpy array.
True means no data, False means data.
If the raster tile has no null values, None is returned.
It is possible to specify whether NaN values should be considered as no data when creating the mask.

#### to_numpy_masked_array

```python
def to_numpy_masked_array(nan_is_null=False) -> np.ma.MaskedArray
```

Return the raster tile as a masked numpy array

#### coords_x

```python
def coords_x(pixel_center=False) -> np.ndarray
```

Return the x coordinates of the raster tile
If pixel_center is True, the coordinates will be the center of the pixels.
Otherwise they will be the upper left edges.

#### coords_y

```python
def coords_y(pixel_center=False) -> np.ndarray
```

Return the y coordinates of the raster tile
If pixel_center is True, the coordinates will be the center of the pixels.
Otherwise they will be the upper left edges.

#### to_xarray

```python
def to_xarray(
        clip_with_bounds: gety.SpatialBounds | None = None) -> xr.DataArray
```

Return the raster tile as an xarray.DataArray.

**Notes**:

- Xarray does not support masked arrays.
- Masked pixels are converted to NaNs and the nodata value is set to NaN as well.
- Xarray uses numpy&#x27;s datetime64[ns] which only covers the years from 1678 to 2262.
- Date times that are outside of the defined range are clipped to the limits of the range.

#### spatial_partition

```python
def spatial_partition() -> gety.SpatialPartition2D
```

Return the spatial partition of the raster tile

#### is_empty

```python
def is_empty() -> bool
```

Returns true if the tile is empty

#### from_ge_record_batch

```python
@staticmethod
def from_ge_record_batch(record_batch: pa.RecordBatch) -> RasterTile2D
```

Create a RasterTile2D from an Arrow record batch recieved from the Geo Engine

## RasterTileStack2D Objects

```python
class RasterTileStack2D()
```

A stack of all the bands of a raster tile as produced by the Geo Engine

#### \_\_init\_\_

```python
def __init__(tile_shape: tuple[int, int], data: list[pa.Array],
             geo_transform: gety.GeoTransform, crs: str,
             time: gety.TimeInterval, bands: list[int])
```

Create a RasterTileStack2D object

#### single_band

```python
def single_band(index: int) -> RasterTile2D
```

Return a single band from the stack

#### to_numpy_masked_array_stack

```python
def to_numpy_masked_array_stack() -> np.ma.MaskedArray
```

Return the raster stack as a 3D masked numpy array

#### to_xarray

```python
def to_xarray(
        clip_with_bounds: gety.SpatialBounds | None = None) -> xr.DataArray
```

Return the raster stack as an xarray.DataArray

#### tile_stream_to_stack_stream

```python
async def tile_stream_to_stack_stream(
    raster_stream: AsyncIterator[RasterTile2D]
) -> AsyncIterator[RasterTileStack2D]
```

Convert a stream of raster tiles to stream of stacked tiles
