---
sidebar_label: workflow
title: workflow
---

A workflow representation and methods on workflows

## WorkflowId Objects

```python
class WorkflowId()
```

A wrapper around a workflow UUID

#### \_\_init\_\_

```python
def __init__(workflow_id: UUID | str) -> None
```

Create a new WorkflowId from an UUID or uuid as str

#### from_response

```python
@classmethod
def from_response(cls, response: geoc.IdResponse) -> WorkflowId
```

Create a `WorkflowId` from an http response

## RasterStreamProcessing Objects

```python
class RasterStreamProcessing()
```

Helper class to process raster stream data

#### read_arrow_ipc

```python
@classmethod
def read_arrow_ipc(cls, arrow_ipc: bytes) -> pa.RecordBatch
```

Read an Arrow IPC file from a byte array

#### process_bytes

```python
@classmethod
def process_bytes(cls, tile_bytes: bytes | None) -> RasterTile2D | None
```

Process a tile from a byte array

#### merge_tiles

```python
@classmethod
def merge_tiles(cls, tiles: list[xr.DataArray]) -> xr.DataArray | None
```

Merge a list of tiles into a single xarray

## Workflow Objects

```python
class Workflow()
```

Holds a workflow id and allows querying data

#### get_result_descriptor

```python
def get_result_descriptor() -> ResultDescriptor
```

Return the metadata of the workflow result

#### workflow_definition

```python
def workflow_definition(timeout: int = 60) -> geoc.Workflow
```

Return the workflow definition for this workflow

#### get_dataframe

```python
def get_dataframe(bbox: QueryRectangle,
                  timeout: int = 3600,
                  resolve_classifications: bool = False) -> gpd.GeoDataFrame
```

Query a workflow and return the WFS result as a GeoPandas `GeoDataFrame`

#### wms_get_map_as_image

```python
def wms_get_map_as_image(bbox: QueryRectangle,
                         raster_colorizer: RasterColorizer,
                         spatial_resolution: SpatialResolution) -> Image.Image
```

Return the result of a WMS request as a PIL Image

#### plot_json

```python
def plot_json(bbox: QueryRectangle,
              spatial_resolution: SpatialResolution | None = None,
              timeout: int = 3600) -> geoc.WrappedPlotOutput
```

Query a workflow and return the plot chart result as WrappedPlotOutput

#### plot_chart

```python
def plot_chart(bbox: QueryRectangle,
               spatial_resolution: SpatialResolution | None = None,
               timeout: int = 3600) -> VegaLite
```

Query a workflow and return the plot chart result as a vega plot

#### get_array

```python
def get_array(bbox: QueryRectangle,
              spatial_resolution: SpatialResolution | None = None,
              timeout=3600,
              force_no_data_value: float | None = None) -> np.ndarray
```

Query a workflow and return the raster result as a numpy array

## Parameters

bbox : A bounding box for the query
timeout : HTTP request timeout in seconds
force_no_data_value: If not None, use this value as no data value for the requested raster data. Otherwise, use the Geo Engine will produce masked rasters.

#### get_xarray

```python
def get_xarray(bbox: QueryRectangle,
               spatial_resolution: SpatialResolution | None = None,
               timeout=3600,
               force_no_data_value: float | None = None) -> xr.DataArray
```

Query a workflow and return the raster result as a georeferenced xarray

## Parameters

bbox : A bounding box for the query
timeout : HTTP request timeout in seconds
force_no_data_value: If not None, use this value as no data value for the requested raster data. Otherwise, use the Geo Engine will produce masked rasters.

#### download_raster

```python
def download_raster(
        bbox: QueryRectangle,
        file_path: str,
        timeout=3600,
        file_format: str = "image/tiff",
        force_no_data_value: float | None = None,
        spatial_resolution: SpatialResolution | None = None) -> None
```

Query a workflow and save the raster result as a file on disk

## Parameters

bbox : A bounding box for the query
file_path : The path to the file to save the raster to
timeout : HTTP request timeout in seconds
file_format : The format of the returned raster
force_no_data_value: If not None, use this value as no data value for the requested raster data. Otherwise, use the Geo Engine will produce masked rasters.

#### get_provenance

```python
def get_provenance(timeout: int = 60) -> list[ProvenanceEntry]
```

Query the provenance of the workflow

#### metadata_zip

```python
def metadata_zip(path: PathLike | BytesIO, timeout: int = 60) -> None
```

Query workflow metadata and citations and stores it as zip file to `path`

#### save_as_dataset

```python
def save_as_dataset(query_rectangle: QueryRectangle,
                    name: None | str,
                    display_name: str,
                    description: str = "",
                    timeout: int = 3600) -> Task
```

Init task to store the workflow result as a layer

#### raster_stream

```python
async def raster_stream(query_rectangle: QueryRectangle | RasterQueryRectangle,
                        open_timeout: int = 60) -> AsyncIterator[RasterTile2D]
```

Stream the workflow result as series of RasterTile2D (transformable to numpy and xarray)

#### raster_stream_into_xarray

```python
async def raster_stream_into_xarray(query_rectangle: RasterQueryRectangle,
                                    clip_to_query_rectangle: bool = False,
                                    open_timeout: int = 60) -> xr.DataArray
```

Stream the workflow result into memory and output a single xarray.

NOTE: You can run out of memory if the query rectangle is too large.

#### vector_stream

```python
async def vector_stream(
        query_rectangle: QueryRectangle,
        time_start_column: str = "time_start",
        time_end_column: str = "time_end",
        open_timeout: int = 60) -> AsyncIterator[gpd.GeoDataFrame]
```

Stream the workflow result as series of `GeoDataFrame`s

#### vector_stream_into_geopandas

```python
async def vector_stream_into_geopandas(
        query_rectangle: QueryRectangle,
        time_start_column: str = "time_start",
        time_end_column: str = "time_end",
        open_timeout: int = 60) -> gpd.GeoDataFrame
```

Stream the workflow result into memory and output a single geo data frame.

NOTE: You can run out of memory if the query rectangle is too large.

#### register_workflow

```python
def register_workflow(workflow: dict[str, Any] | WorkflowBuilderOperator,
                      timeout: int = 60) -> Workflow
```

Register a workflow in Geo Engine and receive a `WorkflowId`

#### workflow_by_id

```python
def workflow_by_id(workflow_id: UUID | str) -> Workflow
```

Create a workflow object from a workflow id

#### get_quota

```python
def get_quota(user_id: UUID | None = None, timeout: int = 60) -> geoc.Quota
```

Gets a user&#x27;s quota. Only admins can get other users&#x27; quota.

#### update_quota

```python
def update_quota(user_id: UUID,
                 new_available_quota: int,
                 timeout: int = 60) -> None
```

Update a user&#x27;s quota. Only admins can perform this operation.

#### data_usage

```python
def data_usage(offset: int = 0, limit: int = 10) -> list[geoc.DataUsage]
```

Get data usage

#### data_usage_summary

```python
def data_usage_summary(granularity: geoc.UsageSummaryGranularity,
                       dataset: str | None = None,
                       offset: int = 0,
                       limit: int = 10) -> pd.DataFrame
```

Get data usage summary
