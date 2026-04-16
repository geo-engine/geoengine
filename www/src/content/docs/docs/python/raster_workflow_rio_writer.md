---
sidebar_label: raster_workflow_rio_writer
title: raster_workflow_rio_writer
---

A module that contains classes to write raster data from a Geo Engine raster workflow.

## RasterWorkflowRioWriter Objects

```python
class RasterWorkflowRioWriter()
```

A class to write raster data from a Geo Engine raster workflow to a GDAL dataset.
It creates a new dataset for each time interval and writes the tiles to the dataset.
Multiple bands are supported and the bands are written to the dataset in the order of the result descriptor.

#### \_\_init\_\_

```python
def __init__(dataset_prefix,
             workflow: Workflow,
             no_data_value=0,
             data_type: np.dtype | None = None,
             print_info=False,
             rio_kwargs=None)
```

Create a new RasterWorkflowGdalWriter instance.

#### close_current_dataset

```python
def close_current_dataset()
```

Close the current dataset

#### create_gdal_geo_transform_width_height

```python
def create_gdal_geo_transform_width_height(query: QueryRectangle)
```

Create the tiling geo transform, width and height for the current query.

#### query_and_write

```python
async def query_and_write(query: RasterQueryRectangle, skip_empty_times=True)
```

Query the raster workflow and write the resulting tiles to a GDAL dataset per timeslice.

**Arguments**:

- `query`: The QueryRectangle to write to GDAL dataset(s)
- `skip_empty_times`: Skip timeslices where all pixels are empty/nodata
