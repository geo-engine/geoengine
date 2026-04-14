---
title: Workflows
---

This section introduces the workflow API of Geo Engine.

## ResultDescriptor

Call `/workflow/{workflowId}/metadata` to get the result descriptor of the workflow. It describes the result of the workflow by data type, spatial reference, temporal and spatial extent and some more information that is specific to raster and vector results.

### Example response for rasters

```json
{
    "type": "raster",
    "dataType": "U8",
    "spatialReference": "EPSG:4326",
    "measurement": {
        "type": "unitless"
    },
    "time": {
        "start": "2014-01-01T00:00:00.000Z",
        "end": "2014-07-01T00:00:00.000Z"
    },
    "bbox": {
        "upperLeftCoordinate": [-180.0, 90.0],
        "lowerRightCoordinate": [180.0, -90.0]
    }
}
```

### Example response for vectors

```json
{
    "type": "vector",
    "dataType": "MultiPoint",
    "spatialReference": "EPSG:4326",
    "columns": {
        "id": "int",
        "name": "text",
        "value": "float"
    },
    "time": {
        "start": "2014-04-01T00:00:00.000Z",
        "end": "2014-07-01T00:00:00.000Z"
    },
    "bbox": {
        "lowerLeftCoordinate": [3.9662060000000001, 45.9030360000000002],
        "upperRightCoordinate": [19.171284, 51.8473430000000022]
    }
}
```
