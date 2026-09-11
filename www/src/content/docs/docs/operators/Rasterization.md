---
title: Rasterization
---

The `Rasterization` operator creates a raster from a point vector source.
It offers two options for rasterization: A grid rasterization and a (gaussian) density rasterization (heatmap).

## Errors

If the `cutoff` is not in `[0, 1)` or the `stddev` is negative, an error will be thrown.

## Parameters

| Name              | Type                  | Description                                             | Examples |
| ----------------- | --------------------- | ------------------------------------------------------- | -------- |
| spatialResolution | SpatialResolution     | Spatial resolution of the output grid in x/y direction. |          |
| originCoordinate  | Coordinate2D          | Origin coordinate to which the raster grid is aligned.  |          |
| densityParams     | null or DensityParams |                                                         |          |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| vector | VectorOperator | An operator that produces vector data. |

## Examples

```json
{
    "type": "Raster",
    "operator": {
        "type": "Rasterization",
        "params": {
            "type": "grid",
            "spatialResolution": {
                "x": 10,
                "y": 10
            },
            "gridSizeMode": "fixed",
            "originCoordinate": {
                "x": 0,
                "y": 0
            }
        },
        "sources": {
            "vector": {
                "type": "OgrSource",
                "params": {
                    "data": "ne_10m_ports",
                    "attributeProjection": null,
                    "attributeFilters": null
                }
            }
        }
    }
}
```

```json
{
    "type": "Raster",
    "operator": {
        "type": "Rasterization",
        "params": {
            "type": "density",
            "cutoff": 0.01,
            "stddev": 1
        },
        "sources": {
            "vector": {
                "type": "OgrSource",
                "params": {
                    "data": "ne_10m_ports",
                    "attributeProjection": null,
                    "attributeFilters": null
                }
            }
        }
    }
}
```
