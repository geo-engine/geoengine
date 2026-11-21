---
title: Use Geo Engine with an AI Agent
sidebar:
    order: 2
---

<!-- Generated from ../python/examples/ai_agent_workflow.ipynb by plugins/notebook.ts. Do not edit. -->

This page is written for AI agents that work with Geo Engine for a user, and for the people who set them up.
An agent that can run Python should use the [`geoengine` Python library](/docs/python/).
It needs no special interface, only this page and the reference pages it links to.

## Give your agent the docs

- `https://www.geoengine.io/llms.txt` lists all pages of this documentation.
- Every page is also served as Markdown: append `.md` to its path, e.g., `https://www.geoengine.io/docs/operators/expression.md`.
- The [Geo Engine repository](https://github.com/geo-engine/geoengine) contains the agent skill `.agents/skills/use-geoengine/`, which leads an agent to this page when a task needs Geo Engine.
  Copy the folder into the skills directory of your agent.

## Workflow

Work in these steps.
Each one is cheap, so check the result of a step before you go on.

### 1. Connect

```python
import geoengine as ge

ge.initialize("http://localhost:3030/api")
```

Ask the user for the server URL.
Without credentials, this creates an anonymous session.
For a registered user, set the environment variables `GEOENGINE_EMAIL` and `GEOENGINE_PASSWORD`, or `GEOENGINE_TOKEN`, instead of writing secrets into code.

### 2. Find data

Data is offered as [layers](/docs/the-geo-engine/layers) in collections, and as [datasets](/docs/the-geo-engine/datasets) with a name.

```python
root = ge.layer_collection()  # the collections of all data providers
catalog = root.get_items_by_name_unique("Data Catalog").load()

for item in catalog.search("ports"):
    print(item.name, item.description)

layer = catalog.search("ports")[0].load()
ports_in_germany = layer.as_processing_graph()

for dataset in ge.list_datasets():
    print(dataset.name, dataset.display_name)
```

Search inside the collection of one provider; a search over the root collection is not supported.

### 3. Build and register a processing graph

A [processing graph](/docs/the-geo-engine/processing-graphs) is a JSON tree of operators.
Read the page of each [operator](/docs/operators/) or [plot](/docs/plots/) before you use it.
It lists the parameters and sources and has an example that you can copy.

```python
ndvi = ge.register_processing_graph(
    {
        "type": "Raster",
        "operator": {"type": "GdalSource", "params": {"data": "ndvi"}},
    }
)
```

The top-level `type` is `Raster`, `Vector` or `Plot`, depending on what the outermost operator returns.
Registering computes nothing and is idempotent, so build a large graph in small steps and register each of them.

### 4. Check what the result looks like

```python
print(ndvi.get_result_descriptor())
```

The result descriptor tells you the data type, the spatial reference, the time bounds, and the bands of a raster or the columns of a vector result.
You need them for the query in the next step.

### 5. Look at a small result first

A query needs a bounding box and a time interval.
For rasters, also pass a coarse `spatial_resolution` while you explore.

```python
from datetime import UTC, datetime

time = datetime(2014, 4, 1, tzinfo=UTC)
query = ge.QueryRectangle((-180.0, -90.0, 180.0, 90.0), (time, time))  # xmin, ymin, xmax, ymax

# a picture that you can look at
image = ndvi.wms_get_map_as_image(
    query,
    ge.SingleBandRasterColorizer(band=0, band_colorizer=ge.Colorizer.linear_with_mpl_cmap("viridis", (0, 255))),
    ge.SpatialResolution(0.5, 0.5),
)
image  # in a script: image.save("ndvi.png"), then look at the file
```

```python
# numbers instead of pixels
statistics = ge.register_processing_graph(
    {
        "type": "Plot",
        "operator": {
            "type": "Statistics",
            "params": {"percentiles": []},
            "sources": {"source": {"type": "GdalSource", "params": {"data": "ndvi"}}},
        },
    }
)
print(statistics.plot_json(query, ge.SpatialResolution(0.5, 0.5)).data["vegaSpec"]["data"]["values"])
```

```python
# a vector result as a GeoPandas data frame
ports_in_germany.get_dataframe(query).head()
```

### 6. Fetch the full result

| Result                              | Method                                                               |
| ----------------------------------- | -------------------------------------------------------------------- |
| Vector, as a GeoPandas data frame   | `get_dataframe(query)`                                               |
| Raster, as an xarray `DataArray`    | `get_xarray(query, spatial_resolution)`                              |
| Raster, as a GeoTIFF file           | `download_raster(query, file_path)`                                  |
| Plot, as a Vega-Lite chart or JSON  | `plot_chart(query)`, `plot_json(query)`                              |
| Large results, tile by tile         | `raster_stream(query)`, `vector_stream(query)` (async)               |
| Raster, stored as a dataset for use | `save_as_dataset(query, name, display_name)`, then wait for the task |

See the [`ProcessingGraph` reference](/docs/python/processing_graph) for all parameters.

## Rules that prevent most mistakes

- **Time is never optional.**
  Take the time of a query from the time bounds of the result descriptor.
  A raster query outside the time bounds does not fail; it returns no data, which looks like a raster of zeros.
- **The bounding box is in the spatial reference of the result.**
  `QueryRectangle` assumes `EPSG:4326`; pass `srs=` for anything else.
- **Nothing is reprojected implicitly.**
  Operators with several sources need the same spatial reference for all of them.
  Add a [`Reprojection`](/docs/operators/reprojection) operator where they differ.
- **Parameters are required unless their description says _Optional_.**
  Use the names and the nesting from the operator page, including whether a source is one operator or a list.
- **Start small.**
  Results are computed for the queried area, time and resolution only.
  A whole dataset in its native resolution can be very large, so explore with a small area or a coarse resolution, and use a [`Statistics`](/docs/plots/statistics) plot where numbers are enough.
- **Read the errors.**
  The server answers with an error name and a message, e.g., `UnknownDatasetName: Dataset name 'nope' does not exist`.
  A `ValueError` that starts with `No match found when deserializing the JSON string into ProcessingGraph` comes from the Python library, before any request: the JSON does not match the schema of an operator.
  Its text lists all operators and is not helpful, so compare your JSON with the example on the operator page.

## Without Python

Agents that cannot run Python can use the [REST API](/docs/api/rest-api) with the same steps; the [Quickstart](/docs/getting-started/quickstart) shows them with `curl`.
The complete API is described by the OpenAPI document that every server provides at `/api/api-docs/openapi.json`.
