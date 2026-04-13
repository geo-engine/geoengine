---
title: Layers
---

<!-- TODO: Link to workflow section in `/geoengine` rather than `/api` when available -->

A layer is a browsable unit in Geo Engine.
In general, it is a named [`Workflow`](../api/workflows.md) with additional meta information like a description and a default [`Colorizer`](../datatypes/colorizer.md).
Layers are identified by a `LayerId`, which is usually a UUID.
Every layer can be part of one or more [`Layer collections`](./layers.md#layer-collections).

## Layer collections

Layer collections are groups of [`Layers`](./layers.md).
The collections themselves can be grouped inside other collections.
Every layer collection has a name and a description.
Layer collections, just like layers, can be part of one or more other layer collections.

## Browsing

Inside Geo Engine's web interface, you can browse the available layers and layer collections when adding data.

Inside Python, you can use the

```python
ge.layer_collection()
```

function to get a list of the root collection which contains paths to all underlying layers.
