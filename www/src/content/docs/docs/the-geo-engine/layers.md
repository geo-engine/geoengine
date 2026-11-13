---
title: Layers
sidebar:
    order: 8
---

A layer is a browsable unit in Geo Engine.
In general, it is a named [processing graph](/docs/the-geo-engine/processing-graphs) with additional meta information like a description and a default [`Colorizer`](/docs/datatypes/colorizer).
Layers are identified by a `LayerId`, which is usually a UUID.
Every layer can be part of one or more [`Layer collections`](/docs/the-geo-engine/layers#layer-collections).

Layers are what users see when they add data in the web apps.
A layer often wraps a single dataset with a source operator, but it can contain any processing graph, e.g., a precomputed index like the NDVI.

## Layer collections

Layer collections are groups of [`Layers`](/docs/the-geo-engine/layers).
The collections themselves can be grouped inside other collections.
Every layer collection has a name and a description.
Layer collections, just like layers, can be part of one or more other layer collections.

## Where layers come from

Layers and collections come from these sources:

- The **internal layer database** holds the layers and collections that users and administrators create, with `/layerDb/…` endpoints.
- Every [data connector](/docs/the-geo-engine/data-connectors) presents its data as layers and collections, too, e.g., a STAC data connector has a layer per dataset.

Every layer is therefore identified by the id of its source (the internal layer database or a data connector) and its layer id.
In the API, this source id is called `provider`.
`GET /layers/collections` lists the root collections of all sources.

To use a layer in a computation, get its processing graph with `GET /layers/{provider}/{layer}` or register it directly with `POST /layers/{provider}/{layer}/processingGraphId`.

## Browsing

Inside Geo Engine's web interface, you can browse the available layers and layer collections when adding data.

Inside Python, you can use the

```python
ge.layer_collection()
```

function to get a list of the root collection which contains paths to all underlying layers.

Collections of data connectors that support it can be searched with `GET /layers/collections/search/{provider}/{collection}`.

## Projects

A project is a map that a user composes in the web apps: a set of layers with their symbology, a spatial reference, and the spatial and temporal extent of the view.
Every change to a project creates a new version, so earlier versions remain available (`GET /project/{project}/versions`).
Like datasets and layers, projects are shared through [permissions](/docs/the-geo-engine/users-and-permissions).
