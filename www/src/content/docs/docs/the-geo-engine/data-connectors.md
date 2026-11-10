---
title: Data Connectors
sidebar:
    order: 9
---

A data connector connects Geo Engine to an external data source.
It makes the data of that source available as [layers and layer collections](/docs/the-geo-engine/layers) and as [external datasets](/docs/the-geo-engine/datasets#external-data), without copying it into Geo Engine.
Source operators load external data with the same `data` parameter as internal datasets, using the `ExternalDataId`.

:::note
Data connectors were formerly called _data providers_.
The API, the configuration and the Python library still use the old name, e.g., `DataProviderId`, the `/layerDb/providers` endpoints, the `{provider}` path parameter, the `[dataprovider]` configuration section and the `…ProviderDefinition` types.
:::

## Configuring data connectors

A data connector is defined by a JSON _definition_.
Its `type` selects the kind of data connector, and its other fields configure it, e.g., the URL of the source, credentials, or a cache lifetime.
Every data connector has a name, a description and a `priority` that orders the data connectors in listings.
Some types take an `id` in their definition; the others, e.g., `Gbif` or `Pangaea`, always use the same fixed id.

Administrators manage data connector definitions with the `/layerDb/providers` endpoints or the `manager` app.
When the server creates its database schema, i.e., on the first start with an empty schema, it also loads the definitions in `[dataprovider] provider_defs_path`.
Files added there later are not loaded on a restart.
Access to a data connector is controlled by [permissions](/docs/the-geo-engine/users-and-permissions), like access to datasets.

STAC credentials are stored in PostgreSQL.
Set `[dataprovider] password_encryption_key` (or `GEOENGINE__DATAPROVIDER__PASSWORD_ENCRYPTION_KEY`) to store them encrypted.

## Available data connectors

| Type                     | Data source                                                                                                                          |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `StacProviderDefinition` | A collection of a [STAC](https://stacspec.org/) API, split into datasets of bands with the same data type, resolution and projection |
| `SentinelS2L2ACogs`      | Sentinel-2 Level-2A imagery as Cloud Optimized GeoTIFFs, found through a STAC API                                                    |
| `CopernicusDataspace`    | Imagery of the [Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu/), via its STAC and S3 endpoints                    |
| `NetCdfCf`               | NetCDF files that follow the CF conventions, on a server volume                                                                      |
| `EbvPortal`              | Essential Biodiversity Variables (EBV) cubes of the EBV Data Portal, stored as NetCDF                                                |
| `Edr`                    | An [OGC API – Environmental Data Retrieval](https://ogcapi.ogc.org/edr/) service                                                     |
| `Gbif`                   | Species occurrences of [GBIF](https://www.gbif.org/), from a PostgreSQL copy of the GBIF data                                        |
| `GfbioAbcd`              | Biodiversity data in the ABCD standard of the [GFBio](https://www.gfbio.org/) collections, from PostgreSQL                           |
| `GfbioCollections`       | Collections of the GFBio collection API, combined with ABCD and Pangaea data                                                         |
| `Pangaea`                | Datasets of the [PANGAEA](https://www.pangaea.de/) data publisher                                                                    |
| `WildLIVE!`              | Camera trap data of the WildLIVE! portal                                                                                             |
| `DatasetLayerListing`    | Not an external source: lists internal datasets as layers, grouped into collections by tags                                          |

The fields of each type are part of the [OpenAPI specification](/docs/api/rest-api#openapi-specification), under `TypedDataProviderDefinition`.

## Using data from data connectors

You do not need to know which data connector a layer comes from to use it.
Browse the layer collections of a data connector, then register the layer as a processing graph:

```bash
curl -X POST http://localhost:3030/api/layers/<data connector id>/<layer id>/processingGraphId \
    -H "Authorization: Bearer $TOKEN"
```

To keep a copy of external raster data in Geo Engine, `POST /layers/{provider}/{layer}/dataset` persists a layer as an internal dataset.
