# OGCAPIApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**collection**](OGCAPIApi.md#collection) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId} | OGC API Collection Metadata |
| [**collectionCoverageTileset**](OGCAPIApi.md#collectioncoveragetileset) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles/{tileMatrixSetId} | OGC API Collection Coverage Tileset Metadata |
| [**collectionCoverageTilesets**](OGCAPIApi.md#collectioncoveragetilesets) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles | OGC API Collection Coverage Tilesets List |
| [**collectionTileset**](OGCAPIApi.md#collectiontileset) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId} | OGC API Collection Tileset Metadata |
| [**collectionTilesets**](OGCAPIApi.md#collectiontilesets) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles | OGC API Collection Tilesets List |
| [**collections**](OGCAPIApi.md#collections) | **GET** /ogc/{dataConnectorId}/{layerId}/collections | OGC API Collections List |
| [**conformance**](OGCAPIApi.md#conformance) | **GET** /ogc/{dataConnectorId}/{layerId}/conformance | OGC API Conformance Classes |
| [**coverageTile**](OGCAPIApi.md#coveragetile) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol} | OGC API Coverage Tile |
| [**landingPage**](OGCAPIApi.md#landingpage) | **GET** /ogc/{dataConnectorId}/{layerId}/ | OGC API Landing Page |
| [**tile**](OGCAPIApi.md#tile) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol} | OGC API Tile |
| [**tileMatrixSet**](OGCAPIApi.md#tilematrixset) | **GET** /ogc/{dataConnectorId}/{layerId}/tileMatrixSets/{tileMatrixSetId} | OGC API Tile Matrix Set Definition |
| [**tileMatrixSets**](OGCAPIApi.md#tilematrixsets) | **GET** /ogc/{dataConnectorId}/{layerId}/tileMatrixSets | OGC API Tile Matrix Set List |



## collection

> Collection collection(dataConnectorId, layerId)

OGC API Collection Metadata

Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { CollectionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
  } satisfies CollectionRequest;

  try {
    const data = await api.collection(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |

### Return type

[**Collection**](Collection.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## collectionCoverageTileset

> TileSet collectionCoverageTileset(dataConnectorId, layerId, tileMatrixSetId)

OGC API Collection Coverage Tileset Metadata

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html) and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { CollectionCoverageTilesetRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
    // TileMatrixSetId | Tile matrix set identifier
    tileMatrixSetId: ...,
  } satisfies CollectionCoverageTilesetRequest;

  try {
    const data = await api.collectionCoverageTileset(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |
| **tileMatrixSetId** | [](.md) | Tile matrix set identifier | [Defaults to `undefined`] |

### Return type

[**TileSet**](TileSet.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **404** | Collection or tile matrix set not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## collectionCoverageTilesets

> TileSets collectionCoverageTilesets(dataConnectorId, layerId)

OGC API Collection Coverage Tilesets List

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html) and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { CollectionCoverageTilesetsRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
  } satisfies CollectionCoverageTilesetsRequest;

  try {
    const data = await api.collectionCoverageTilesets(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |

### Return type

[**TileSets**](TileSets.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **404** | Collection not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## collectionTileset

> TileSet collectionTileset(dataConnectorId, layerId, tileMatrixSetId)

OGC API Collection Tileset Metadata

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { CollectionTilesetRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
    // TileMatrixSetId | Tile matrix set identifier
    tileMatrixSetId: ...,
  } satisfies CollectionTilesetRequest;

  try {
    const data = await api.collectionTileset(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |
| **tileMatrixSetId** | [](.md) | Tile matrix set identifier | [Defaults to `undefined`] |

### Return type

[**TileSet**](TileSet.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **404** | Collection or tile matrix set not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## collectionTilesets

> TileSets collectionTilesets(dataConnectorId, layerId)

OGC API Collection Tilesets List

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { CollectionTilesetsRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
  } satisfies CollectionTilesetsRequest;

  try {
    const data = await api.collectionTilesets(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |

### Return type

[**TileSets**](TileSets.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **404** | Collection not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## collections

> Collections collections(dataConnectorId, layerId, datetime, bbox, limit, f)

OGC API Collections List

Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).  Inside Geo Engine, every [&#x60;Layer&#x60;] gets its own OGC API endpoint. Inside this endpoint, this [&#x60;Layer&#x60;] is represented as a single [&#x60;Collection&#x60;](ogcapi_types::common::Collection). Therefore, the list of collections for a given layer will always contain exactly one collection, and the &#x60;collectionId&#x60; will always be the same as the [&#x60;LayerId&#x60;].  

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { CollectionsRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
    // string | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`). (optional)
    datetime: 2018-02-12T23:20:50Z,
    // string | Only features with geometries intersecting the bounding box are selected. Provide four or six comma-separated numbers in CRS84 order: minLon,minLat,maxLon,maxLat (optionally with vertical min/max). (optional)
    bbox: -180,-90,180,90,
    // number | Optional limit for the number of first-level collections returned (minimum: 1, maximum: 10000, default: 10). (optional)
    limit: 10,
    // CollectionsResponseFormat | Response format. If omitted, the `Accept` header is used. (optional)
    f: json,
  } satisfies CollectionsRequest;

  try {
    const data = await api.collections(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |
| **datetime** | `string` | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (&#x60;..&#x60;). | [Optional] [Defaults to `undefined`] |
| **bbox** | `string` | Only features with geometries intersecting the bounding box are selected. Provide four or six comma-separated numbers in CRS84 order: minLon,minLat,maxLon,maxLat (optionally with vertical min/max). | [Optional] [Defaults to `undefined`] |
| **limit** | `number` | Optional limit for the number of first-level collections returned (minimum: 1, maximum: 10000, default: 10). | [Optional] [Defaults to `undefined`] |
| **f** | `CollectionsResponseFormat` | Response format. If omitted, the &#x60;Accept&#x60; header is used. | [Optional] [Defaults to `undefined`] [Enum: json] |

### Return type

[**Collections**](Collections.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## conformance

> Conformance conformance(dataConnectorId, layerId)

OGC API Conformance Classes

Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { ConformanceRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
  } satisfies ConformanceRequest;

  try {
    const data = await api.conformance(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |

### Return type

[**Conformance**](Conformance.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## coverageTile

> Blob coverageTile(dataConnectorId, layerId, tileMatrixSetId, tileMatrix, tileRow, tileCol, datetime, properties, excludeProperties)

OGC API Coverage Tile

Returns the raw values of the tile as &#x60;GeoTIFF&#x60;, with one band per selected field. The tile covers the exact spatial extent of the tile and has the tile size in pixels as its number of cells.  Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html) and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html) (field selection and tiled coverage data).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { CoverageTileRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
    // TileMatrixSetId | Tile matrix set identifier
    tileMatrixSetId: ...,
    // number | Tile matrix level
    tileMatrix: 56,
    // number | Tile row
    tileRow: 789,
    // number | Tile column
    tileCol: 789,
    // string | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`). (optional)
    datetime: 2018-02-12T23:20:50Z,
    // string | Comma-separated list of the fields (bands) to return, in this order. If omitted, all fields are returned. Must not be combined with `exclude-properties`. (optional)
    properties: red,green,blue,
    // string | Comma-separated list of the fields (bands) not to return. An empty value returns all fields. Must not be combined with `properties`. (optional)
    excludeProperties: alpha,
  } satisfies CoverageTileRequest;

  try {
    const data = await api.coverageTile(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |
| **tileMatrixSetId** | [](.md) | Tile matrix set identifier | [Defaults to `undefined`] |
| **tileMatrix** | `number` | Tile matrix level | [Defaults to `undefined`] |
| **tileRow** | `number` | Tile row | [Defaults to `undefined`] |
| **tileCol** | `number` | Tile column | [Defaults to `undefined`] |
| **datetime** | `string` | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (&#x60;..&#x60;). | [Optional] [Defaults to `undefined`] |
| **properties** | `string` | Comma-separated list of the fields (bands) to return, in this order. If omitted, all fields are returned. Must not be combined with &#x60;exclude-properties&#x60;. | [Optional] [Defaults to `undefined`] |
| **excludeProperties** | `string` | Comma-separated list of the fields (bands) not to return. An empty value returns all fields. Must not be combined with &#x60;properties&#x60;. | [Optional] [Defaults to `undefined`] |

### Return type

**Blob**

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `image/tiff; application=geotiff`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | GeoTIFF Image |  -  |
| **400** | Invalid tile coordinates, datetime or field selection |  -  |
| **404** | Collection or tile matrix set not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## landingPage

> LandingPage landingPage(dataConnectorId, layerId)

OGC API Landing Page

Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { LandingPageRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
  } satisfies LandingPageRequest;

  try {
    const data = await api.landingPage(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |

### Return type

[**LandingPage**](LandingPage.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## tile

> Blob tile(dataConnectorId, layerId, tileMatrixSetId, tileMatrix, tileRow, tileCol, datetime)

OGC API Tile

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).  ## Sketch  &#x60;&#x60;&#x60;text pointOfOrigin (cornerOfOrigin&#x3D;topLeft)  tileMatrixMinX, tileMatrixMaxY                                               tileMatrixMaxX        |                                                                            |        v                                                                            v        +---------------------------+---------------------------+-----+---------------------------+ ---&gt; tileCol axis        | 0,0                       | 1,0                       | ... | matrixWidth-1,0           |        |                           |                           |     |                           |        +---------------------------+---------------------------+-----+---------------------------+        | 0,1                       | 1,1                       | ... | matrixWidth-1,1           |        |                           |                           |     |                           |        +---------------------------+---------------------------+-----+---------------------------+        | ...                       | ...                       | ... | ...                       |        |                           |                           |     |                           |        +---------------------------+---------------------------+-----+---------------------------+        | 0,                        | 1,                        | ... | matrixWidth-1,            |  v     |   matrixHeight-1          |   matrixHeight-1          |     |   matrixHeight-1          | --+ tileHeight tileMatrixMinY                     |                           |     |                           |   | (in pixels)        +---------------------------+---------------------------+-----+---------------------------+ --+  |                                                                   |&lt;-       tileWidth       -&gt;|  v                                                                   |        (in pixels)        | tileRow axis &#x60;&#x60;&#x60;  

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { TileRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
    // TileMatrixSetId | Tile matrix set identifier
    tileMatrixSetId: ...,
    // number | Tile matrix level
    tileMatrix: 56,
    // number | Tile row
    tileRow: 789,
    // number | Tile column
    tileCol: 789,
    // string | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`). (optional)
    datetime: 2018-02-12T23:20:50Z,
  } satisfies TileRequest;

  try {
    const data = await api.tile(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |
| **tileMatrixSetId** | [](.md) | Tile matrix set identifier | [Defaults to `undefined`] |
| **tileMatrix** | `number` | Tile matrix level | [Defaults to `undefined`] |
| **tileRow** | `number` | Tile row | [Defaults to `undefined`] |
| **tileCol** | `number` | Tile column | [Defaults to `undefined`] |
| **datetime** | `string` | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (&#x60;..&#x60;). | [Optional] [Defaults to `undefined`] |

### Return type

**Blob**

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `image/png`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | PNG Image |  -  |
| **400** | Invalid tile coordinates or datetime |  -  |
| **404** | Collection or tile matrix set not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## tileMatrixSet

> TileMatrixSet tileMatrixSet(dataConnectorId, layerId, tileMatrixSetId)

OGC API Tile Matrix Set Definition

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html). Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { TileMatrixSetRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
    // TileMatrixSetId | Tile matrix set identifier
    tileMatrixSetId: ...,
  } satisfies TileMatrixSetRequest;

  try {
    const data = await api.tileMatrixSet(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |
| **tileMatrixSetId** | [](.md) | Tile matrix set identifier | [Defaults to `undefined`] |

### Return type

[**TileMatrixSet**](TileMatrixSet.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **404** | Tile matrix set not found |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## tileMatrixSets

> TileMatrixSets tileMatrixSets(dataConnectorId, layerId)

OGC API Tile Matrix Set List

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html). Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).

### Example

```ts
import {
  Configuration,
  OGCAPIApi,
} from '@geoengine/api-client';
import type { TileMatrixSetsRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCAPIApi(config);

  const body = {
    // string | ID of the data connector
    dataConnectorId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | ID of the layer, which is used as collection ID
    layerId: layerId_example,
  } satisfies TileMatrixSetsRequest;

  try {
    const data = await api.tileMatrixSets(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **dataConnectorId** | `string` | ID of the data connector | [Defaults to `undefined`] |
| **layerId** | `string` | ID of the layer, which is used as collection ID | [Defaults to `undefined`] |

### Return type

[**TileMatrixSets**](TileMatrixSets.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

