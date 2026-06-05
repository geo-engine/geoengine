# OGCAPIApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**collection**](OGCAPIApi.md#collection) | **GET** /ogc/ogc/{processingGraphId}/collections/{processingGraphId} | OGC API Collection Metadata |
| [**collectionTileset**](OGCAPIApi.md#collectiontileset) | **GET** /ogc/ogc/{processingGraphId}/collections/{collectionId}/tiles/{tileMatrixSetId} | OGC API Collection Tileset Metadata |
| [**collectionTilesets**](OGCAPIApi.md#collectiontilesets) | **GET** /ogc/ogc/{processingGraphId}/collections/{collectionId}/tiles | OGC API Collection Tilesets List |
| [**collections**](OGCAPIApi.md#collections) | **GET** /ogc/ogc/{processingGraphId}/collections | OGC API Collections List |
| [**conformance**](OGCAPIApi.md#conformance) | **GET** /ogc/ogc/{processingGraphId}/conformance | OGC API Conformance Classes |
| [**landingPage**](OGCAPIApi.md#landingpage) | **GET** /ogc/ogc/{processingGraphId}/ | OGC API Landing Page |
| [**tile**](OGCAPIApi.md#tile) | **GET** /ogc/ogc/{processingGraphId}/collections/{collectionId}/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol} | OGC API Tile |
| [**tileMatrixSet**](OGCAPIApi.md#tilematrixset) | **GET** /ogc/ogc/{processingGraphId}/tileMatrixSets/{tileMatrixSetId} | OGC API Tile Matrix Set Definition |
| [**tileMatrixSets**](OGCAPIApi.md#tilematrixsets) | **GET** /ogc/ogc/{processingGraphId}/tileMatrixSets | OGC API Tile Matrix Set List |



## collection

> Collection collection(processingGraphId)

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |

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


## collectionTileset

> TileSetMetadataResponse collectionTileset(processingGraphId, collectionId, tileMatrixSetId)

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Collection identifier
    collectionId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Tile matrix set identifier
    tileMatrixSetId: tileMatrixSetId_example,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |
| **collectionId** | `string` | Collection identifier | [Defaults to `undefined`] |
| **tileMatrixSetId** | `string` | Tile matrix set identifier | [Defaults to `undefined`] |

### Return type

[**TileSetMetadataResponse**](TileSetMetadataResponse.md)

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

> TileSetsResponse collectionTilesets(processingGraphId, collectionId)

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Collection identifier
    collectionId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |
| **collectionId** | `string` | Collection identifier | [Defaults to `undefined`] |

### Return type

[**TileSetsResponse**](TileSetsResponse.md)

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

> Collections collections(processingGraphId, datetime, bbox, limit, f)

OGC API Collections List

Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |
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

> Conformance conformance(processingGraphId)

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |

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


## landingPage

> LandingPage landingPage(processingGraphId)

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |

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

> Blob tile(processingGraphId, collectionId, tileMatrixSetId, tileMatrix, tileRow, tileCol, datetime)

OGC API Tile

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Collection identifier
    collectionId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Tile matrix set identifier
    tileMatrixSetId: tileMatrixSetId_example,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |
| **collectionId** | `string` | Collection identifier | [Defaults to `undefined`] |
| **tileMatrixSetId** | `string` | Tile matrix set identifier | [Defaults to `undefined`] |
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

> TileMatrixSet tileMatrixSet(processingGraphId, tileMatrixSetId)

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Tile matrix set identifier
    tileMatrixSetId: tileMatrixSetId_example,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |
| **tileMatrixSetId** | `string` | Tile matrix set identifier | [Defaults to `undefined`] |

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

> TileMatrixSets tileMatrixSets(processingGraphId)

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
    // string | ID of the processing graph, which is used as collection ID
    processingGraphId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
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
| **processingGraphId** | `string` | ID of the processing graph, which is used as collection ID | [Defaults to `undefined`] |

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

