# \OgcapiApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**collection**](OgcapiApi.md#collection) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId} | OGC API Collection Metadata
[**collection_coverage_tileset**](OgcapiApi.md#collection_coverage_tileset) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles/{tileMatrixSetId} | OGC API Collection Coverage Tileset Metadata
[**collection_coverage_tilesets**](OgcapiApi.md#collection_coverage_tilesets) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles | OGC API Collection Coverage Tilesets List
[**collection_tileset**](OgcapiApi.md#collection_tileset) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId} | OGC API Collection Tileset Metadata
[**collection_tilesets**](OgcapiApi.md#collection_tilesets) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles | OGC API Collection Tilesets List
[**collections**](OgcapiApi.md#collections) | **GET** /ogc/{dataConnectorId}/{layerId}/collections | OGC API Collections List
[**conformance**](OgcapiApi.md#conformance) | **GET** /ogc/{dataConnectorId}/{layerId}/conformance | OGC API Conformance Classes
[**coverage_tile**](OgcapiApi.md#coverage_tile) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/coverage/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol} | OGC API Coverage Tile
[**landing_page**](OgcapiApi.md#landing_page) | **GET** /ogc/{dataConnectorId}/{layerId}/ | OGC API Landing Page
[**tile**](OgcapiApi.md#tile) | **GET** /ogc/{dataConnectorId}/{layerId}/collections/{layerId}/map/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol} | OGC API Tile
[**tile_matrix_set**](OgcapiApi.md#tile_matrix_set) | **GET** /ogc/{dataConnectorId}/{layerId}/tileMatrixSets/{tileMatrixSetId} | OGC API Tile Matrix Set Definition
[**tile_matrix_sets**](OgcapiApi.md#tile_matrix_sets) | **GET** /ogc/{dataConnectorId}/{layerId}/tileMatrixSets | OGC API Tile Matrix Set List



## collection

> models::Collection collection(data_connector_id, layer_id)
OGC API Collection Metadata

Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |

### Return type

[**models::Collection**](Collection.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## collection_coverage_tileset

> models::TileSet collection_coverage_tileset(data_connector_id, layer_id, tile_matrix_set_id)
OGC API Collection Coverage Tileset Metadata

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html) and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |
**tile_matrix_set_id** | **String** | Tile matrix set identifier | [required] |

### Return type

[**models::TileSet**](TileSet.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## collection_coverage_tilesets

> models::TileSets collection_coverage_tilesets(data_connector_id, layer_id)
OGC API Collection Coverage Tilesets List

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html) and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |

### Return type

[**models::TileSets**](TileSets.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## collection_tileset

> models::TileSet collection_tileset(data_connector_id, layer_id, tile_matrix_set_id)
OGC API Collection Tileset Metadata

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |
**tile_matrix_set_id** | **String** | Tile matrix set identifier | [required] |

### Return type

[**models::TileSet**](TileSet.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## collection_tilesets

> models::TileSets collection_tilesets(data_connector_id, layer_id)
OGC API Collection Tilesets List

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |

### Return type

[**models::TileSets**](TileSets.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## collections

> models::Collections collections(data_connector_id, layer_id, datetime, bbox, limit, f)
OGC API Collections List

Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).  Inside Geo Engine, every [`Layer`] gets its own OGC API endpoint. Inside this endpoint, this [`Layer`] is represented as a single [`Collection`](ogcapi_types::common::Collection). Therefore, the list of collections for a given layer will always contain exactly one collection, and the `collectionId` will always be the same as the [`LayerId`].  

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |
**datetime** | Option<**String**> | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`). |  |
**bbox** | Option<**String**> | Only features with geometries intersecting the bounding box are selected. Provide four or six comma-separated numbers in CRS84 order: minLon,minLat,maxLon,maxLat (optionally with vertical min/max). |  |
**limit** | Option<**i32**> | Optional limit for the number of first-level collections returned (minimum: 1, maximum: 10000, default: 10). |  |
**f** | Option<**String**> | Response format. If omitted, the `Accept` header is used. |  |

### Return type

[**models::Collections**](Collections.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## conformance

> models::Conformance conformance(data_connector_id, layer_id)
OGC API Conformance Classes

Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |

### Return type

[**models::Conformance**](Conformance.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## coverage_tile

> std::path::PathBuf coverage_tile(data_connector_id, layer_id, tile_matrix_set_id, tile_matrix, tile_row, tile_col, datetime, properties, exclude_properties)
OGC API Coverage Tile

Returns the raw values of the tile as `GeoTIFF`, with one band per selected field. The tile covers the exact spatial extent of the tile and has the tile size in pixels as its number of cells.  Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/20-057/20-057.html) and [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html) (field selection and tiled coverage data).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |
**tile_matrix_set_id** | **String** | Tile matrix set identifier | [required] |
**tile_matrix** | **i32** | Tile matrix level | [required] |
**tile_row** | **i64** | Tile row | [required] |
**tile_col** | **i64** | Tile column | [required] |
**datetime** | Option<**String**> | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`). |  |
**properties** | Option<**String**> | Comma-separated list of the fields (bands) to return, in this order. If omitted, all fields are returned. Must not be combined with `exclude-properties`. |  |
**exclude_properties** | Option<**String**> | Comma-separated list of the fields (bands) not to return. An empty value returns all fields. Must not be combined with `properties`. |  |

### Return type

[**std::path::PathBuf**](std::path::PathBuf.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: image/tiff; application=geotiff

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## landing_page

> models::LandingPage landing_page(data_connector_id, layer_id)
OGC API Landing Page

Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |

### Return type

[**models::LandingPage**](LandingPage.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## tile

> std::path::PathBuf tile(data_connector_id, layer_id, tile_matrix_set_id, tile_matrix, tile_row, tile_col, datetime)
OGC API Tile

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).  ## Sketch  ```text pointOfOrigin (cornerOfOrigin=topLeft)  tileMatrixMinX, tileMatrixMaxY                                               tileMatrixMaxX        |                                                                            |        v                                                                            v        +---------------------------+---------------------------+-----+---------------------------+ ---> tileCol axis        | 0,0                       | 1,0                       | ... | matrixWidth-1,0           |        |                           |                           |     |                           |        +---------------------------+---------------------------+-----+---------------------------+        | 0,1                       | 1,1                       | ... | matrixWidth-1,1           |        |                           |                           |     |                           |        +---------------------------+---------------------------+-----+---------------------------+        | ...                       | ...                       | ... | ...                       |        |                           |                           |     |                           |        +---------------------------+---------------------------+-----+---------------------------+        | 0,                        | 1,                        | ... | matrixWidth-1,            |  v     |   matrixHeight-1          |   matrixHeight-1          |     |   matrixHeight-1          | --+ tileHeight tileMatrixMinY                     |                           |     |                           |   | (in pixels)        +---------------------------+---------------------------+-----+---------------------------+ --+  |                                                                   |<-       tileWidth       ->|  v                                                                   |        (in pixels)        | tileRow axis ```  

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |
**tile_matrix_set_id** | **String** | Tile matrix set identifier | [required] |
**tile_matrix** | **i32** | Tile matrix level | [required] |
**tile_row** | **i64** | Tile row | [required] |
**tile_col** | **i64** | Tile column | [required] |
**datetime** | Option<**String**> | Either a date-time or an interval, half-bounded or bounded. Date and time expressions adhere to RFC 3339. Half-bounded intervals use double dots (`..`). |  |

### Return type

[**std::path::PathBuf**](std::path::PathBuf.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: image/png

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## tile_matrix_set

> models::TileMatrixSet tile_matrix_set(data_connector_id, layer_id, tile_matrix_set_id)
OGC API Tile Matrix Set Definition

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html). Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |
**tile_matrix_set_id** | **String** | Tile matrix set identifier | [required] |

### Return type

[**models::TileMatrixSet**](TileMatrixSet.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## tile_matrix_sets

> models::TileMatrixSets tile_matrix_sets(data_connector_id, layer_id)
OGC API Tile Matrix Set List

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html). Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**data_connector_id** | **uuid::Uuid** | ID of the data connector | [required] |
**layer_id** | **String** | ID of the layer, which is used as collection ID | [required] |

### Return type

[**models::TileMatrixSets**](TileMatrixSets.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

