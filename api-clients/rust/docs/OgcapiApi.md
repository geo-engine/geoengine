# \OgcapiApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**collection**](OgcapiApi.md#collection) | **GET** /ogc/ogc/{processingGraphId}/collections/{processingGraphId} | OGC API Collection Metadata
[**collection_tileset**](OgcapiApi.md#collection_tileset) | **GET** /ogc/ogc/{processingGraphId}/collections/{collectionId}/tiles/{tileMatrixSetId} | OGC API Collection Tileset Metadata
[**collection_tilesets**](OgcapiApi.md#collection_tilesets) | **GET** /ogc/ogc/{processingGraphId}/collections/{collectionId}/tiles | OGC API Collection Tilesets List
[**collections**](OgcapiApi.md#collections) | **GET** /ogc/ogc/{processingGraphId}/collections | OGC API Collections List
[**conformance**](OgcapiApi.md#conformance) | **GET** /ogc/ogc/{processingGraphId}/conformance | OGC API Conformance Classes
[**landing_page**](OgcapiApi.md#landing_page) | **GET** /ogc/ogc/{processingGraphId}/ | OGC API Landing Page
[**tile**](OgcapiApi.md#tile) | **GET** /ogc/ogc/{processingGraphId}/collections/{collectionId}/tiles/{tileMatrixSetId}/{tileMatrix}/{tileRow}/{tileCol} | OGC API Tile
[**tile_matrix_set**](OgcapiApi.md#tile_matrix_set) | **GET** /ogc/ogc/{processingGraphId}/tileMatrixSets/{tileMatrixSetId} | OGC API Tile Matrix Set Definition
[**tile_matrix_sets**](OgcapiApi.md#tile_matrix_sets) | **GET** /ogc/ogc/{processingGraphId}/tileMatrixSets | OGC API Tile Matrix Set List



## collection

> models::Collection collection(processing_graph_id)
OGC API Collection Metadata

Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |

### Return type

[**models::Collection**](Collection.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## collection_tileset

> models::TileSetMetadataResponse collection_tileset(processing_graph_id, collection_id, tile_matrix_set_id)
OGC API Collection Tileset Metadata

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |
**collection_id** | **uuid::Uuid** | Collection identifier | [required] |
**tile_matrix_set_id** | **String** | Tile matrix set identifier | [required] |

### Return type

[**models::TileSetMetadataResponse**](TileSetMetadataResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## collection_tilesets

> models::TileSetsResponse collection_tilesets(processing_graph_id, collection_id)
OGC API Collection Tilesets List

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |
**collection_id** | **uuid::Uuid** | Collection identifier | [required] |

### Return type

[**models::TileSetsResponse**](TileSetsResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## collections

> models::Collections collections(processing_graph_id, datetime, bbox, limit, f)
OGC API Collections List

Cf. [OGC API - Common - Part 2: Collections](https://docs.ogc.org/DRAFTS/20-024.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |
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

> models::Conformance conformance(processing_graph_id)
OGC API Conformance Classes

Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |

### Return type

[**models::Conformance**](Conformance.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## landing_page

> models::LandingPage landing_page(processing_graph_id)
OGC API Landing Page

Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |

### Return type

[**models::LandingPage**](LandingPage.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## tile

> std::path::PathBuf tile(processing_graph_id, collection_id, tile_matrix_set_id, tile_matrix, tile_row, tile_col, datetime)
OGC API Tile

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |
**collection_id** | **uuid::Uuid** | Collection identifier | [required] |
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

> models::TileMatrixSet tile_matrix_set(processing_graph_id, tile_matrix_set_id)
OGC API Tile Matrix Set Definition

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html). Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |
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

> models::TileMatrixSets tile_matrix_sets(processing_graph_id)
OGC API Tile Matrix Set List

Cf. [OGC API - Tiles - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html). Cf. [OGC Two Dimensional Tile Matrix Set and Tile Set Metadata](https://docs.ogc.org/is/17-083r4/17-083r4.html).

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph_id** | **uuid::Uuid** | ID of the processing graph, which is used as collection ID | [required] |

### Return type

[**models::TileMatrixSets**](TileMatrixSets.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

