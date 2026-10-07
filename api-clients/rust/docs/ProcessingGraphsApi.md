# \ProcessingGraphsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**dataset_from_processing_graph_handler**](ProcessingGraphsApi.md#dataset_from_processing_graph_handler) | **POST** /datasetFromProcessingGraph/{id} | Create a task for creating a new dataset from the result of the processing graph given by its `id` and the dataset parameters in the request body. Returns the id of the created task
[**get_processing_graph_all_metadata_zip_handler**](ProcessingGraphsApi.md#get_processing_graph_all_metadata_zip_handler) | **GET** /processingGraphs/{id}/allMetadata/zip | Gets a ZIP archive of the processing graph, its provenance and the output metadata.
[**get_processing_graph_metadata_handler**](ProcessingGraphsApi.md#get_processing_graph_metadata_handler) | **GET** /processingGraphs/{id}/metadata | Gets the result metadata of a processing graph
[**get_processing_graph_provenance_handler**](ProcessingGraphsApi.md#get_processing_graph_provenance_handler) | **GET** /processingGraphs/{id}/provenance | Gets the provenance of all datasets used in a processing graph.
[**load_processing_graph_handler**](ProcessingGraphsApi.md#load_processing_graph_handler) | **GET** /processingGraphs/{id} | Retrieves an existing processing graph.
[**raster_stream_websocket**](ProcessingGraphsApi.md#raster_stream_websocket) | **GET** /processingGraphs/{id}/rasterStream | Query a processing graph raster result as a stream of tiles via a websocket connection.
[**register_processing_graph_handler**](ProcessingGraphsApi.md#register_processing_graph_handler) | **POST** /processingGraphs | Registers a new processing graph.



## dataset_from_processing_graph_handler

> models::TaskResponse dataset_from_processing_graph_handler(id, raster_dataset_from_processing_graph)
Create a task for creating a new dataset from the result of the processing graph given by its `id` and the dataset parameters in the request body. Returns the id of the created task

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Processing graph id | [required] |
**raster_dataset_from_processing_graph** | [**RasterDatasetFromProcessingGraph**](RasterDatasetFromProcessingGraph.md) |  | [required] |

### Return type

[**models::TaskResponse**](TaskResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_processing_graph_all_metadata_zip_handler

> std::path::PathBuf get_processing_graph_all_metadata_zip_handler(id)
Gets a ZIP archive of the processing graph, its provenance and the output metadata.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Processing graph id | [required] |

### Return type

[**std::path::PathBuf**](std::path::PathBuf.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/zip

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_processing_graph_metadata_handler

> models::TypedResultDescriptor get_processing_graph_metadata_handler(id)
Gets the result metadata of a processing graph

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Processing graph id | [required] |

### Return type

[**models::TypedResultDescriptor**](TypedResultDescriptor.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_processing_graph_provenance_handler

> Vec<models::ProvenanceEntry> get_processing_graph_provenance_handler(id)
Gets the provenance of all datasets used in a processing graph.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Processing graph id | [required] |

### Return type

[**Vec<models::ProvenanceEntry>**](ProvenanceEntry.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## load_processing_graph_handler

> models::ProcessingGraph load_processing_graph_handler(id)
Retrieves an existing processing graph.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Processing graph id | [required] |

### Return type

[**models::ProcessingGraph**](ProcessingGraph.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## raster_stream_websocket

> raster_stream_websocket(id, spatial_bounds, time_interval, attributes, result_type)
Query a processing graph raster result as a stream of tiles via a websocket connection.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Processing graph id | [required] |
**spatial_bounds** | [**SpatialPartition2D**](SpatialPartition2D.md) |  | [required] |
**time_interval** | **String** |  | [required] |
**attributes** | **String** |  | [required] |
**result_type** | [**RasterStreamWebsocketResultType**](RasterStreamWebsocketResultType.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## register_processing_graph_handler

> models::IdResponse register_processing_graph_handler(processing_graph)
Registers a new processing graph.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**processing_graph** | [**ProcessingGraph**](ProcessingGraph.md) |  | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

