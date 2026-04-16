# \WorkflowsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**dataset_from_workflow_handler**](WorkflowsApi.md#dataset_from_workflow_handler) | **POST** /datasetFromWorkflow/{id} | Create a task for creating a new dataset from the result of the workflow given by its `id` and the dataset parameters in the request body. Returns the id of the created task
[**get_workflow_all_metadata_zip_handler**](WorkflowsApi.md#get_workflow_all_metadata_zip_handler) | **GET** /workflow/{id}/allMetadata/zip | Gets a ZIP archive of the worklow, its provenance and the output metadata.
[**get_workflow_metadata_handler**](WorkflowsApi.md#get_workflow_metadata_handler) | **GET** /workflow/{id}/metadata | Gets the metadata of a workflow
[**get_workflow_provenance_handler**](WorkflowsApi.md#get_workflow_provenance_handler) | **GET** /workflow/{id}/provenance | Gets the provenance of all datasets used in a workflow.
[**load_workflow_handler**](WorkflowsApi.md#load_workflow_handler) | **GET** /workflow/{id} | Retrieves an existing Workflow.
[**raster_stream_websocket**](WorkflowsApi.md#raster_stream_websocket) | **GET** /workflow/{id}/rasterStream | Query a workflow raster result as a stream of tiles via a websocket connection.
[**register_workflow_handler**](WorkflowsApi.md#register_workflow_handler) | **POST** /workflow | Registers a new Workflow.



## dataset_from_workflow_handler

> models::TaskResponse dataset_from_workflow_handler(id, raster_dataset_from_workflow)
Create a task for creating a new dataset from the result of the workflow given by its `id` and the dataset parameters in the request body. Returns the id of the created task

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Workflow id | [required] |
**raster_dataset_from_workflow** | [**RasterDatasetFromWorkflow**](RasterDatasetFromWorkflow.md) |  | [required] |

### Return type

[**models::TaskResponse**](TaskResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_workflow_all_metadata_zip_handler

> std::path::PathBuf get_workflow_all_metadata_zip_handler(id)
Gets a ZIP archive of the worklow, its provenance and the output metadata.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Workflow id | [required] |

### Return type

[**std::path::PathBuf**](std::path::PathBuf.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/zip

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_workflow_metadata_handler

> models::TypedResultDescriptor get_workflow_metadata_handler(id)
Gets the metadata of a workflow

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Workflow id | [required] |

### Return type

[**models::TypedResultDescriptor**](TypedResultDescriptor.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_workflow_provenance_handler

> Vec<models::ProvenanceEntry> get_workflow_provenance_handler(id)
Gets the provenance of all datasets used in a workflow.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Workflow id | [required] |

### Return type

[**Vec<models::ProvenanceEntry>**](ProvenanceEntry.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## load_workflow_handler

> models::Workflow load_workflow_handler(id)
Retrieves an existing Workflow.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Workflow id | [required] |

### Return type

[**models::Workflow**](Workflow.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## raster_stream_websocket

> raster_stream_websocket(id, spatial_bounds, time_interval, attributes, result_type)
Query a workflow raster result as a stream of tiles via a websocket connection.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Workflow id | [required] |
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


## register_workflow_handler

> models::IdResponse register_workflow_handler(workflow)
Registers a new Workflow.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**workflow** | [**Workflow**](Workflow.md) |  | [required] |

### Return type

[**models::IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

