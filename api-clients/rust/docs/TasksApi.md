# \TasksApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**abort_handler**](TasksApi.md#abort_handler) | **DELETE** /tasks/{id} | Abort a running task.
[**list_handler**](TasksApi.md#list_handler) | **GET** /tasks/list | Retrieve the status of all tasks.
[**status_handler**](TasksApi.md#status_handler) | **GET** /tasks/{id}/status | Retrieve the status of a task.



## abort_handler

> abort_handler(id, force)
Abort a running task.

# Parameters  * `force` - If true, the task will be aborted without clean-up.             You can abort a task that is already in the process of aborting.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Task id | [required] |
**force** | Option<**bool**> |  |  |

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_handler

> Vec<models::TaskStatusWithId> list_handler(filter, offset, limit)
Retrieve the status of all tasks.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**filter** | Option<**String**> |  | [required] |
**offset** | **i32** |  | [required] |
**limit** | **i32** |  | [required] |

### Return type

[**Vec<models::TaskStatusWithId>**](TaskStatusWithId.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## status_handler

> models::TaskStatus status_handler(id)
Retrieve the status of a task.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Task id | [required] |

### Return type

[**models::TaskStatus**](TaskStatus.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

