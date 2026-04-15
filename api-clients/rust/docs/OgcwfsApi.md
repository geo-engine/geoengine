# \OgcwfsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**wfs_handler**](OgcwfsApi.md#wfs_handler) | **GET** /wfs/{workflow} | OGC WFS endpoint



## wfs_handler

> models::GeoJson wfs_handler(workflow, request, bbox, count, filter, namespaces, property_name, result_type, service, sort_by, srs_name, time, type_names, version)
OGC WFS endpoint

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**workflow** | **uuid::Uuid** | Workflow id | [required] |
**request** | [**WfsRequest**](WfsRequest.md) | type of WFS request | [required] |
**bbox** | Option<**String**> |  |  |
**count** | Option<**i64**> |  |  |
**filter** | Option<**String**> |  |  |
**namespaces** | Option<**String**> |  |  |
**property_name** | Option<**String**> |  |  |
**result_type** | Option<**String**> |  |  |
**service** | Option<[**WfsService**](WfsService.md)> |  |  |
**sort_by** | Option<**String**> |  |  |
**srs_name** | Option<**String**> |  |  |
**time** | Option<**String**> |  |  |
**type_names** | Option<**String**> |  |  |
**version** | Option<**String**> |  |  |

### Return type

[**models::GeoJson**](GeoJson.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

