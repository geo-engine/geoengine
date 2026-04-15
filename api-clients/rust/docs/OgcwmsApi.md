# \OgcwmsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**wms_handler**](OgcwmsApi.md#wms_handler) | **GET** /wms/{workflow} | OGC WMS endpoint



## wms_handler

> std::path::PathBuf wms_handler(workflow, request, bbox, bgcolor, crs, elevation, exceptions, format, height, info_format, layer, layers, query_layers, service, sld, sld_body, styles, time, transparent, version, width)
OGC WMS endpoint

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**workflow** | **uuid::Uuid** | Workflow id | [required] |
**request** | [**WmsRequest**](WmsRequest.md) | type of WMS request | [required] |
**bbox** | Option<**String**> |  |  |
**bgcolor** | Option<**String**> |  |  |
**crs** | Option<**String**> |  |  |
**elevation** | Option<**String**> |  |  |
**exceptions** | Option<**String**> |  |  |
**format** | Option<**String**> |  |  |
**height** | Option<**i32**> |  |  |
**info_format** | Option<**String**> |  |  |
**layer** | Option<**String**> |  |  |
**layers** | Option<**String**> |  |  |
**query_layers** | Option<**String**> |  |  |
**service** | Option<[**WmsService**](WmsService.md)> |  |  |
**sld** | Option<**String**> |  |  |
**sld_body** | Option<**String**> |  |  |
**styles** | Option<**String**> |  |  |
**time** | Option<**String**> |  |  |
**transparent** | Option<**bool**> |  |  |
**version** | Option<**String**> |  |  |
**width** | Option<**i32**> |  |  |

### Return type

[**std::path::PathBuf**](std::path::PathBuf.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: image/png

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

