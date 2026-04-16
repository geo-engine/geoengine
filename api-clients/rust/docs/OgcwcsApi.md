# \OgcwcsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**wcs_handler**](OgcwcsApi.md#wcs_handler) | **GET** /wcs/{workflow} | OGC WCS endpoint



## wcs_handler

> String wcs_handler(workflow, request, boundingbox, format, gridbasecrs, gridoffsets, gridorigin, identifier, identifiers, nodatavalue, resx, resy, service, time, version)
OGC WCS endpoint

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**workflow** | **uuid::Uuid** | Workflow id | [required] |
**request** | [**WcsRequest**](WcsRequest.md) | type of WCS request | [required] |
**boundingbox** | Option<**String**> |  |  |
**format** | Option<[**GetCoverageFormat**](GetCoverageFormat.md)> |  |  |
**gridbasecrs** | Option<**String**> |  |  |
**gridoffsets** | Option<**String**> |  |  |
**gridorigin** | Option<**String**> |  |  |
**identifier** | Option<**String**> |  |  |
**identifiers** | Option<**String**> |  |  |
**nodatavalue** | Option<**f64**> |  |  |
**resx** | Option<**f64**> |  |  |
**resy** | Option<**f64**> |  |  |
**service** | Option<[**WcsService**](WcsService.md)> |  |  |
**time** | Option<**String**> |  |  |
**version** | Option<**String**> |  |  |

### Return type

**String**

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: text/xml

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

