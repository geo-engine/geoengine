# \CacheApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**clear_handler**](CacheApi.md#clear_handler) | **POST** /cache/clear | Clear operator results, STAC query metadata and GDAL worker caches.



## clear_handler

> clear_handler()
Clear operator results, STAC query metadata and GDAL worker caches.

Requires an administrator. Caching remains enabled, with unchanged capacities and lifetimes. Waits for every GDAL worker to close its dataset and clear its remote file caches. Run without concurrent queries for comparable fresh-access measurements: concurrent work can repopulate caches during clearance. Does not clear Wildlive's persistent database cache, operating-system caches, or caches in clients, proxies and upstream services.

### Parameters

This endpoint does not need any parameter.

### Return type

 (empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

