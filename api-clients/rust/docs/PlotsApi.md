# \PlotsApi

All URIs are relative to *https://geoengine.io/api*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_plot_handler**](PlotsApi.md#get_plot_handler) | **GET** /plot/{id} | Generates a plot.



## get_plot_handler

> models::WrappedPlotOutput get_plot_handler(bbox, time, spatial_resolution, id, crs)
Generates a plot.

# Example  1. Upload the file `plain_data.csv` with the following content:  ```csv a 1 2 ``` 2. Create a dataset from it using the \"Plain Data\" example at `/dataset`. 3. Create a statistics workflow using the \"Statistics Plot\" example at `/workflow`. 4. Generate the plot with this handler.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**bbox** | **String** |  | [required] |
**time** | **String** |  | [required] |
**spatial_resolution** | **String** |  | [required] |
**id** | **uuid::Uuid** | Workflow id | [required] |
**crs** | Option<**String**> |  |  |

### Return type

[**models::WrappedPlotOutput**](WrappedPlotOutput.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

