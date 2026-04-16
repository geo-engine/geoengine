# PlotsApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getPlotHandler**](PlotsApi.md#getplothandler) | **GET** /plot/{id} | Generates a plot. |



## getPlotHandler

> WrappedPlotOutput getPlotHandler(bbox, time, spatialResolution, id, crs)

Generates a plot.

# Example  1. Upload the file &#x60;plain_data.csv&#x60; with the following content:  &#x60;&#x60;&#x60;csv a 1 2 &#x60;&#x60;&#x60; 2. Create a dataset from it using the \&quot;Plain Data\&quot; example at &#x60;/dataset&#x60;. 3. Create a statistics workflow using the \&quot;Statistics Plot\&quot; example at &#x60;/workflow&#x60;. 4. Generate the plot with this handler.

### Example

```ts
import {
  Configuration,
  PlotsApi,
} from '@geoengine/api-client';
import type { GetPlotHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new PlotsApi(config);

  const body = {
    // string
    bbox: 0,-0.3,0.2,0,
    // string
    time: 2020-01-01T00:00:00.0Z,
    // string
    spatialResolution: 0.1,0.1,
    // string | Workflow id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string (optional)
    crs: EPSG:4326,
  } satisfies GetPlotHandlerRequest;

  try {
    const data = await api.getPlotHandler(body);
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **bbox** | `string` |  | [Defaults to `undefined`] |
| **time** | `string` |  | [Defaults to `undefined`] |
| **spatialResolution** | `string` |  | [Defaults to `undefined`] |
| **id** | `string` | Workflow id | [Defaults to `undefined`] |
| **crs** | `string` |  | [Optional] [Defaults to `undefined`] |

### Return type

[**WrappedPlotOutput**](WrappedPlotOutput.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

