# SpatialReferencesApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getSpatialReferenceSpecificationHandler**](SpatialReferencesApi.md#getspatialreferencespecificationhandler) | **GET** /spatialReferenceSpecification/{srsString} |  |



## getSpatialReferenceSpecificationHandler

> SpatialReferenceSpecification getSpatialReferenceSpecificationHandler(srsString)



### Example

```ts
import {
  Configuration,
  SpatialReferencesApi,
} from '@geoengine/api-client';
import type { GetSpatialReferenceSpecificationHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new SpatialReferencesApi(config);

  const body = {
    // string
    srsString: EPSG:4326,
  } satisfies GetSpatialReferenceSpecificationHandlerRequest;

  try {
    const data = await api.getSpatialReferenceSpecificationHandler(body);
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
| **srsString** | `string` |  | [Defaults to `undefined`] |

### Return type

[**SpatialReferenceSpecification**](SpatialReferenceSpecification.md)

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

