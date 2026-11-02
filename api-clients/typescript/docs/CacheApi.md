# CacheApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**clearHandler**](CacheApi.md#clearhandler) | **POST** /cache/clear | Clear operator results, STAC query metadata and GDAL worker caches. |



## clearHandler

> clearHandler()

Clear operator results, STAC query metadata and GDAL worker caches.

Requires an administrator. Caching remains enabled, with unchanged capacities and lifetimes. Waits for every GDAL worker to close its dataset and clear its remote file caches. Run without concurrent queries for comparable fresh-access measurements: concurrent work can repopulate caches during clearance. Does not clear Wildlive\&#39;s persistent database cache, operating-system caches, or caches in clients, proxies and upstream services.

### Example

```ts
import {
  Configuration,
  CacheApi,
} from '@geoengine/api-client';
import type { ClearHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new CacheApi(config);

  try {
    const data = await api.clearHandler();
    console.log(data);
  } catch (error) {
    console.error(error);
  }
}

// Run the test
example().catch(console.error);
```

### Parameters

This endpoint does not need any parameter.

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | Caches cleared; all GDAL workers have acknowledged clearance. |  -  |
| **401** | Authorization failed |  -  |
| **500** | Cache clearance failed; some caches may already have been cleared. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

