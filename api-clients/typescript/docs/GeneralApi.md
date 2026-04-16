# GeneralApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**availableHandler**](GeneralApi.md#availablehandler) | **GET** /available | Server availablity check. |
| [**serverInfoHandler**](GeneralApi.md#serverinfohandler) | **GET** /info | Shows information about the server software version. |



## availableHandler

> availableHandler()

Server availablity check.

### Example

```ts
import {
  Configuration,
  GeneralApi,
} from '@geoengine/api-client';
import type { AvailableHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const api = new GeneralApi();

  try {
    const data = await api.availableHandler();
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

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | Server availablity check |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## serverInfoHandler

> ServerInfo serverInfoHandler()

Shows information about the server software version.

### Example

```ts
import {
  Configuration,
  GeneralApi,
} from '@geoengine/api-client';
import type { ServerInfoHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const api = new GeneralApi();

  try {
    const data = await api.serverInfoHandler();
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

[**ServerInfo**](ServerInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Server software information |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

