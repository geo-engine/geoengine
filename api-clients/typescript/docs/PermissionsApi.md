# PermissionsApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addPermissionHandler**](PermissionsApi.md#addpermissionhandler) | **PUT** /permissions | Adds a new permission. |
| [**getResourcePermissionsHandler**](PermissionsApi.md#getresourcepermissionshandler) | **GET** /permissions/resources/{resource_type}/{resource_id} | Lists permission for a given resource. |
| [**removePermissionHandler**](PermissionsApi.md#removepermissionhandler) | **DELETE** /permissions | Removes an existing permission. |



## addPermissionHandler

> addPermissionHandler(permissionRequest)

Adds a new permission.

### Example

```ts
import {
  Configuration,
  PermissionsApi,
} from '@geoengine/api-client';
import type { AddPermissionHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new PermissionsApi(config);

  const body = {
    // PermissionRequest
    permissionRequest: {"resource":{"type":"layer","id":"00000000-0000-0000-0000-000000000000"},"roleId":"00000000-0000-0000-0000-000000000000","permission":"Read"},
  } satisfies AddPermissionHandlerRequest;

  try {
    const data = await api.addPermissionHandler(body);
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
| **permissionRequest** | [PermissionRequest](PermissionRequest.md) |  | |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getResourcePermissionsHandler

> Array&lt;PermissionListing&gt; getResourcePermissionsHandler(resourceType, resourceId, limit, offset)

Lists permission for a given resource.

### Example

```ts
import {
  Configuration,
  PermissionsApi,
} from '@geoengine/api-client';
import type { GetResourcePermissionsHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new PermissionsApi(config);

  const body = {
    // string | Resource Type
    resourceType: resourceType_example,
    // string | Resource Id
    resourceId: resourceId_example,
    // number
    limit: 56,
    // number
    offset: 56,
  } satisfies GetResourcePermissionsHandlerRequest;

  try {
    const data = await api.getResourcePermissionsHandler(body);
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
| **resourceType** | `string` | Resource Type | [Defaults to `undefined`] |
| **resourceId** | `string` | Resource Id | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |
| **offset** | `number` |  | [Defaults to `undefined`] |

### Return type

[**Array&lt;PermissionListing&gt;**](PermissionListing.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of permission |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## removePermissionHandler

> removePermissionHandler(permissionRequest)

Removes an existing permission.

### Example

```ts
import {
  Configuration,
  PermissionsApi,
} from '@geoengine/api-client';
import type { RemovePermissionHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new PermissionsApi(config);

  const body = {
    // PermissionRequest
    permissionRequest: {resource={type=layer, id=00000000-0000-0000-0000-000000000000}, roleId=00000000-0000-0000-0000-000000000000, permission=Read},
  } satisfies RemovePermissionHandlerRequest;

  try {
    const data = await api.removePermissionHandler(body);
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
| **permissionRequest** | [PermissionRequest](PermissionRequest.md) |  | |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

