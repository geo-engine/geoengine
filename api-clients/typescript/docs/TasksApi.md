# TasksApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**abortHandler**](TasksApi.md#aborthandler) | **DELETE** /tasks/{id} | Abort a running task. |
| [**listHandler**](TasksApi.md#listhandler) | **GET** /tasks/list | Retrieve the status of all tasks. |
| [**statusHandler**](TasksApi.md#statushandler) | **GET** /tasks/{id}/status | Retrieve the status of a task. |



## abortHandler

> abortHandler(id, force)

Abort a running task.

# Parameters  * &#x60;force&#x60; - If true, the task will be aborted without clean-up.             You can abort a task that is already in the process of aborting.

### Example

```ts
import {
  Configuration,
  TasksApi,
} from '@geoengine/api-client';
import type { AbortHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new TasksApi(config);

  const body = {
    // string | Task id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // boolean (optional)
    force: true,
  } satisfies AbortHandlerRequest;

  try {
    const data = await api.abortHandler(body);
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
| **id** | `string` | Task id | [Defaults to `undefined`] |
| **force** | `boolean` |  | [Optional] [Defaults to `undefined`] |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **202** | Task will be aborted. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## listHandler

> Array&lt;TaskStatusWithId&gt; listHandler(filter, offset, limit)

Retrieve the status of all tasks.

### Example

```ts
import {
  Configuration,
  TasksApi,
} from '@geoengine/api-client';
import type { ListHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new TasksApi(config);

  const body = {
    // TaskFilter
    filter: ...,
    // number
    offset: 0,
    // number
    limit: 20,
  } satisfies ListHandlerRequest;

  try {
    const data = await api.listHandler(body);
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
| **filter** | `TaskFilter` |  | [Defaults to `undefined`] [Enum: running, aborted, failed, completed] |
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |

### Return type

[**Array&lt;TaskStatusWithId&gt;**](TaskStatusWithId.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Status of all tasks |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## statusHandler

> TaskStatus statusHandler(id)

Retrieve the status of a task.

### Example

```ts
import {
  Configuration,
  TasksApi,
} from '@geoengine/api-client';
import type { StatusHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new TasksApi(config);

  const body = {
    // string | Task id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies StatusHandlerRequest;

  try {
    const data = await api.statusHandler(body);
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
| **id** | `string` | Task id | [Defaults to `undefined`] |

### Return type

[**TaskStatus**](TaskStatus.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Status of the task (running) |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

