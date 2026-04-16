# WorkflowsApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**datasetFromWorkflowHandler**](WorkflowsApi.md#datasetfromworkflowhandler) | **POST** /datasetFromWorkflow/{id} | Create a task for creating a new dataset from the result of the workflow given by its &#x60;id&#x60; and the dataset parameters in the request body. Returns the id of the created task |
| [**getWorkflowAllMetadataZipHandler**](WorkflowsApi.md#getworkflowallmetadataziphandler) | **GET** /workflow/{id}/allMetadata/zip | Gets a ZIP archive of the worklow, its provenance and the output metadata. |
| [**getWorkflowMetadataHandler**](WorkflowsApi.md#getworkflowmetadatahandler) | **GET** /workflow/{id}/metadata | Gets the metadata of a workflow |
| [**getWorkflowProvenanceHandler**](WorkflowsApi.md#getworkflowprovenancehandler) | **GET** /workflow/{id}/provenance | Gets the provenance of all datasets used in a workflow. |
| [**loadWorkflowHandler**](WorkflowsApi.md#loadworkflowhandler) | **GET** /workflow/{id} | Retrieves an existing Workflow. |
| [**rasterStreamWebsocket**](WorkflowsApi.md#rasterstreamwebsocket) | **GET** /workflow/{id}/rasterStream | Query a workflow raster result as a stream of tiles via a websocket connection. |
| [**registerWorkflowHandler**](WorkflowsApi.md#registerworkflowhandler) | **POST** /workflow | Registers a new Workflow. |



## datasetFromWorkflowHandler

> TaskResponse datasetFromWorkflowHandler(id, rasterDatasetFromWorkflow)

Create a task for creating a new dataset from the result of the workflow given by its &#x60;id&#x60; and the dataset parameters in the request body. Returns the id of the created task

### Example

```ts
import {
  Configuration,
  WorkflowsApi,
} from '@geoengine/api-client';
import type { DatasetFromWorkflowHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new WorkflowsApi(config);

  const body = {
    // string | Workflow id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // RasterDatasetFromWorkflow
    rasterDatasetFromWorkflow: ...,
  } satisfies DatasetFromWorkflowHandlerRequest;

  try {
    const data = await api.datasetFromWorkflowHandler(body);
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
| **id** | `string` | Workflow id | [Defaults to `undefined`] |
| **rasterDatasetFromWorkflow** | [RasterDatasetFromWorkflow](RasterDatasetFromWorkflow.md) |  | |

### Return type

[**TaskResponse**](TaskResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Id of created task |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getWorkflowAllMetadataZipHandler

> Blob getWorkflowAllMetadataZipHandler(id)

Gets a ZIP archive of the worklow, its provenance and the output metadata.

### Example

```ts
import {
  Configuration,
  WorkflowsApi,
} from '@geoengine/api-client';
import type { GetWorkflowAllMetadataZipHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new WorkflowsApi(config);

  const body = {
    // string | Workflow id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies GetWorkflowAllMetadataZipHandlerRequest;

  try {
    const data = await api.getWorkflowAllMetadataZipHandler(body);
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
| **id** | `string` | Workflow id | [Defaults to `undefined`] |

### Return type

**Blob**

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/zip`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | ZIP Archive |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getWorkflowMetadataHandler

> TypedResultDescriptor getWorkflowMetadataHandler(id)

Gets the metadata of a workflow

### Example

```ts
import {
  Configuration,
  WorkflowsApi,
} from '@geoengine/api-client';
import type { GetWorkflowMetadataHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new WorkflowsApi(config);

  const body = {
    // string | Workflow id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies GetWorkflowMetadataHandlerRequest;

  try {
    const data = await api.getWorkflowMetadataHandler(body);
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
| **id** | `string` | Workflow id | [Defaults to `undefined`] |

### Return type

[**TypedResultDescriptor**](TypedResultDescriptor.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Metadata of loaded workflow |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getWorkflowProvenanceHandler

> Array&lt;ProvenanceEntry&gt; getWorkflowProvenanceHandler(id)

Gets the provenance of all datasets used in a workflow.

### Example

```ts
import {
  Configuration,
  WorkflowsApi,
} from '@geoengine/api-client';
import type { GetWorkflowProvenanceHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new WorkflowsApi(config);

  const body = {
    // string | Workflow id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies GetWorkflowProvenanceHandlerRequest;

  try {
    const data = await api.getWorkflowProvenanceHandler(body);
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
| **id** | `string` | Workflow id | [Defaults to `undefined`] |

### Return type

[**Array&lt;ProvenanceEntry&gt;**](ProvenanceEntry.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Provenance of used datasets |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## loadWorkflowHandler

> Workflow loadWorkflowHandler(id)

Retrieves an existing Workflow.

### Example

```ts
import {
  Configuration,
  WorkflowsApi,
} from '@geoengine/api-client';
import type { LoadWorkflowHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new WorkflowsApi(config);

  const body = {
    // string | Workflow id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies LoadWorkflowHandlerRequest;

  try {
    const data = await api.loadWorkflowHandler(body);
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
| **id** | `string` | Workflow id | [Defaults to `undefined`] |

### Return type

[**Workflow**](Workflow.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Workflow loaded from database |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## rasterStreamWebsocket

> rasterStreamWebsocket(id, spatialBounds, timeInterval, attributes, resultType)

Query a workflow raster result as a stream of tiles via a websocket connection.

### Example

```ts
import {
  Configuration,
  WorkflowsApi,
} from '@geoengine/api-client';
import type { RasterStreamWebsocketRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new WorkflowsApi(config);

  const body = {
    // string | Workflow id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // SpatialPartition2D
    spatialBounds: ...,
    // string
    timeInterval: timeInterval_example,
    // string
    attributes: attributes_example,
    // RasterStreamWebsocketResultType
    resultType: ...,
  } satisfies RasterStreamWebsocketRequest;

  try {
    const data = await api.rasterStreamWebsocket(body);
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
| **id** | `string` | Workflow id | [Defaults to `undefined`] |
| **spatialBounds** | [](.md) |  | [Defaults to `undefined`] |
| **timeInterval** | `string` |  | [Defaults to `undefined`] |
| **attributes** | `string` |  | [Defaults to `undefined`] |
| **resultType** | `RasterStreamWebsocketResultType` |  | [Defaults to `undefined`] [Enum: arrow] |

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
| **101** | Upgrade to websocket connection |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## registerWorkflowHandler

> IdResponse registerWorkflowHandler(workflow)

Registers a new Workflow.

### Example

```ts
import {
  Configuration,
  WorkflowsApi,
} from '@geoengine/api-client';
import type { RegisterWorkflowHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new WorkflowsApi(config);

  const body = {
    // Workflow
    workflow: {"type":"Vector","operator":{"type":"MockPointSource","params":{"points":[{"x":0.0,"y":0.1},{"x":1.0,"y":1.1}]}}},
  } satisfies RegisterWorkflowHandlerRequest;

  try {
    const data = await api.registerWorkflowHandler(body);
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
| **workflow** | [Workflow](Workflow.md) |  | |

### Return type

[**IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Id of generated resource |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

