# ProcessingGraphsApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**datasetFromProcessingGraphHandler**](ProcessingGraphsApi.md#datasetfromprocessinggraphhandler) | **POST** /datasetFromProcessingGraph/{id} | Create a task for creating a new dataset from the result of the processing graph given by its &#x60;id&#x60; and the dataset parameters in the request body. Returns the id of the created task |
| [**getProcessingGraphAllMetadataZipHandler**](ProcessingGraphsApi.md#getprocessinggraphallmetadataziphandler) | **GET** /processingGraphs/{id}/allMetadata/zip | Gets a ZIP archive of the processing graph, its provenance and the output metadata. |
| [**getProcessingGraphMetadataHandler**](ProcessingGraphsApi.md#getprocessinggraphmetadatahandler) | **GET** /processingGraphs/{id}/metadata | Gets the result metadata of a processing graph |
| [**getProcessingGraphProvenanceHandler**](ProcessingGraphsApi.md#getprocessinggraphprovenancehandler) | **GET** /processingGraphs/{id}/provenance | Gets the provenance of all datasets used in a processing graph. |
| [**loadProcessingGraphHandler**](ProcessingGraphsApi.md#loadprocessinggraphhandler) | **GET** /processingGraphs/{id} | Retrieves an existing processing graph. |
| [**rasterStreamWebsocket**](ProcessingGraphsApi.md#rasterstreamwebsocket) | **GET** /processingGraphs/{id}/rasterStream | Query a processing graph raster result as a stream of tiles via a websocket connection. |
| [**registerProcessingGraphHandler**](ProcessingGraphsApi.md#registerprocessinggraphhandler) | **POST** /processingGraphs | Registers a new processing graph. |



## datasetFromProcessingGraphHandler

> TaskResponse datasetFromProcessingGraphHandler(id, rasterDatasetFromProcessingGraph)

Create a task for creating a new dataset from the result of the processing graph given by its &#x60;id&#x60; and the dataset parameters in the request body. Returns the id of the created task

### Example

```ts
import {
  Configuration,
  ProcessingGraphsApi,
} from '@geoengine/api-client';
import type { DatasetFromProcessingGraphHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProcessingGraphsApi(config);

  const body = {
    // string | Processing graph id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // RasterDatasetFromProcessingGraph
    rasterDatasetFromProcessingGraph: ...,
  } satisfies DatasetFromProcessingGraphHandlerRequest;

  try {
    const data = await api.datasetFromProcessingGraphHandler(body);
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
| **id** | `string` | Processing graph id | [Defaults to `undefined`] |
| **rasterDatasetFromProcessingGraph** | [RasterDatasetFromProcessingGraph](RasterDatasetFromProcessingGraph.md) |  | |

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


## getProcessingGraphAllMetadataZipHandler

> Blob getProcessingGraphAllMetadataZipHandler(id)

Gets a ZIP archive of the processing graph, its provenance and the output metadata.

### Example

```ts
import {
  Configuration,
  ProcessingGraphsApi,
} from '@geoengine/api-client';
import type { GetProcessingGraphAllMetadataZipHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProcessingGraphsApi(config);

  const body = {
    // string | Processing graph id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies GetProcessingGraphAllMetadataZipHandlerRequest;

  try {
    const data = await api.getProcessingGraphAllMetadataZipHandler(body);
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
| **id** | `string` | Processing graph id | [Defaults to `undefined`] |

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


## getProcessingGraphMetadataHandler

> TypedResultDescriptor getProcessingGraphMetadataHandler(id)

Gets the result metadata of a processing graph

### Example

```ts
import {
  Configuration,
  ProcessingGraphsApi,
} from '@geoengine/api-client';
import type { GetProcessingGraphMetadataHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProcessingGraphsApi(config);

  const body = {
    // string | Processing graph id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies GetProcessingGraphMetadataHandlerRequest;

  try {
    const data = await api.getProcessingGraphMetadataHandler(body);
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
| **id** | `string` | Processing graph id | [Defaults to `undefined`] |

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
| **200** | Result metadata of the processing graph |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getProcessingGraphProvenanceHandler

> Array&lt;ProvenanceEntry&gt; getProcessingGraphProvenanceHandler(id)

Gets the provenance of all datasets used in a processing graph.

### Example

```ts
import {
  Configuration,
  ProcessingGraphsApi,
} from '@geoengine/api-client';
import type { GetProcessingGraphProvenanceHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProcessingGraphsApi(config);

  const body = {
    // string | Processing graph id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies GetProcessingGraphProvenanceHandlerRequest;

  try {
    const data = await api.getProcessingGraphProvenanceHandler(body);
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
| **id** | `string` | Processing graph id | [Defaults to `undefined`] |

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


## loadProcessingGraphHandler

> ProcessingGraph loadProcessingGraphHandler(id)

Retrieves an existing processing graph.

### Example

```ts
import {
  Configuration,
  ProcessingGraphsApi,
} from '@geoengine/api-client';
import type { LoadProcessingGraphHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProcessingGraphsApi(config);

  const body = {
    // string | Processing graph id
    id: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies LoadProcessingGraphHandlerRequest;

  try {
    const data = await api.loadProcessingGraphHandler(body);
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
| **id** | `string` | Processing graph id | [Defaults to `undefined`] |

### Return type

[**ProcessingGraph**](ProcessingGraph.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Processing graph loaded from database |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## rasterStreamWebsocket

> rasterStreamWebsocket(id, spatialBounds, timeInterval, attributes, resultType)

Query a processing graph raster result as a stream of tiles via a websocket connection.

### Example

```ts
import {
  Configuration,
  ProcessingGraphsApi,
} from '@geoengine/api-client';
import type { RasterStreamWebsocketRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProcessingGraphsApi(config);

  const body = {
    // string | Processing graph id
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
| **id** | `string` | Processing graph id | [Defaults to `undefined`] |
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


## registerProcessingGraphHandler

> IdResponse registerProcessingGraphHandler(processingGraph)

Registers a new processing graph.

### Example

```ts
import {
  Configuration,
  ProcessingGraphsApi,
} from '@geoengine/api-client';
import type { RegisterProcessingGraphHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new ProcessingGraphsApi(config);

  const body = {
    // ProcessingGraph
    processingGraph: {"type":"Vector","operator":{"type":"MockPointSource","params":{"points":[{"x":0.0,"y":0.1},{"x":1.0,"y":1.1}]}}},
  } satisfies RegisterProcessingGraphHandlerRequest;

  try {
    const data = await api.registerProcessingGraphHandler(body);
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
| **processingGraph** | [ProcessingGraph](ProcessingGraph.md) |  | |

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

