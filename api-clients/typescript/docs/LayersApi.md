# LayersApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addCollection**](LayersApi.md#addcollection) | **POST** /layerDb/collections/{collection}/collections | Add a new collection to an existing collection |
| [**addExistingCollectionToCollection**](LayersApi.md#addexistingcollectiontocollection) | **POST** /layerDb/collections/{parent}/collections/{collection} | Add an existing collection to a collection |
| [**addExistingLayerToCollection**](LayersApi.md#addexistinglayertocollection) | **POST** /layerDb/collections/{collection}/layers/{layer} | Add an existing layer to a collection |
| [**addLayer**](LayersApi.md#addlayer) | **POST** /layerDb/collections/{collection}/layers | Add a new layer to a collection |
| [**addProvider**](LayersApi.md#addprovider) | **POST** /layerDb/providers | Add a new provider |
| [**autocompleteHandler**](LayersApi.md#autocompletehandler) | **GET** /layers/collections/search/autocomplete/{provider}/{collection} | Autocompletes the search on the contents of the collection of the given provider |
| [**deleteProvider**](LayersApi.md#deleteprovider) | **DELETE** /layerDb/providers/{provider} | Delete an existing provider |
| [**getProviderDefinition**](LayersApi.md#getproviderdefinition) | **GET** /layerDb/providers/{provider} | Get an existing provider\&#39;s definition |
| [**layerHandler**](LayersApi.md#layerhandler) | **GET** /layers/{provider}/{layer} | Retrieves the layer of the given provider |
| [**layerToDataset**](LayersApi.md#layertodataset) | **POST** /layers/{provider}/{layer}/dataset | Persist a raster layer from a provider as a dataset. |
| [**layerToWorkflowIdHandler**](LayersApi.md#layertoworkflowidhandler) | **POST** /layers/{provider}/{layer}/workflowId | Registers a layer from a provider as a workflow and returns the workflow id |
| [**listCollectionHandler**](LayersApi.md#listcollectionhandler) | **GET** /layers/collections/{provider}/{collection} | List the contents of the collection of the given provider |
| [**listProviders**](LayersApi.md#listproviders) | **GET** /layerDb/providers | List all providers |
| [**listRootCollectionsHandler**](LayersApi.md#listrootcollectionshandler) | **GET** /layers/collections | List all layer collections |
| [**providerCapabilitiesHandler**](LayersApi.md#providercapabilitieshandler) | **GET** /layers/{provider}/capabilities |  |
| [**removeCollection**](LayersApi.md#removecollection) | **DELETE** /layerDb/collections/{collection} | Remove a collection |
| [**removeCollectionFromCollection**](LayersApi.md#removecollectionfromcollection) | **DELETE** /layerDb/collections/{parent}/collections/{collection} | Delete a collection from a collection |
| [**removeLayer**](LayersApi.md#removelayer) | **DELETE** /layerDb/layers/{layer} | Remove a collection |
| [**removeLayerFromCollection**](LayersApi.md#removelayerfromcollection) | **DELETE** /layerDb/collections/{collection}/layers/{layer} | Remove a layer from a collection |
| [**searchHandler**](LayersApi.md#searchhandler) | **GET** /layers/collections/search/{provider}/{collection} | Searches the contents of the collection of the given provider |
| [**updateCollection**](LayersApi.md#updatecollection) | **PUT** /layerDb/collections/{collection} | Update a collection |
| [**updateLayer**](LayersApi.md#updatelayer) | **PUT** /layerDb/layers/{layer} | Update a layer |
| [**updateProviderDefinition**](LayersApi.md#updateproviderdefinition) | **PUT** /layerDb/providers/{provider} | Update an existing provider\&#39;s definition |



## addCollection

> IdResponse addCollection(collection, addLayerCollection)

Add a new collection to an existing collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { AddCollectionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer collection id
    collection: 05102bb3-a855-4a37-8a8a-30026a91fef1,
    // AddLayerCollection
    addLayerCollection: ...,
  } satisfies AddCollectionRequest;

  try {
    const data = await api.addCollection(body);
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
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |
| **addLayerCollection** | [AddLayerCollection](AddLayerCollection.md) |  | |

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


## addExistingCollectionToCollection

> addExistingCollectionToCollection(parent, collection)

Add an existing collection to a collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { AddExistingCollectionToCollectionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Parent layer collection id
    parent: 05102bb3-a855-4a37-8a8a-30026a91fef1,
    // string | Layer collection id
    collection: collection_example,
  } satisfies AddExistingCollectionToCollectionRequest;

  try {
    const data = await api.addExistingCollectionToCollection(body);
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
| **parent** | `string` | Parent layer collection id | [Defaults to `undefined`] |
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |

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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## addExistingLayerToCollection

> addExistingLayerToCollection(collection, layer)

Add an existing layer to a collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { AddExistingLayerToCollectionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer collection id
    collection: collection_example,
    // string | Layer id
    layer: layer_example,
  } satisfies AddExistingLayerToCollectionRequest;

  try {
    const data = await api.addExistingLayerToCollection(body);
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
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |
| **layer** | `string` | Layer id | [Defaults to `undefined`] |

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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## addLayer

> IdResponse addLayer(collection, addLayer)

Add a new layer to a collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { AddLayerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer collection id
    collection: 05102bb3-a855-4a37-8a8a-30026a91fef1,
    // AddLayer
    addLayer: ...,
  } satisfies AddLayerRequest;

  try {
    const data = await api.addLayer(body);
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
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |
| **addLayer** | [AddLayer](AddLayer.md) |  | |

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


## addProvider

> IdResponse addProvider(typedDataProviderDefinition)

Add a new provider

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { AddProviderRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // TypedDataProviderDefinition
    typedDataProviderDefinition: ...,
  } satisfies AddProviderRequest;

  try {
    const data = await api.addProvider(body);
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
| **typedDataProviderDefinition** | [TypedDataProviderDefinition](TypedDataProviderDefinition.md) |  | |

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


## autocompleteHandler

> Array&lt;string&gt; autocompleteHandler(provider, collection, searchType, searchString, limit, offset)

Autocompletes the search on the contents of the collection of the given provider

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { AutocompleteHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Data provider id
    provider: ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74,
    // string | Layer collection id
    collection: 05102bb3-a855-4a37-8a8a-30026a91fef1,
    // SearchType
    searchType: fulltext,
    // string
    searchString: test,
    // number
    limit: 20,
    // number
    offset: 0,
  } satisfies AutocompleteHandlerRequest;

  try {
    const data = await api.autocompleteHandler(body);
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
| **provider** | `string` | Data provider id | [Defaults to `undefined`] |
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |
| **searchType** | `SearchType` |  | [Defaults to `undefined`] [Enum: fulltext, prefix] |
| **searchString** | `string` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |
| **offset** | `number` |  | [Defaults to `undefined`] |

### Return type

**Array<string>**

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


## deleteProvider

> deleteProvider(provider)

Delete an existing provider

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { DeleteProviderRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer provider id
    provider: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies DeleteProviderRequest;

  try {
    const data = await api.deleteProvider(body);
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
| **provider** | `string` | Layer provider id | [Defaults to `undefined`] |

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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getProviderDefinition

> TypedDataProviderDefinition getProviderDefinition(provider)

Get an existing provider\&#39;s definition

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { GetProviderDefinitionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer provider id
    provider: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies GetProviderDefinitionRequest;

  try {
    const data = await api.getProviderDefinition(body);
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
| **provider** | `string` | Layer provider id | [Defaults to `undefined`] |

### Return type

[**TypedDataProviderDefinition**](TypedDataProviderDefinition.md)

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


## layerHandler

> Layer layerHandler(provider, layer)

Retrieves the layer of the given provider

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { LayerHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Data provider id
    provider: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Layer id
    layer: layer_example,
  } satisfies LayerHandlerRequest;

  try {
    const data = await api.layerHandler(body);
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
| **provider** | `string` | Data provider id | [Defaults to `undefined`] |
| **layer** | `string` | Layer id | [Defaults to `undefined`] |

### Return type

[**Layer**](Layer.md)

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


## layerToDataset

> TaskResponse layerToDataset(provider, layer)

Persist a raster layer from a provider as a dataset.

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { LayerToDatasetRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Data provider id
    provider: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Layer id
    layer: layer_example,
  } satisfies LayerToDatasetRequest;

  try {
    const data = await api.layerToDataset(body);
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
| **provider** | `string` | Data provider id | [Defaults to `undefined`] |
| **layer** | `string` | Layer id | [Defaults to `undefined`] |

### Return type

[**TaskResponse**](TaskResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Id of created task |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## layerToWorkflowIdHandler

> IdResponse layerToWorkflowIdHandler(provider, layer)

Registers a layer from a provider as a workflow and returns the workflow id

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { LayerToWorkflowIdHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Data provider id
    provider: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Layer id
    layer: layer_example,
  } satisfies LayerToWorkflowIdHandlerRequest;

  try {
    const data = await api.layerToWorkflowIdHandler(body);
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
| **provider** | `string` | Data provider id | [Defaults to `undefined`] |
| **layer** | `string` | Layer id | [Defaults to `undefined`] |

### Return type

[**IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Id of generated resource |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## listCollectionHandler

> LayerCollection listCollectionHandler(provider, collection, offset, limit)

List the contents of the collection of the given provider

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { ListCollectionHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Data provider id
    provider: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | Layer collection id
    collection: collection_example,
    // number
    offset: 0,
    // number
    limit: 20,
  } satisfies ListCollectionHandlerRequest;

  try {
    const data = await api.listCollectionHandler(body);
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
| **provider** | `string` | Data provider id | [Defaults to `undefined`] |
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |

### Return type

[**LayerCollection**](LayerCollection.md)

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


## listProviders

> Array&lt;LayerProviderListing&gt; listProviders(offset, limit)

List all providers

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { ListProvidersRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // number
    offset: 56,
    // number
    limit: 56,
  } satisfies ListProvidersRequest;

  try {
    const data = await api.listProviders(body);
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
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |

### Return type

[**Array&lt;LayerProviderListing&gt;**](LayerProviderListing.md)

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


## listRootCollectionsHandler

> LayerCollection listRootCollectionsHandler(offset, limit)

List all layer collections

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { ListRootCollectionsHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // number
    offset: 0,
    // number
    limit: 20,
  } satisfies ListRootCollectionsHandlerRequest;

  try {
    const data = await api.listRootCollectionsHandler(body);
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
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |

### Return type

[**LayerCollection**](LayerCollection.md)

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


## providerCapabilitiesHandler

> ProviderCapabilities providerCapabilitiesHandler(provider)



### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { ProviderCapabilitiesHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Data provider id
    provider: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies ProviderCapabilitiesHandlerRequest;

  try {
    const data = await api.providerCapabilitiesHandler(body);
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
| **provider** | `string` | Data provider id | [Defaults to `undefined`] |

### Return type

[**ProviderCapabilities**](ProviderCapabilities.md)

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


## removeCollection

> removeCollection(collection)

Remove a collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { RemoveCollectionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer collection id
    collection: collection_example,
  } satisfies RemoveCollectionRequest;

  try {
    const data = await api.removeCollection(body);
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
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |

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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## removeCollectionFromCollection

> removeCollectionFromCollection(parent, collection)

Delete a collection from a collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { RemoveCollectionFromCollectionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Parent layer collection id
    parent: 05102bb3-a855-4a37-8a8a-30026a91fef1,
    // string | Layer collection id
    collection: collection_example,
  } satisfies RemoveCollectionFromCollectionRequest;

  try {
    const data = await api.removeCollectionFromCollection(body);
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
| **parent** | `string` | Parent layer collection id | [Defaults to `undefined`] |
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |

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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## removeLayer

> removeLayer(layer)

Remove a collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { RemoveLayerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer id
    layer: layer_example,
  } satisfies RemoveLayerRequest;

  try {
    const data = await api.removeLayer(body);
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
| **layer** | `string` | Layer id | [Defaults to `undefined`] |

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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## removeLayerFromCollection

> removeLayerFromCollection(collection, layer)

Remove a layer from a collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { RemoveLayerFromCollectionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer collection id
    collection: collection_example,
    // string | Layer id
    layer: layer_example,
  } satisfies RemoveLayerFromCollectionRequest;

  try {
    const data = await api.removeLayerFromCollection(body);
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
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |
| **layer** | `string` | Layer id | [Defaults to `undefined`] |

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
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## searchHandler

> LayerCollection searchHandler(provider, collection, searchType, searchString, limit, offset)

Searches the contents of the collection of the given provider

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { SearchHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Data provider id
    provider: ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74,
    // string | Layer collection id
    collection: 05102bb3-a855-4a37-8a8a-30026a91fef1,
    // SearchType
    searchType: fulltext,
    // string
    searchString: test,
    // number
    limit: 20,
    // number
    offset: 0,
  } satisfies SearchHandlerRequest;

  try {
    const data = await api.searchHandler(body);
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
| **provider** | `string` | Data provider id | [Defaults to `undefined`] |
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |
| **searchType** | `SearchType` |  | [Defaults to `undefined`] [Enum: fulltext, prefix] |
| **searchString** | `string` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |
| **offset** | `number` |  | [Defaults to `undefined`] |

### Return type

[**LayerCollection**](LayerCollection.md)

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


## updateCollection

> updateCollection(collection, updateLayerCollection)

Update a collection

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { UpdateCollectionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer collection id
    collection: 05102bb3-a855-4a37-8a8a-30026a91fef1,
    // UpdateLayerCollection
    updateLayerCollection: ...,
  } satisfies UpdateCollectionRequest;

  try {
    const data = await api.updateCollection(body);
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
| **collection** | `string` | Layer collection id | [Defaults to `undefined`] |
| **updateLayerCollection** | [UpdateLayerCollection](UpdateLayerCollection.md) |  | |

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
| **200** |  |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## updateLayer

> updateLayer(layer, updateLayer)

Update a layer

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { UpdateLayerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer id
    layer: 05102bb3-a855-4a37-8a8a-30026a91fef1,
    // UpdateLayer
    updateLayer: ...,
  } satisfies UpdateLayerRequest;

  try {
    const data = await api.updateLayer(body);
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
| **layer** | `string` | Layer id | [Defaults to `undefined`] |
| **updateLayer** | [UpdateLayer](UpdateLayer.md) |  | |

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
| **200** |  |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## updateProviderDefinition

> updateProviderDefinition(provider, typedDataProviderDefinition)

Update an existing provider\&#39;s definition

### Example

```ts
import {
  Configuration,
  LayersApi,
} from '@geoengine/api-client';
import type { UpdateProviderDefinitionRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new LayersApi(config);

  const body = {
    // string | Layer provider id
    provider: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // TypedDataProviderDefinition
    typedDataProviderDefinition: ...,
  } satisfies UpdateProviderDefinitionRequest;

  try {
    const data = await api.updateProviderDefinition(body);
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
| **provider** | `string` | Layer provider id | [Defaults to `undefined`] |
| **typedDataProviderDefinition** | [TypedDataProviderDefinition](TypedDataProviderDefinition.md) |  | |

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

