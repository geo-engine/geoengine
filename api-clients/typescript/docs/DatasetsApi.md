# DatasetsApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addDatasetTilesHandler**](DatasetsApi.md#adddatasettileshandler) | **POST** /dataset/{dataset}/tiles | Add a tile to a gdal dataset. |
| [**autoCreateDatasetHandler**](DatasetsApi.md#autocreatedatasethandler) | **POST** /dataset/auto | Creates a new dataset using previously uploaded files. The format of the files will be automatically detected when possible. |
| [**createDatasetHandler**](DatasetsApi.md#createdatasethandler) | **POST** /dataset | Creates a new dataset referencing files. Users can reference previously uploaded files. Admins can reference files from a volume. |
| [**deleteDatasetHandler**](DatasetsApi.md#deletedatasethandler) | **DELETE** /dataset/{dataset} | Delete a dataset |
| [**getDatasetHandler**](DatasetsApi.md#getdatasethandler) | **GET** /dataset/{dataset} | Retrieves details about a dataset using the internal name. |
| [**getLoadingInfoHandler**](DatasetsApi.md#getloadinginfohandler) | **GET** /dataset/{dataset}/loadingInfo | Retrieves the loading information of a dataset |
| [**listDatasetsHandler**](DatasetsApi.md#listdatasetshandler) | **GET** /datasets | Lists available datasets. |
| [**listVolumeFileLayersHandler**](DatasetsApi.md#listvolumefilelayershandler) | **GET** /dataset/volumes/{volume_name}/files/{file_name}/layers | List the layers of a file in a volume. |
| [**listVolumesHandler**](DatasetsApi.md#listvolumeshandler) | **GET** /dataset/volumes | Lists available volumes. |
| [**suggestMetaDataHandler**](DatasetsApi.md#suggestmetadatahandler) | **POST** /dataset/suggest | Inspects an upload and suggests metadata that can be used when creating a new dataset based on it. Tries to automatically detect the main file and layer name if not specified. |
| [**updateDatasetHandler**](DatasetsApi.md#updatedatasethandler) | **POST** /dataset/{dataset} | Update details about a dataset using the internal name. |
| [**updateDatasetProvenanceHandler**](DatasetsApi.md#updatedatasetprovenancehandler) | **PUT** /dataset/{dataset}/provenance |  |
| [**updateDatasetSymbologyHandler**](DatasetsApi.md#updatedatasetsymbologyhandler) | **PUT** /dataset/{dataset}/symbology | Updates the dataset\&#39;s symbology |
| [**updateLoadingInfoHandler**](DatasetsApi.md#updateloadinginfohandler) | **PUT** /dataset/{dataset}/loadingInfo | Updates the dataset\&#39;s loading info |



## addDatasetTilesHandler

> addDatasetTilesHandler(dataset, autoCreateDataset)

Add a tile to a gdal dataset.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { AddDatasetTilesHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset Name
    dataset: dataset_example,
    // AutoCreateDataset
    autoCreateDataset: ...,
  } satisfies AddDatasetTilesHandlerRequest;

  try {
    const data = await api.addDatasetTilesHandler(body);
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
| **dataset** | `string` | Dataset Name | [Defaults to `undefined`] |
| **autoCreateDataset** | [AutoCreateDataset](AutoCreateDataset.md) |  | |

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


## autoCreateDatasetHandler

> DatasetNameResponse autoCreateDatasetHandler(autoCreateDataset)

Creates a new dataset using previously uploaded files. The format of the files will be automatically detected when possible.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { AutoCreateDatasetHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // AutoCreateDataset
    autoCreateDataset: ...,
  } satisfies AutoCreateDatasetHandlerRequest;

  try {
    const data = await api.autoCreateDatasetHandler(body);
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
| **autoCreateDataset** | [AutoCreateDataset](AutoCreateDataset.md) |  | |

### Return type

[**DatasetNameResponse**](DatasetNameResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** |  |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |
| **413** | Payload too large |  -  |
| **415** | Media type of application/json is expected |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## createDatasetHandler

> DatasetNameResponse createDatasetHandler(createDataset)

Creates a new dataset referencing files. Users can reference previously uploaded files. Admins can reference files from a volume.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { CreateDatasetHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // CreateDataset
    createDataset: ...,
  } satisfies CreateDatasetHandlerRequest;

  try {
    const data = await api.createDatasetHandler(body);
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
| **createDataset** | [CreateDataset](CreateDataset.md) |  | |

### Return type

[**DatasetNameResponse**](DatasetNameResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** |  |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## deleteDatasetHandler

> deleteDatasetHandler(dataset)

Delete a dataset

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { DeleteDatasetHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset id
    dataset: dataset_example,
  } satisfies DeleteDatasetHandlerRequest;

  try {
    const data = await api.deleteDatasetHandler(body);
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
| **dataset** | `string` | Dataset id | [Defaults to `undefined`] |

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
| **200** | OK |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getDatasetHandler

> Dataset getDatasetHandler(dataset)

Retrieves details about a dataset using the internal name.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { GetDatasetHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset Name
    dataset: dataset_example,
  } satisfies GetDatasetHandlerRequest;

  try {
    const data = await api.getDatasetHandler(body);
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
| **dataset** | `string` | Dataset Name | [Defaults to `undefined`] |

### Return type

[**Dataset**](Dataset.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## getLoadingInfoHandler

> MetaDataDefinition getLoadingInfoHandler(dataset)

Retrieves the loading information of a dataset

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { GetLoadingInfoHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset Name
    dataset: dataset_example,
  } satisfies GetLoadingInfoHandlerRequest;

  try {
    const data = await api.getLoadingInfoHandler(body);
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
| **dataset** | `string` | Dataset Name | [Defaults to `undefined`] |

### Return type

[**MetaDataDefinition**](MetaDataDefinition.md)

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


## listDatasetsHandler

> Array&lt;DatasetListing&gt; listDatasetsHandler(order, offset, limit, filter, tags)

Lists available datasets.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { ListDatasetsHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // OrderBy
    order: NameAsc,
    // number
    offset: 0,
    // number
    limit: 2,
    // string (optional)
    filter: Germany,
    // Array<string> (optional)
    tags: ['tag1', 'tag2'],
  } satisfies ListDatasetsHandlerRequest;

  try {
    const data = await api.listDatasetsHandler(body);
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
| **order** | `OrderBy` |  | [Defaults to `undefined`] [Enum: NameAsc, NameDesc] |
| **offset** | `number` |  | [Defaults to `undefined`] |
| **limit** | `number` |  | [Defaults to `undefined`] |
| **filter** | `string` |  | [Optional] [Defaults to `undefined`] |
| **tags** | `Array<string>` |  | [Optional] |

### Return type

[**Array&lt;DatasetListing&gt;**](DatasetListing.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## listVolumeFileLayersHandler

> VolumeFileLayersResponse listVolumeFileLayersHandler(volumeName, fileName)

List the layers of a file in a volume.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { ListVolumeFileLayersHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Volume name
    volumeName: volumeName_example,
    // string | File name
    fileName: fileName_example,
  } satisfies ListVolumeFileLayersHandlerRequest;

  try {
    const data = await api.listVolumeFileLayersHandler(body);
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
| **volumeName** | `string` | Volume name | [Defaults to `undefined`] |
| **fileName** | `string` | File name | [Defaults to `undefined`] |

### Return type

[**VolumeFileLayersResponse**](VolumeFileLayersResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** |  |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## listVolumesHandler

> Array&lt;Volume&gt; listVolumesHandler()

Lists available volumes.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { ListVolumesHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  try {
    const data = await api.listVolumesHandler();
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

[**Array&lt;Volume&gt;**](Volume.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## suggestMetaDataHandler

> MetaDataSuggestion suggestMetaDataHandler(suggestMetaData)

Inspects an upload and suggests metadata that can be used when creating a new dataset based on it. Tries to automatically detect the main file and layer name if not specified.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { SuggestMetaDataHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // SuggestMetaData
    suggestMetaData: ...,
  } satisfies SuggestMetaDataHandlerRequest;

  try {
    const data = await api.suggestMetaDataHandler(body);
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
| **suggestMetaData** | [SuggestMetaData](SuggestMetaData.md) |  | |

### Return type

[**MetaDataSuggestion**](MetaDataSuggestion.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## updateDatasetHandler

> updateDatasetHandler(dataset, updateDataset)

Update details about a dataset using the internal name.

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { UpdateDatasetHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset Name
    dataset: dataset_example,
    // UpdateDataset
    updateDataset: ...,
  } satisfies UpdateDatasetHandlerRequest;

  try {
    const data = await api.updateDatasetHandler(body);
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
| **dataset** | `string` | Dataset Name | [Defaults to `undefined`] |
| **updateDataset** | [UpdateDataset](UpdateDataset.md) |  | |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## updateDatasetProvenanceHandler

> updateDatasetProvenanceHandler(dataset, provenances)



### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { UpdateDatasetProvenanceHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset Name
    dataset: dataset_example,
    // Provenances
    provenances: ...,
  } satisfies UpdateDatasetProvenanceHandlerRequest;

  try {
    const data = await api.updateDatasetProvenanceHandler(body);
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
| **dataset** | `string` | Dataset Name | [Defaults to `undefined`] |
| **provenances** | [Provenances](Provenances.md) |  | |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## updateDatasetSymbologyHandler

> updateDatasetSymbologyHandler(dataset, symbology)

Updates the dataset\&#39;s symbology

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { UpdateDatasetSymbologyHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset Name
    dataset: dataset_example,
    // Symbology
    symbology: ...,
  } satisfies UpdateDatasetSymbologyHandlerRequest;

  try {
    const data = await api.updateDatasetSymbologyHandler(body);
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
| **dataset** | `string` | Dataset Name | [Defaults to `undefined`] |
| **symbology** | [Symbology](Symbology.md) |  | |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


## updateLoadingInfoHandler

> updateLoadingInfoHandler(dataset, metaDataDefinition)

Updates the dataset\&#39;s loading info

### Example

```ts
import {
  Configuration,
  DatasetsApi,
} from '@geoengine/api-client';
import type { UpdateLoadingInfoHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new DatasetsApi(config);

  const body = {
    // string | Dataset Name
    dataset: dataset_example,
    // MetaDataDefinition
    metaDataDefinition: ...,
  } satisfies UpdateLoadingInfoHandlerRequest;

  try {
    const data = await api.updateLoadingInfoHandler(body);
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
| **dataset** | `string` | Dataset Name | [Defaults to `undefined`] |
| **metaDataDefinition** | [MetaDataDefinition](MetaDataDefinition.md) |  | |

### Return type

`void` (Empty response body)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **400** | Bad request |  -  |
| **401** | Authorization failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

