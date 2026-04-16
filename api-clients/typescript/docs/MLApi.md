# MLApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addMlModel**](MLApi.md#addmlmodel) | **POST** /ml/models | Create a new ml model. |
| [**getMlModel**](MLApi.md#getmlmodel) | **GET** /ml/models/{model_name} | Get ml model by name. |
| [**listMlModels**](MLApi.md#listmlmodels) | **GET** /ml/models | List ml models. |



## addMlModel

> MlModelNameResponse addMlModel(mlModel)

Create a new ml model.

### Example

```ts
import {
  Configuration,
  MLApi,
} from '@geoengine/api-client';
import type { AddMlModelRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new MLApi(config);

  const body = {
    // MlModel
    mlModel: ...,
  } satisfies AddMlModelRequest;

  try {
    const data = await api.addMlModel(body);
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
| **mlModel** | [MlModel](MlModel.md) |  | |

### Return type

[**MlModelNameResponse**](MlModelNameResponse.md)

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


## getMlModel

> MlModel getMlModel(modelName)

Get ml model by name.

### Example

```ts
import {
  Configuration,
  MLApi,
} from '@geoengine/api-client';
import type { GetMlModelRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new MLApi(config);

  const body = {
    // string | Ml Model Name
    modelName: modelName_example,
  } satisfies GetMlModelRequest;

  try {
    const data = await api.getMlModel(body);
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
| **modelName** | `string` | Ml Model Name | [Defaults to `undefined`] |

### Return type

[**MlModel**](MlModel.md)

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


## listMlModels

> Array&lt;MlModel&gt; listMlModels()

List ml models.

### Example

```ts
import {
  Configuration,
  MLApi,
} from '@geoengine/api-client';
import type { ListMlModelsRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new MLApi(config);

  try {
    const data = await api.listMlModels();
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

[**Array&lt;MlModel&gt;**](MlModel.md)

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

