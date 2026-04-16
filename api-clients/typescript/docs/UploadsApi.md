# UploadsApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**listUploadFileLayersHandler**](UploadsApi.md#listuploadfilelayershandler) | **GET** /uploads/{upload_id}/files/{file_name}/layers | List the layers of on uploaded file. |
| [**listUploadFilesHandler**](UploadsApi.md#listuploadfileshandler) | **GET** /uploads/{upload_id}/files | List the files of on upload. |
| [**uploadHandler**](UploadsApi.md#uploadhandler) | **POST** /upload | Uploads files. |



## listUploadFileLayersHandler

> UploadFileLayersResponse listUploadFileLayersHandler(uploadId, fileName)

List the layers of on uploaded file.

### Example

```ts
import {
  Configuration,
  UploadsApi,
} from '@geoengine/api-client';
import type { ListUploadFileLayersHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UploadsApi(config);

  const body = {
    // string | Upload id
    uploadId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // string | File name
    fileName: fileName_example,
  } satisfies ListUploadFileLayersHandlerRequest;

  try {
    const data = await api.listUploadFileLayersHandler(body);
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
| **uploadId** | `string` | Upload id | [Defaults to `undefined`] |
| **fileName** | `string` | File name | [Defaults to `undefined`] |

### Return type

[**UploadFileLayersResponse**](UploadFileLayersResponse.md)

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


## listUploadFilesHandler

> UploadFilesResponse listUploadFilesHandler(uploadId)

List the files of on upload.

### Example

```ts
import {
  Configuration,
  UploadsApi,
} from '@geoengine/api-client';
import type { ListUploadFilesHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UploadsApi(config);

  const body = {
    // string | Upload id
    uploadId: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
  } satisfies ListUploadFilesHandlerRequest;

  try {
    const data = await api.listUploadFilesHandler(body);
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
| **uploadId** | `string` | Upload id | [Defaults to `undefined`] |

### Return type

[**UploadFilesResponse**](UploadFilesResponse.md)

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


## uploadHandler

> IdResponse uploadHandler(files)

Uploads files.

### Example

```ts
import {
  Configuration,
  UploadsApi,
} from '@geoengine/api-client';
import type { UploadHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new UploadsApi(config);

  const body = {
    // Array<Blob>
    files: /path/to/file.txt,
  } satisfies UploadHandlerRequest;

  try {
    const data = await api.uploadHandler(body);
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
| **files** | `Array<Blob>` |  | |

### Return type

[**IdResponse**](IdResponse.md)

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: `multipart/form-data`
- **Accept**: `application/json`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Id of generated resource |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

