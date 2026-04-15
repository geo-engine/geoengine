# OGCWCSApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**wcsHandler**](OGCWCSApi.md#wcshandler) | **GET** /wcs/{workflow} | OGC WCS endpoint |



## wcsHandler

> string wcsHandler(workflow, request, boundingbox, format, gridbasecrs, gridoffsets, gridorigin, identifier, identifiers, nodatavalue, resx, resy, service, time, version)

OGC WCS endpoint

### Example

```ts
import {
  Configuration,
  OGCWCSApi,
} from '@geoengine/api-client';
import type { WcsHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCWCSApi(config);

  const body = {
    // string | Workflow id
    workflow: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // WcsRequest | type of WCS request
    request: ...,
    // string (optional)
    boundingbox: -90,-180,90,180,urn:ogc:def:crs:EPSG::4326,
    // GetCoverageFormat (optional)
    format: ...,
    // string (optional)
    gridbasecrs: urn:ogc:def:crs:EPSG::4326,
    // string (optional)
    gridoffsets: -0.1,0.1,
    // string (optional)
    gridorigin: 90,-180,
    // string (optional)
    identifier: <Workflow Id>,
    // string (optional)
    identifiers: <Workflow Id>,
    // number (optional)
    nodatavalue: 1.2,
    // number (optional)
    resx: 1.2,
    // number (optional)
    resy: 1.2,
    // WcsService (optional)
    service: ...,
    // string (optional)
    time: time_example,
    // WcsVersion (optional)
    version: ...,
  } satisfies WcsHandlerRequest;

  try {
    const data = await api.wcsHandler(body);
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
| **workflow** | `string` | Workflow id | [Defaults to `undefined`] |
| **request** | `WcsRequest` | type of WCS request | [Defaults to `undefined`] [Enum: GetCapabilities, DescribeCoverage, GetCoverage] |
| **boundingbox** | `string` |  | [Optional] [Defaults to `undefined`] |
| **format** | `GetCoverageFormat` |  | [Optional] [Defaults to `undefined`] [Enum: image/tiff] |
| **gridbasecrs** | `string` |  | [Optional] [Defaults to `undefined`] |
| **gridoffsets** | `string` |  | [Optional] [Defaults to `undefined`] |
| **gridorigin** | `string` |  | [Optional] [Defaults to `undefined`] |
| **identifier** | `string` |  | [Optional] [Defaults to `undefined`] |
| **identifiers** | `string` |  | [Optional] [Defaults to `undefined`] |
| **nodatavalue** | `number` |  | [Optional] [Defaults to `undefined`] |
| **resx** | `number` |  | [Optional] [Defaults to `undefined`] |
| **resy** | `number` |  | [Optional] [Defaults to `undefined`] |
| **service** | `WcsService` |  | [Optional] [Defaults to `undefined`] [Enum: WCS] |
| **time** | `string` |  | [Optional] [Defaults to `undefined`] |
| **version** | `WcsVersion` |  | [Optional] [Defaults to `undefined`] [Enum: 1.1.0, 1.1.1] |

### Return type

**string**

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `text/xml`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

