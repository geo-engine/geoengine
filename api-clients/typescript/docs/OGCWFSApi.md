# OGCWFSApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**wfsHandler**](OGCWFSApi.md#wfshandler) | **GET** /wfs/{workflow} | OGC WFS endpoint |



## wfsHandler

> GeoJson wfsHandler(workflow, request, bbox, count, filter, namespaces, propertyName, resultType, service, sortBy, srsName, time, typeNames, version)

OGC WFS endpoint

### Example

```ts
import {
  Configuration,
  OGCWFSApi,
} from '@geoengine/api-client';
import type { WfsHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCWFSApi(config);

  const body = {
    // string | Workflow id
    workflow: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // WfsRequest | type of WFS request
    request: ...,
    // string (optional)
    bbox: -90,-180,90,180,
    // number (optional)
    count: 789,
    // string (optional)
    filter: filter_example,
    // string (optional)
    namespaces: namespaces_example,
    // string (optional)
    propertyName: propertyName_example,
    // string (optional)
    resultType: resultType_example,
    // WfsService (optional)
    service: ...,
    // string (optional)
    sortBy: sortBy_example,
    // string (optional)
    srsName: EPSG:4326,
    // string (optional)
    time: 2014-04-01T12:00:00.000Z,
    // string (optional)
    typeNames: <Workflow Id>,
    // WfsVersion (optional)
    version: ...,
  } satisfies WfsHandlerRequest;

  try {
    const data = await api.wfsHandler(body);
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
| **request** | `WfsRequest` | type of WFS request | [Defaults to `undefined`] [Enum: GetCapabilities, GetFeature] |
| **bbox** | `string` |  | [Optional] [Defaults to `undefined`] |
| **count** | `number` |  | [Optional] [Defaults to `undefined`] |
| **filter** | `string` |  | [Optional] [Defaults to `undefined`] |
| **namespaces** | `string` |  | [Optional] [Defaults to `undefined`] |
| **propertyName** | `string` |  | [Optional] [Defaults to `undefined`] |
| **resultType** | `string` |  | [Optional] [Defaults to `undefined`] |
| **service** | `WfsService` |  | [Optional] [Defaults to `undefined`] [Enum: WFS] |
| **sortBy** | `string` |  | [Optional] [Defaults to `undefined`] |
| **srsName** | `string` |  | [Optional] [Defaults to `undefined`] |
| **time** | `string` |  | [Optional] [Defaults to `undefined`] |
| **typeNames** | `string` |  | [Optional] [Defaults to `undefined`] |
| **version** | `WfsVersion` |  | [Optional] [Defaults to `undefined`] [Enum: 2.0.0] |

### Return type

[**GeoJson**](GeoJson.md)

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

