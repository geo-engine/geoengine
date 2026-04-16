# OGCWMSApi

All URIs are relative to *https://geoengine.io/api*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**wmsHandler**](OGCWMSApi.md#wmshandler) | **GET** /wms/{workflow} | OGC WMS endpoint |



## wmsHandler

> Blob wmsHandler(workflow, request, bbox, bgcolor, crs, elevation, exceptions, format, height, infoFormat, layer, layers, queryLayers, service, sld, sldBody, styles, time, transparent, version, width)

OGC WMS endpoint

### Example

```ts
import {
  Configuration,
  OGCWMSApi,
} from '@geoengine/api-client';
import type { WmsHandlerRequest } from '@geoengine/api-client';

async function example() {
  console.log("🚀 Testing @geoengine/api-client SDK...");
  const config = new Configuration({ 
    // Configure HTTP bearer authorization: session_token
    accessToken: "YOUR BEARER TOKEN",
  });
  const api = new OGCWMSApi(config);

  const body = {
    // string | Workflow id
    workflow: 38400000-8cf0-11bd-b23e-10b96e4ef00d,
    // WmsRequest | type of WMS request
    request: ...,
    // string (optional)
    bbox: -90,-180,90,180,
    // string (optional)
    bgcolor: bgcolor_example,
    // string (optional)
    crs: EPSG:4326,
    // string (optional)
    elevation: elevation_example,
    // GetMapExceptionFormat (optional)
    exceptions: ...,
    // WmsResponseFormat (optional)
    format: ...,
    // number (optional)
    height: 256,
    // string (optional)
    infoFormat: infoFormat_example,
    // string (optional)
    layer: layer_example,
    // string (optional)
    layers: <Workflow Id>,
    // string (optional)
    queryLayers: queryLayers_example,
    // WmsService (optional)
    service: ...,
    // string (optional)
    sld: sld_example,
    // string (optional)
    sldBody: sldBody_example,
    // string (optional)
    styles: custom:{"type":"linearGradient","breakpoints":[{"value":1,"color":[0,0,0,255]},{"value":255,"color":[255,255,255,255]}],"noDataColor":[0,0,0,0],"defaultColor":[0,0,0,0]},
    // string (optional)
    time: 2014-04-01T12:00:00.000Z,
    // boolean (optional)
    transparent: true,
    // WmsVersion (optional)
    version: ...,
    // number (optional)
    width: 512,
  } satisfies WmsHandlerRequest;

  try {
    const data = await api.wmsHandler(body);
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
| **request** | `WmsRequest` | type of WMS request | [Defaults to `undefined`] [Enum: GetCapabilities, GetMap, GetFeatureInfo, GetStyles, GetLegendGraphic] |
| **bbox** | `string` |  | [Optional] [Defaults to `undefined`] |
| **bgcolor** | `string` |  | [Optional] [Defaults to `undefined`] |
| **crs** | `string` |  | [Optional] [Defaults to `undefined`] |
| **elevation** | `string` |  | [Optional] [Defaults to `undefined`] |
| **exceptions** | `GetMapExceptionFormat` |  | [Optional] [Defaults to `undefined`] [Enum: XML, JSON] |
| **format** | `WmsResponseFormat` |  | [Optional] [Defaults to `undefined`] [Enum: text/xml, image/png] |
| **height** | `number` |  | [Optional] [Defaults to `undefined`] |
| **infoFormat** | `string` |  | [Optional] [Defaults to `undefined`] |
| **layer** | `string` |  | [Optional] [Defaults to `undefined`] |
| **layers** | `string` |  | [Optional] [Defaults to `undefined`] |
| **queryLayers** | `string` |  | [Optional] [Defaults to `undefined`] |
| **service** | `WmsService` |  | [Optional] [Defaults to `undefined`] [Enum: WMS] |
| **sld** | `string` |  | [Optional] [Defaults to `undefined`] |
| **sldBody** | `string` |  | [Optional] [Defaults to `undefined`] |
| **styles** | `string` |  | [Optional] [Defaults to `undefined`] |
| **time** | `string` |  | [Optional] [Defaults to `undefined`] |
| **transparent** | `boolean` |  | [Optional] [Defaults to `undefined`] |
| **version** | `WmsVersion` |  | [Optional] [Defaults to `undefined`] [Enum: 1.3.0] |
| **width** | `number` |  | [Optional] [Defaults to `undefined`] |

### Return type

**Blob**

### Authorization

[session_token](../README.md#session_token)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `image/png`


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | PNG Image |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)

