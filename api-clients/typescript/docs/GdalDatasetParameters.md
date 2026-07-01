
# GdalDatasetParameters

Parameters for loading data using Gdal

## Properties

Name | Type
------------ | -------------
`filePath` | string
`rasterbandChannel` | number
`geoTransform` | [GeoTransform](GeoTransform.md)
`width` | number
`height` | number
`fileNotFoundHandling` | [FileNotFoundHandling](FileNotFoundHandling.md)
`noDataValue` | number
`propertiesMapping` | [Array&lt;GdalMetadataMapping&gt;](GdalMetadataMapping.md)
`gdalOpenOptions` | Array&lt;string&gt;
`gdalConfigOptions` | Array&lt;Array&lt;string&gt;&gt;
`allowAlphabandAsMask` | boolean

## Example

```typescript
import type { GdalDatasetParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "filePath": null,
  "rasterbandChannel": null,
  "geoTransform": null,
  "width": null,
  "height": null,
  "fileNotFoundHandling": null,
  "noDataValue": null,
  "propertiesMapping": null,
  "gdalOpenOptions": null,
  "gdalConfigOptions": null,
  "allowAlphabandAsMask": null,
} satisfies GdalDatasetParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalDatasetParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


