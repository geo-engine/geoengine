
# GdalDatasetParameters

Parameters for loading data using Gdal

## Properties

Name | Type
------------ | -------------
`allowAlphabandAsMask` | boolean
`fileNotFoundHandling` | [FileNotFoundHandling](FileNotFoundHandling.md)
`filePath` | string
`gdalConfigOptions` | Array&lt;Array&lt;string&gt;&gt;
`gdalOpenOptions` | Array&lt;string&gt;
`geoTransform` | [GeoTransform](GeoTransform.md)
`height` | number
`noDataValue` | number
`propertiesMapping` | [Array&lt;GdalMetadataMapping&gt;](GdalMetadataMapping.md)
`rasterbandChannel` | number
`width` | number

## Example

```typescript
import type { GdalDatasetParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "allowAlphabandAsMask": null,
  "fileNotFoundHandling": null,
  "filePath": null,
  "gdalConfigOptions": null,
  "gdalOpenOptions": null,
  "geoTransform": null,
  "height": null,
  "noDataValue": null,
  "propertiesMapping": null,
  "rasterbandChannel": null,
  "width": null,
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


