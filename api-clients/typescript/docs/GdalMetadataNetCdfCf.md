
# GdalMetadataNetCdfCf

Meta data for 4D `NetCDF` CF datasets

## Properties

Name | Type
------------ | -------------
`bandOffset` | number
`cacheTtl` | number
`end` | number
`params` | [GdalDatasetParameters](GdalDatasetParameters.md)
`resultDescriptor` | [RasterResultDescriptor](RasterResultDescriptor.md)
`start` | number
`step` | [TimeStep](TimeStep.md)
`type` | string

## Example

```typescript
import type { GdalMetadataNetCdfCf } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "bandOffset": null,
  "cacheTtl": null,
  "end": null,
  "params": null,
  "resultDescriptor": null,
  "start": null,
  "step": null,
  "type": null,
} satisfies GdalMetadataNetCdfCf

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalMetadataNetCdfCf
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


