
# GdalMetaDataStatic


## Properties

Name | Type
------------ | -------------
`cacheTtl` | number
`params` | [GdalDatasetParameters](GdalDatasetParameters.md)
`resultDescriptor` | [RasterResultDescriptor](RasterResultDescriptor.md)
`time` | [TimeInterval](TimeInterval.md)
`type` | string

## Example

```typescript
import type { GdalMetaDataStatic } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "cacheTtl": null,
  "params": null,
  "resultDescriptor": null,
  "time": null,
  "type": null,
} satisfies GdalMetaDataStatic

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalMetaDataStatic
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


