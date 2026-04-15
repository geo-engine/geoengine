
# GdalMetaDataRegular


## Properties

Name | Type
------------ | -------------
`cacheTtl` | number
`dataTime` | [TimeInterval](TimeInterval.md)
`params` | [GdalDatasetParameters](GdalDatasetParameters.md)
`resultDescriptor` | [RasterResultDescriptor](RasterResultDescriptor.md)
`step` | [TimeStep](TimeStep.md)
`timePlaceholders` | [{ [key: string]: GdalSourceTimePlaceholder; }](GdalSourceTimePlaceholder.md)
`type` | string

## Example

```typescript
import type { GdalMetaDataRegular } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "cacheTtl": null,
  "dataTime": null,
  "params": null,
  "resultDescriptor": null,
  "step": null,
  "timePlaceholders": null,
  "type": null,
} satisfies GdalMetaDataRegular

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalMetaDataRegular
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


