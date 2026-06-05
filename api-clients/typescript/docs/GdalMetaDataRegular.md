
# GdalMetaDataRegular


## Properties

Name | Type
------------ | -------------
`type` | string
`resultDescriptor` | [RasterResultDescriptor](RasterResultDescriptor.md)
`params` | [GdalDatasetParameters](GdalDatasetParameters.md)
`timePlaceholders` | [{ [key: string]: GdalSourceTimePlaceholder; }](GdalSourceTimePlaceholder.md)
`dataTime` | [TimeInterval](TimeInterval.md)
`step` | [TimeStep](TimeStep.md)
`cacheTtl` | number

## Example

```typescript
import type { GdalMetaDataRegular } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "resultDescriptor": null,
  "params": null,
  "timePlaceholders": null,
  "dataTime": null,
  "step": null,
  "cacheTtl": null,
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


