
# MetaDataDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`loadingInfo` | [OgrSourceDataset](OgrSourceDataset.md)
`resultDescriptor` | [RasterResultDescriptor](RasterResultDescriptor.md)
`params` | [Array&lt;GdalLoadingInfoTemporalSlice&gt;](GdalLoadingInfoTemporalSlice.md)
`timePlaceholders` | [{ [key: string]: GdalSourceTimePlaceholder; }](GdalSourceTimePlaceholder.md)
`dataTime` | [TimeInterval](TimeInterval.md)
`step` | [TimeStep](TimeStep.md)
`cacheTtl` | number
`time` | [TimeInterval](TimeInterval.md)
`start` | number
`end` | number
`bandOffset` | number

## Example

```typescript
import type { MetaDataDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "loadingInfo": null,
  "resultDescriptor": null,
  "params": null,
  "timePlaceholders": null,
  "dataTime": null,
  "step": null,
  "cacheTtl": null,
  "time": null,
  "start": null,
  "end": null,
  "bandOffset": null,
} satisfies MetaDataDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MetaDataDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


