
# MetaDataDefinition


## Properties

Name | Type
------------ | -------------
`loadingInfo` | [OgrSourceDataset](OgrSourceDataset.md)
`resultDescriptor` | [RasterResultDescriptor](RasterResultDescriptor.md)
`type` | string
`cacheTtl` | number
`dataTime` | [TimeInterval](TimeInterval.md)
`params` | [Array&lt;GdalLoadingInfoTemporalSlice&gt;](GdalLoadingInfoTemporalSlice.md)
`step` | [TimeStep](TimeStep.md)
`timePlaceholders` | [{ [key: string]: GdalSourceTimePlaceholder; }](GdalSourceTimePlaceholder.md)
`time` | [TimeInterval](TimeInterval.md)
`bandOffset` | number
`end` | number
`start` | number

## Example

```typescript
import type { MetaDataDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "loadingInfo": null,
  "resultDescriptor": null,
  "type": null,
  "cacheTtl": null,
  "dataTime": null,
  "params": null,
  "step": null,
  "timePlaceholders": null,
  "time": null,
  "bandOffset": null,
  "end": null,
  "start": null,
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


