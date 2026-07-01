
# GdalMetaDataList


## Properties

Name | Type
------------ | -------------
`type` | string
`resultDescriptor` | [RasterResultDescriptor](RasterResultDescriptor.md)
`params` | [Array&lt;GdalLoadingInfoTemporalSlice&gt;](GdalLoadingInfoTemporalSlice.md)

## Example

```typescript
import type { GdalMetaDataList } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "resultDescriptor": null,
  "params": null,
} satisfies GdalMetaDataList

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalMetaDataList
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


