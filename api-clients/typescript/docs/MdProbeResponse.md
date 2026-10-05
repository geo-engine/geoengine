
# MdProbeResponse

What a client needs to create an `MdGdalSource` dataset from the probed files: the dataset-level metadata for `POST /dataset`, plus the rows for `POST /dataset/{dataset}/md-tiles`.

## Properties

Name | Type
------------ | -------------
`metaData` | [MetaDataDefinition](MetaDataDefinition.md)
`tiles` | [Array&lt;AddDatasetMdTile&gt;](AddDatasetMdTile.md)

## Example

```typescript
import type { MdProbeResponse } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "metaData": null,
  "tiles": null,
} satisfies MdProbeResponse

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MdProbeResponse
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


