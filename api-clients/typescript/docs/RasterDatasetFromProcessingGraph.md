
# RasterDatasetFromProcessingGraph

parameter for the dataset from processing graph handler (body)

## Properties

Name | Type
------------ | -------------
`name` | string
`displayName` | string
`description` | string
`query` | [RasterToDatasetQueryRectangle](RasterToDatasetQueryRectangle.md)
`asCog` | boolean

## Example

```typescript
import type { RasterDatasetFromProcessingGraph } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": null,
  "displayName": null,
  "description": null,
  "query": null,
  "asCog": null,
} satisfies RasterDatasetFromProcessingGraph

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterDatasetFromProcessingGraph
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


