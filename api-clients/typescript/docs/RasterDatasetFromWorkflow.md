
# RasterDatasetFromWorkflow

parameter for the dataset from workflow handler (body)

## Properties

Name | Type
------------ | -------------
`asCog` | boolean
`description` | string
`displayName` | string
`name` | string
`query` | [RasterToDatasetQueryRectangle](RasterToDatasetQueryRectangle.md)

## Example

```typescript
import type { RasterDatasetFromWorkflow } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "asCog": null,
  "description": null,
  "displayName": null,
  "name": null,
  "query": null,
} satisfies RasterDatasetFromWorkflow

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterDatasetFromWorkflow
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


