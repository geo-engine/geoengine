
# StacSearchGrid

Native projected-area-of-use grid for distinct dataset/cell/time-step STAC searches.

## Properties

Name | Type
------------ | -------------
`datasetId` | string
`projection` | string
`projString` | string
`extent` | Array&lt;number&gt;
`cellSize` | number
`columns` | number
`rows` | number

## Example

```typescript
import type { StacSearchGrid } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "datasetId": null,
  "projection": null,
  "projString": null,
  "extent": null,
  "cellSize": null,
  "columns": null,
  "rows": null,
} satisfies StacSearchGrid

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StacSearchGrid
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


