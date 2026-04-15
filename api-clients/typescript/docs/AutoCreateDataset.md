
# AutoCreateDataset


## Properties

Name | Type
------------ | -------------
`datasetDescription` | string
`datasetName` | string
`layerName` | string
`mainFile` | string
`tags` | Array&lt;string&gt;
`upload` | string

## Example

```typescript
import type { AutoCreateDataset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "datasetDescription": null,
  "datasetName": null,
  "layerName": null,
  "mainFile": null,
  "tags": null,
  "upload": null,
} satisfies AutoCreateDataset

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AutoCreateDataset
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


