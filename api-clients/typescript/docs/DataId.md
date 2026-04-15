
# DataId

The identifier for loadable data. It is used in the source operators to get the loading info (aka parametrization) for accessing the data. Internal data is loaded from datasets, external from `DataProvider`s.

## Properties

Name | Type
------------ | -------------
`datasetId` | string
`type` | string
`layerId` | string
`providerId` | string

## Example

```typescript
import type { DataId } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "datasetId": null,
  "type": null,
  "layerId": null,
  "providerId": null,
} satisfies DataId

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DataId
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


