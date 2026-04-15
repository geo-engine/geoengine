
# DatasetDefinition


## Properties

Name | Type
------------ | -------------
`metaData` | [MetaDataDefinition](MetaDataDefinition.md)
`properties` | [AddDataset](AddDataset.md)

## Example

```typescript
import type { DatasetDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "metaData": null,
  "properties": null,
} satisfies DatasetDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DatasetDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


