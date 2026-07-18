
# MetaDataSuggestion


## Properties

Name | Type
------------ | -------------
`mainFile` | string
`layerName` | string
`metaData` | [MetaDataDefinition](MetaDataDefinition.md)

## Example

```typescript
import type { MetaDataSuggestion } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "mainFile": null,
  "layerName": null,
  "metaData": null,
} satisfies MetaDataSuggestion

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MetaDataSuggestion
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


