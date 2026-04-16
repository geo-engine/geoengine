
# MockMetaData


## Properties

Name | Type
------------ | -------------
`loadingInfo` | [MockDatasetDataSourceLoadingInfo](MockDatasetDataSourceLoadingInfo.md)
`resultDescriptor` | [VectorResultDescriptor](VectorResultDescriptor.md)
`type` | string

## Example

```typescript
import type { MockMetaData } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "loadingInfo": null,
  "resultDescriptor": null,
  "type": null,
} satisfies MockMetaData

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MockMetaData
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


