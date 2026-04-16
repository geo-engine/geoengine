
# VectorColumnInfo


## Properties

Name | Type
------------ | -------------
`dataType` | [FeatureDataType](FeatureDataType.md)
`measurement` | [Measurement](Measurement.md)

## Example

```typescript
import type { VectorColumnInfo } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "dataType": null,
  "measurement": null,
} satisfies VectorColumnInfo

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as VectorColumnInfo
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


