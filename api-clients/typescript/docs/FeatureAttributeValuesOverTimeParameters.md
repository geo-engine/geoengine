
# FeatureAttributeValuesOverTimeParameters

The parameter spec for `FeatureAttributeValuesOverTime`.

## Properties

Name | Type
------------ | -------------
`idColumn` | string
`valueColumn` | string

## Example

```typescript
import type { FeatureAttributeValuesOverTimeParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "idColumn": null,
  "valueColumn": null,
} satisfies FeatureAttributeValuesOverTimeParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as FeatureAttributeValuesOverTimeParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


