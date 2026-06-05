
# LegacyTypedOperator

An enum to differentiate between `Operator` variants

## Properties

Name | Type
------------ | -------------
`type` | string
`operator` | [LegacyTypedOperatorOperator](LegacyTypedOperatorOperator.md)

## Example

```typescript
import type { LegacyTypedOperator } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "operator": null,
} satisfies LegacyTypedOperator

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LegacyTypedOperator
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


