
# DerivedNumber


## Properties

Name | Type
------------ | -------------
`type` | string
`attribute` | string
`factor` | number
`defaultValue` | number

## Example

```typescript
import type { DerivedNumber } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "attribute": null,
  "factor": null,
  "defaultValue": null,
} satisfies DerivedNumber

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DerivedNumber
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


