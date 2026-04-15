
# GdalSourceParameters

Parameters for the [`GdalSource`] operator.

## Properties

Name | Type
------------ | -------------
`data` | string
`overviewLevel` | number

## Example

```typescript
import type { GdalSourceParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "data": null,
  "overviewLevel": null,
} satisfies GdalSourceParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalSourceParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


