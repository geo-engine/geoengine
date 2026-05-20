
# StacTimeStep


## Properties

Name | Type
------------ | -------------
`granularity` | [TimeGranularity](TimeGranularity.md)
`value` | number

## Example

```typescript
import type { StacTimeStep } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "granularity": null,
  "value": null,
} satisfies StacTimeStep

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StacTimeStep
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


