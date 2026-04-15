
# StacApiRetries


## Properties

Name | Type
------------ | -------------
`exponentialBackoffFactor` | number
`initialDelayMs` | number
`numberOfRetries` | number

## Example

```typescript
import type { StacApiRetries } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "exponentialBackoffFactor": null,
  "initialDelayMs": null,
  "numberOfRetries": null,
} satisfies StacApiRetries

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StacApiRetries
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


