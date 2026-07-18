
# StacApiRetries


## Properties

Name | Type
------------ | -------------
`numberOfRetries` | number
`initialDelayMs` | number
`exponentialBackoffFactor` | number

## Example

```typescript
import type { StacApiRetries } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "numberOfRetries": null,
  "initialDelayMs": null,
  "exponentialBackoffFactor": null,
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


