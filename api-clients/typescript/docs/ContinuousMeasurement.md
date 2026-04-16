
# ContinuousMeasurement


## Properties

Name | Type
------------ | -------------
`measurement` | string
`type` | string
`unit` | string

## Example

```typescript
import type { ContinuousMeasurement } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "measurement": null,
  "type": null,
  "unit": null,
} satisfies ContinuousMeasurement

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ContinuousMeasurement
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


