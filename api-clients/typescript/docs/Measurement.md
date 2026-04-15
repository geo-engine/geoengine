
# Measurement


## Properties

Name | Type
------------ | -------------
`type` | string
`measurement` | string
`unit` | string
`classes` | { [key: string]: string; }

## Example

```typescript
import type { Measurement } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "measurement": null,
  "unit": null,
  "classes": null,
} satisfies Measurement

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Measurement
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


