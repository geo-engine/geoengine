
# ReflectanceParameters


## Properties

Name | Type
------------ | -------------
`solarCorrection` | boolean
`forceHRV` | boolean
`forceSatellite` | number

## Example

```typescript
import type { ReflectanceParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "solarCorrection": null,
  "forceHRV": null,
  "forceSatellite": null,
} satisfies ReflectanceParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ReflectanceParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


