
# PolygonRasterizationTimeRegular

Regular time slices that contain all features whose time intersects them.

## Properties

Name | Type
------------ | -------------
`type` | string
`step` | [TimeStep](TimeStep.md)
`reference` | number

## Example

```typescript
import type { PolygonRasterizationTimeRegular } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "step": null,
  "reference": null,
} satisfies PolygonRasterizationTimeRegular

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PolygonRasterizationTimeRegular
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


