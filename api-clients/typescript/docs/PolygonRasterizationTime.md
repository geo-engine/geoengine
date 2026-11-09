
# PolygonRasterizationTime

How the time slices of the output raster are derived from the features.

## Properties

Name | Type
------------ | -------------
`type` | string
`step` | [TimeStep](TimeStep.md)
`reference` | number

## Example

```typescript
import type { PolygonRasterizationTime } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "step": null,
  "reference": null,
} satisfies PolygonRasterizationTime

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PolygonRasterizationTime
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


