
# RasterFootprints

When and where the data of a raster processing graph may exist.

## Properties

Name | Type
------------ | -------------
`spatialReference` | string
`steps` | [Array&lt;TimeStepFootprint&gt;](TimeStepFootprint.md)
`next` | [TimeInterval](TimeInterval.md)
`tooLarge` | boolean

## Example

```typescript
import type { RasterFootprints } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "spatialReference": null,
  "steps": null,
  "next": null,
  "tooLarge": null,
} satisfies RasterFootprints

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterFootprints
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


