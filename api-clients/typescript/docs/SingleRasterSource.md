
# SingleRasterSource

A single raster operator as a source for this operator.

## Properties

Name | Type
------------ | -------------
`raster` | [RasterOperator](RasterOperator.md)

## Example

```typescript
import type { SingleRasterSource } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "raster": null,
} satisfies SingleRasterSource

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SingleRasterSource
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


