
# Rasterization

The `Rasterization` operator creates a raster from a point vector source. It offers two options for rasterization: A grid rasterization and a (gaussian) density rasterization (heatmap).  ## Errors  If the `cutoff` is not in `[0, 1)` or the `stddev` is negative, an error will be thrown. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [RasterizationParameters](RasterizationParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { Rasterization } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies Rasterization

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Rasterization
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


