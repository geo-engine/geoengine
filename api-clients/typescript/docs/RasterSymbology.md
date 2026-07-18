
# RasterSymbology


## Properties

Name | Type
------------ | -------------
`type` | string
`opacity` | number
`rasterColorizer` | [RasterColorizer](RasterColorizer.md)

## Example

```typescript
import type { RasterSymbology } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "opacity": null,
  "rasterColorizer": null,
} satisfies RasterSymbology

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterSymbology
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


