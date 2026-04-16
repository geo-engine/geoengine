
# MultipleRasterSources

One or more raster operators as sources for this operator.

## Properties

Name | Type
------------ | -------------
`rasters` | [Array&lt;RasterOperator&gt;](RasterOperator.md)

## Example

```typescript
import type { MultipleRasterSources } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "rasters": null,
} satisfies MultipleRasterSources

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MultipleRasterSources
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


