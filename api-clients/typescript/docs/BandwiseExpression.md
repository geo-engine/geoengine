
# BandwiseExpression

The `BandwiseExpression` operator performs a pixel-wise mathematical expression on each band of a raster source. For more details on the expression syntax, see the `Expression` operator. Note that in the `BandwiseExpression` operator it is only possible to map one pixel value to another and not reference any other pixels or bands. The variable name for the pixel value is `x`.  ## Errors  The parsing of the expression can fail if there are, for example, syntax errors.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [BandwiseExpressionParameters](BandwiseExpressionParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { BandwiseExpression } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies BandwiseExpression

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as BandwiseExpression
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


