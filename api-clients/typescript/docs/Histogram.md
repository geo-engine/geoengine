
# Histogram

The `Histogram` is a _plot operator_ that computes a histogram plot either over attributes of a vector dataset or values of a raster source. The output is a plot in [Vega-Lite](https://vega.github.io/vega-lite/) specification.  For instance, you want to plot the data distribution of numeric attributes of a feature collection. Then you can use a histogram with a suitable number of buckets to visualize and assess this.  ## Errors  The operator returns an error if the selected column (`columnName`) does not exist or is not numeric.  ## Notes  If `bounds` or `buckets` are not defined, the operator will determine these values by itself which requires processing the data twice.  If the `buckets` parameter is set to `squareRootChoiceRule`, the operator estimates it using the square root of the number of elements in the data. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [HistogramParameters](HistogramParameters.md)
`sources` | [SingleVectorOrRasterSource](SingleVectorOrRasterSource.md)

## Example

```typescript
import type { Histogram } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies Histogram

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Histogram
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


