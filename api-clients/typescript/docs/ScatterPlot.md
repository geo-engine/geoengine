
# ScatterPlot

The `ScatterPlot` is a _plot operator_ that computes a scatter plot over two attributes of a vector dataset. Thereby, the operator considers all data in the given query rectangle.  In case of more than `500` points to plot, the representation changes from a regular scatter plot to a 2D Histogram with buckets determined from the underlying data.  ## Errors  The operator returns an error if one of the selected columns does not exist or is not numeric.  ## Notes  If your dataset contains `infinite` or `NAN` values, they are ignored for the computation. Moreover, if your dataset contains more than `10.000` values, the buckets of the histogram are generated based on those `10.000` values. Later values outside those bounds are ignored. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [ScatterPlotParameters](ScatterPlotParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { ScatterPlot } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies ScatterPlot

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ScatterPlot
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


