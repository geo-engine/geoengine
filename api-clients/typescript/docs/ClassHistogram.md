
# ClassHistogram

The `ClassHistogram` is a _plot operator_ that computes a histogram plot either over categorical attributes of a vector dataset or categorical values of a raster source. The output is a plot in [Vega-Lite](https://vega.github.io/vega-lite/) specification.  For instance, you want to plot the frequencies of the classes of a categorical attribute of a feature collection. Then you can use a class histogram to visualize and assess this.  ## Errors  The operator returns an error if…  - the selected column (`columnName`) does not exist or is not numeric, - the source is a raster and the property `columnName` is set, or - the input [`Measurement`](../datatypes/measurement) is not categorical.  The operator returns an error if  ## Notes  The operator only uses values of the categorical [`Measurement`](../datatypes/measurement). It ignores missing or no-data values and values that are not covered by the [`Measurement`](../datatypes/measurement). 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [ClassHistogramParameters](ClassHistogramParameters.md)
`sources` | [SingleRasterOrVectorSource](SingleRasterOrVectorSource.md)

## Example

```typescript
import type { ClassHistogram } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies ClassHistogram

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ClassHistogram
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


