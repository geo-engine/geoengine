
# BoxPlot

The `BoxPlot` is a _plot operator_ that computes a box plot over  - a selection of numerical columns of a single vector dataset, or - multiple raster datasets.  Thereby, the operator considers all data in the given query rectangle.  The boxes of the plot span the 1st and 3rd quartile and highlight the median. The whiskers indicate the minimum and maximum values of the corresponding attribute or raster.  ## Errors  The operator returns an error in the following cases.  - Vector data: The `attribute` for one of the given `columnNames` is not numeric. - Vector data: The `attribute` for one of the given `columnNames` does not exist. - Raster data: The length of the `columnNames` parameter does not match the number of input rasters.  ## Notes  If your dataset contains `infinite` or `NAN` values, they are ignored for the computation. Moreover, if your dataset contains more than `10.000`values (which is likely for rasters), the median and quartiles are estimated using the P^2 algorithm described in:  R. Jain and I. Chlamtac, The P^2 algorithm for dynamic calculation of quantiles and histograms without storing observations, Communications of the ACM, Volume 28 (October), Number 10, 1985, p. 1076-1085. <https://www.cse.wustl.edu/~jain/papers/ftp/psqr.pdf>

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [BoxPlotParameters](BoxPlotParameters.md)
`sources` | [MultipleRasterOrSingleVectorSource](MultipleRasterOrSingleVectorSource.md)

## Example

```typescript
import type { BoxPlot } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies BoxPlot

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as BoxPlot
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


