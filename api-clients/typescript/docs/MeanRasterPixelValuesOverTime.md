
# MeanRasterPixelValuesOverTime

The `MeanRasterPixelValuesOverTime` is a _plot operator_ that computes a time series plot of mean raster values.  For each time step in the raster time series, it computes one mean value per band. The output is a plot in Vega-Lite specification. For a single band, it is an area or line plot. For multiple bands, it is a line plot with one line per band.  For instance, you want to plot the mean temperature of a monthly raster time series. Then, you can use this operator to generate a time series plot.  ## Errors  The operator returns an error if one of the selected bands (`attributes`) does not exist.  ## Notes  If the selected bands have different [`Measurement`](../datatypes/measurement)s, the y-axis has no unit.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [MeanRasterPixelValuesOverTimeParameters](MeanRasterPixelValuesOverTimeParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { MeanRasterPixelValuesOverTime } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies MeanRasterPixelValuesOverTime

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MeanRasterPixelValuesOverTime
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


