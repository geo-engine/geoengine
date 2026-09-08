
# BandNeighborhoodAggregate

The `BandNeighborhoodAggregate` operator performs a pixel-wise aggregate function over neighboring bands. The output is a raster time series with the same number of bands as the input raster. The pixel values are replaced by the result of the aggregate function. This allows, for example, the computation of a moving average over the bands of a raster time series.  ## Types  The following describes the types used in the parameters.  ### NeighborhoodAggregate  There are several types of neighborhood aggregate functions.  #### Average  This aggregate function computes the average of the neighboring bands. The `windowSize` parameter defines the number of bands to consider for the average and must be an odd number. For the borders, the window is reduced to the available bands.  #### `FirstDerivative`  This aggregate function computes an approximation of the first derivative of the neighboring bands using the central difference method. To compute the distance between neighboring bands, a `bandDistance` parameter is required.  ## Errors  The operation fails if there are not enough bands in the input raster to compute the aggregate function or the number of bands does not match the requirements of the aggregate function.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [BandNeighborhoodAggregateParameters](BandNeighborhoodAggregateParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { BandNeighborhoodAggregate } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies BandNeighborhoodAggregate

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as BandNeighborhoodAggregate
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


