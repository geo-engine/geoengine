
# NeighborhoodAggregate

The `NeighborhoodAggregate` operator computes an aggregate function for a pixel and its neighborhood. The operator can be defined as a neighborhood matrix with either weights or predefined shapes and an aggregate function. For each time step in the raster time series, the operator computes the aggregate for each pixel and its neighborhood.  ## Types  There are several types of neighborhoods. They define a matrix of weights. The rows and columns of this matrix must be odd.  ### `WeightsMatrix`  The weights matrix is defined as an n × m matrix of floating-point values. It is applied to the pixel and its neighborhood to serve as the input for the aggregate function.  ### Rectangle  The rectangle neighborhood is defined by its shape n × m. The result is a weights matrix with all weights set to `1.0`.  ### `AggregateFunction`  The aggregate function computes a single value from a set of values. Supported functions are `sum` and `standardDeviation`.  ## Errors  If the neighborhood rows or columns are not positive or odd, an error is thrown.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [NeighborhoodAggregateParameters](NeighborhoodAggregateParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { NeighborhoodAggregate } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies NeighborhoodAggregate

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as NeighborhoodAggregate
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


