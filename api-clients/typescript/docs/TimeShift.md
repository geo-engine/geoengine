
# TimeShift

The `TimeShift` operator allows retrieving data temporally relative to the actual [`QueryRectangle`](../datatypes/queryrectangle). It shifts the query rectangle by a given amount of time and modifies the result data accordingly. Users have two options for specifying the time shift:  1. Relative shift – shift relatively to the query rectangle, e.g., one month or one year to the past.    This can be useful for comparing multiple points in time relative to the query rectangle. 2. Absolute shift – change query rectangle to a fixed temporal reference, e.g., January 2014.    This can be used to compare data in the query rectangle\'s time to a fixed point of reference.  The output is either a stream of raster data or a stream of vector data depending on the input.  An example usage scenario is to compare the current time with the previous time of the same raster data. For instance, a raster source outputs monthly data aggregates of mean temperatures. If you want to compute the difference between the current month and the previous month, you can use the `TimeShift` operator. You will have two workflows. One is the unmodified temperature raster source. The other is the same source, shifted by one month. Then, you can use both workflows as sources of an [`Expression`](/docs/operators/expression) operator.  _Note_: This operator modifies the time values of the returned data. For rasters and vector data, it shifts the time intervals opposite to the time shift specified in the operator. This is necessary to have only data inside the result that is part of the [`QueryRectangle`\'s](../datatypes/queryrectangle) time interval. As an example, we shift monthly data by one month to the past. Our query rectangle points to February. Then, the operator shifts the query rectangle to January. The data, originally valid for January, is shifted forward to February again, to fit into the original query rectangle, which is February. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [TimeShiftParameters](TimeShiftParameters.md)
`sources` | [SingleRasterOrVectorSource](SingleRasterOrVectorSource.md)

## Example

```typescript
import type { TimeShift } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies TimeShift

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TimeShift
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


