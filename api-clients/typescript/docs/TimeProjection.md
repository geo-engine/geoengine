
# TimeProjection

The `TimeProjection` projects vector dataset timestamps to new granularities and ranges. The output is a new vector dataset with the same geometry and attributes as the input. However, each time step is projected to a new time range. Moreover, the [`QueryRectangle`\'s](../datatypes/queryrectangle) temporal extent is enlarged as well to include the projected time range.  An example usage scenario is to transform snapshot observations into yearly time slices. For instance, animal occurrences are observed at a daily granularity. If you want to aggregate the data to a yearly granularity, you can use the `TimeProjection` operator. This will change the validity of each element in the dataset to the full year where it was observed. This is, for instance, useful when you want to combine it with raster time series and use different temporal semantics than the originally recorded validities.  ## Errors  If the `step` is negative, an error is thrown. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [TimeProjectionParameters](TimeProjectionParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { TimeProjection } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies TimeProjection

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TimeProjection
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


