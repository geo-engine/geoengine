
# ColumnRangeFilter

The `ColumnRangeFilter` operator allows filtering `FeatureCollection`s. Users can define one or more data ranges for a column in the data table that is then filtered. The filter can be used for numerical as well as textual columns. Each range is inclusive, i.e. `[start, end]` includes both the `start` and the `end` values.  ## Errors  If the value in the `column` parameter is not a column of the feature collection, an error is thrown.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [ColumnRangeFilterParameters](ColumnRangeFilterParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { ColumnRangeFilter } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies ColumnRangeFilter

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ColumnRangeFilter
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


