
# LineSimplification

The `LineSimplification` operator allows simplifying `FeatureCollection`s of (multi-)lines or (multi-)polygons by removing vertices. Users can select a simplification algorithm and specify an `epsilon` for parametrization. Alternatively, they can omit the `epsilon`, which results in the epsilon being automatically determined by the query\'s spatial resolution.  ## Errors  - If `epsilon` is set but <= 0, an error is thrown. - If the input is not a `MultiPolygon` or `MultiLineString`, an error is thrown.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [LineSimplificationParameters](LineSimplificationParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { LineSimplification } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies LineSimplification

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LineSimplification
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


