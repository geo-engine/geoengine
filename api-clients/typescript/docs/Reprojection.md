
# Reprojection

The `Reprojection` operator reprojects data from one spatial reference system to another. It accepts exactly one input which can either be a raster or a vector data stream. The operator produces all data that, after reprojection, is contained in the query rectangle.  ## Data Type Specifics  The concrete behavior depends on the data type.  ### Vector Data  The operator reprojects all coordinates of the features individually. The result contains all features that, after reprojection, are intersected by the query rectangle.  ### Raster Data  To create tiles in the target projection, the operator loads corresponding tiles in the source projection. For each output pixel, the value of the nearest input pixel is used.  If parts of a tile are outside of the source extent after projection, the operator produces NO DATA values.  ## Inputs  The `Reprojection` operator expects exactly one _raster_ or _vector_ input.  ## Errors  The operator returns an error if the target projection is unknown or if input data cannot be reprojected.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [ReprojectionParameters](ReprojectionParameters.md)
`sources` | [SingleRasterOrVectorSource](SingleRasterOrVectorSource.md)

## Example

```typescript
import type { Reprojection } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies Reprojection

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Reprojection
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


