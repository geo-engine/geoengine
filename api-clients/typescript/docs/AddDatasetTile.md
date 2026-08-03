
# AddDatasetTile


## Properties

Name | Type
------------ | -------------
`time` | [TimeInterval](TimeInterval.md)
`spatialPartition` | [SpatialPartition2D](SpatialPartition2D.md)
`band` | number
`zIndex` | number
`params` | [GdalDatasetParameters](GdalDatasetParameters.md)

## Example

```typescript
import type { AddDatasetTile } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "time": null,
  "spatialPartition": null,
  "band": null,
  "zIndex": null,
  "params": null,
} satisfies AddDatasetTile

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AddDatasetTile
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


