
# AddDatasetMdTile

One MD array file of an `MdGdalSource` dataset, covering all of that file\'s z slices.

## Properties

Name | Type
------------ | -------------
`spatialPartition` | [SpatialPartition2D](SpatialPartition2D.md)
`band` | number
`zIndex` | number
`arrayName` | string
`arrayGroup` | string
`timeDescriptor` | [TimeDescriptor](TimeDescriptor.md)
`timeSteps` | [Array&lt;TimeInterval&gt;](TimeInterval.md)
`params` | [GdalDatasetParameters](GdalDatasetParameters.md)

## Example

```typescript
import type { AddDatasetMdTile } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "spatialPartition": null,
  "band": null,
  "zIndex": null,
  "arrayName": null,
  "arrayGroup": null,
  "timeDescriptor": null,
  "timeSteps": null,
  "params": null,
} satisfies AddDatasetMdTile

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AddDatasetMdTile
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


