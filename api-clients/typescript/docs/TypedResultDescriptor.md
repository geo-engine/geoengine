
# TypedResultDescriptor


## Properties

Name | Type
------------ | -------------
`spatialReference` | string
`time` | [TimeInterval](TimeInterval.md)
`bbox` | [BoundingBox2D](BoundingBox2D.md)
`type` | string
`dataType` | [VectorDataType](VectorDataType.md)
`spatialGrid` | [SpatialGridDescriptor](SpatialGridDescriptor.md)
`bands` | [Array&lt;RasterBandDescriptor&gt;](RasterBandDescriptor.md)
`columns` | [{ [key: string]: VectorColumnInfo; }](VectorColumnInfo.md)

## Example

```typescript
import type { TypedResultDescriptor } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "spatialReference": null,
  "time": null,
  "bbox": null,
  "type": null,
  "dataType": null,
  "spatialGrid": null,
  "bands": null,
  "columns": null,
} satisfies TypedResultDescriptor

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TypedResultDescriptor
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


