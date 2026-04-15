
# TypedResultDescriptor


## Properties

Name | Type
------------ | -------------
`bbox` | [BoundingBox2D](BoundingBox2D.md)
`spatialReference` | string
`time` | [TimeInterval](TimeInterval.md)
`type` | string
`bands` | [Array&lt;RasterBandDescriptor&gt;](RasterBandDescriptor.md)
`dataType` | [VectorDataType](VectorDataType.md)
`spatialGrid` | [SpatialGridDescriptor](SpatialGridDescriptor.md)
`columns` | [{ [key: string]: VectorColumnInfo; }](VectorColumnInfo.md)

## Example

```typescript
import type { TypedResultDescriptor } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "bbox": null,
  "spatialReference": null,
  "time": null,
  "type": null,
  "bands": null,
  "dataType": null,
  "spatialGrid": null,
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


