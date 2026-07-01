
# VectorResultDescriptor


## Properties

Name | Type
------------ | -------------
`dataType` | [VectorDataType](VectorDataType.md)
`spatialReference` | string
`columns` | [{ [key: string]: VectorColumnInfo; }](VectorColumnInfo.md)
`time` | [TimeInterval](TimeInterval.md)
`bbox` | [BoundingBox2D](BoundingBox2D.md)

## Example

```typescript
import type { VectorResultDescriptor } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "dataType": null,
  "spatialReference": null,
  "columns": null,
  "time": null,
  "bbox": null,
} satisfies VectorResultDescriptor

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as VectorResultDescriptor
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


