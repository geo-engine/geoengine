
# OgrSourceDataset


## Properties

Name | Type
------------ | -------------
`attributeQuery` | string
`cacheTtl` | number
`columns` | [OgrSourceColumnSpec](OgrSourceColumnSpec.md)
`dataType` | [VectorDataType](VectorDataType.md)
`defaultGeometry` | [TypedGeometry](TypedGeometry.md)
`fileName` | string
`forceOgrSpatialFilter` | boolean
`forceOgrTimeFilter` | boolean
`layerName` | string
`onError` | [OgrSourceErrorSpec](OgrSourceErrorSpec.md)
`sqlQuery` | string
`time` | [OgrSourceDatasetTimeType](OgrSourceDatasetTimeType.md)

## Example

```typescript
import type { OgrSourceDataset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "attributeQuery": null,
  "cacheTtl": null,
  "columns": null,
  "dataType": null,
  "defaultGeometry": null,
  "fileName": null,
  "forceOgrSpatialFilter": null,
  "forceOgrTimeFilter": null,
  "layerName": null,
  "onError": null,
  "sqlQuery": null,
  "time": null,
} satisfies OgrSourceDataset

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OgrSourceDataset
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


