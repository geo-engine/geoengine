
# OgrSourceDataset


## Properties

Name | Type
------------ | -------------
`fileName` | string
`layerName` | string
`dataType` | [VectorDataType](VectorDataType.md)
`time` | [OgrSourceDatasetTimeType](OgrSourceDatasetTimeType.md)
`defaultGeometry` | [TypedGeometry](TypedGeometry.md)
`columns` | [OgrSourceColumnSpec](OgrSourceColumnSpec.md)
`forceOgrTimeFilter` | boolean
`forceOgrSpatialFilter` | boolean
`onError` | [OgrSourceErrorSpec](OgrSourceErrorSpec.md)
`sqlQuery` | string
`attributeQuery` | string
`cacheTtl` | number

## Example

```typescript
import type { OgrSourceDataset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "fileName": null,
  "layerName": null,
  "dataType": null,
  "time": null,
  "defaultGeometry": null,
  "columns": null,
  "forceOgrTimeFilter": null,
  "forceOgrSpatialFilter": null,
  "onError": null,
  "sqlQuery": null,
  "attributeQuery": null,
  "cacheTtl": null,
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


