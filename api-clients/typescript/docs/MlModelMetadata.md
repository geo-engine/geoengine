
# MlModelMetadata


## Properties

Name | Type
------------ | -------------
`inputNoDataHandling` | [MlModelInputNoDataHandling](MlModelInputNoDataHandling.md)
`inputShape` | [MlTensorShape3D](MlTensorShape3D.md)
`inputType` | [RasterDataType](RasterDataType.md)
`outputNoDataHandling` | [MlModelOutputNoDataHandling](MlModelOutputNoDataHandling.md)
`outputShape` | [MlTensorShape3D](MlTensorShape3D.md)
`outputType` | [RasterDataType](RasterDataType.md)

## Example

```typescript
import type { MlModelMetadata } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "inputNoDataHandling": null,
  "inputShape": null,
  "inputType": null,
  "outputNoDataHandling": null,
  "outputShape": null,
  "outputType": null,
} satisfies MlModelMetadata

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MlModelMetadata
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


