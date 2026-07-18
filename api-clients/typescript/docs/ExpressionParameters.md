
# ExpressionParameters

## Types  The following describes the types used in the parameters.

## Properties

Name | Type
------------ | -------------
`expression` | string
`outputType` | [RasterDataType](RasterDataType.md)
`outputBand` | [RasterBandDescriptor](RasterBandDescriptor.md)
`mapNoData` | boolean

## Example

```typescript
import type { ExpressionParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "expression": null,
  "outputType": null,
  "outputBand": null,
  "mapNoData": null,
} satisfies ExpressionParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ExpressionParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


