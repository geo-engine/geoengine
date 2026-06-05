
# ReprojectionParameters

Parameters for the `Reprojection` operator.

## Properties

Name | Type
------------ | -------------
`targetSpatialReference` | string
`deriveOutSpec` | [DeriveOutRasterSpecsSource](DeriveOutRasterSpecsSource.md)

## Example

```typescript
import type { ReprojectionParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "targetSpatialReference": null,
  "deriveOutSpec": null,
} satisfies ReprojectionParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ReprojectionParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


