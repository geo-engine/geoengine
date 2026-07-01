
# MlTensorShape3D

A struct describing tensor shape for `MlModelMetadata`

## Properties

Name | Type
------------ | -------------
`y` | number
`x` | number
`bands` | number

## Example

```typescript
import type { MlTensorShape3D } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "y": null,
  "x": null,
  "bands": null,
} satisfies MlTensorShape3D

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MlTensorShape3D
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


