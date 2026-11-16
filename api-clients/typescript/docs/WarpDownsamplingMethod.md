
# WarpDownsamplingMethod

How `Warp` computes an output pixel that is coarser than the input in at least one axis, as `gdalwarp -r` with the same name. Input pixels without data are left out.

## Properties

Name | Type
------------ | -------------

## Example

```typescript
import type { WarpDownsamplingMethod } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
} satisfies WarpDownsamplingMethod

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as WarpDownsamplingMethod
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


