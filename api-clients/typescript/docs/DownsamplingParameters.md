
# DownsamplingParameters

Parameters for the `Downsampling` operator.

## Properties

Name | Type
------------ | -------------
`samplingMethod` | [DownsamplingMethod](DownsamplingMethod.md)
`outputResolution` | [DownsamplingResolution](DownsamplingResolution.md)
`outputOriginReference` | [Coordinate2D](Coordinate2D.md)

## Example

```typescript
import type { DownsamplingParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "samplingMethod": null,
  "outputResolution": null,
  "outputOriginReference": null,
} satisfies DownsamplingParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DownsamplingParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


