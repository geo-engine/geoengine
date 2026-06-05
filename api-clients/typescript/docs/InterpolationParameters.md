
# InterpolationParameters

Parameters for the `Interpolation` operator.

## Properties

Name | Type
------------ | -------------
`interpolation` | [InterpolationMethod](InterpolationMethod.md)
`outputResolution` | [InterpolationResolution](InterpolationResolution.md)
`outputOriginReference` | [Coordinate2D](Coordinate2D.md)

## Example

```typescript
import type { InterpolationParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "interpolation": null,
  "outputResolution": null,
  "outputOriginReference": null,
} satisfies InterpolationParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as InterpolationParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


