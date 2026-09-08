
# RasterScalingParameters


## Properties

Name | Type
------------ | -------------
`slope` | [SlopeOffsetSelection](SlopeOffsetSelection.md)
`offset` | [SlopeOffsetSelection](SlopeOffsetSelection.md)
`outputMeasurement` | [Measurement](Measurement.md)
`scalingMode` | [ScalingMode](ScalingMode.md)

## Example

```typescript
import type { RasterScalingParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "slope": null,
  "offset": null,
  "outputMeasurement": null,
  "scalingMode": null,
} satisfies RasterScalingParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterScalingParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


