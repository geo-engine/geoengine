
# OnnxObjectDetection

The `OnnxObjectDetection` operator runs an object-detection model on a raster and emits the detections as polygons with a `class` and a `score` column.  ## Inputs  The `OnnxObjectDetection` operator expects exactly one _raster_ input. Its pixel size must match the model\'s input shape and its spatial resolution must match the resolution the model was trained at; neither is resampled.  ## Outputs  The output features carry the columns `class` (category) and `score` (float), and the time interval of the source tile that produced them.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [OnnxObjectDetectionParameters](OnnxObjectDetectionParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { OnnxObjectDetection } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies OnnxObjectDetection

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OnnxObjectDetection
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


