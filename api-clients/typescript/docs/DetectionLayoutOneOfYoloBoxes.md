
# DetectionLayoutOneOfYoloBoxes

YOLO raw (pre-NMS) detect output, channel-major `[4(+1 obj), C, N]`. `objectness` selects YOLOv5/v6/v7 (`true`, objectness at channel 4) vs YOLOv8/v9/v10 (`false`, class scores start at channel 4).

## Properties

Name | Type
------------ | -------------
`objectness` | boolean

## Example

```typescript
import type { DetectionLayoutOneOfYoloBoxes } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "objectness": null,
} satisfies DetectionLayoutOneOfYoloBoxes

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DetectionLayoutOneOfYoloBoxes
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


