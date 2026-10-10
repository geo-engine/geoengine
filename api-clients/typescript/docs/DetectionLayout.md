
# DetectionLayout

How to interpret the raw output tensor of a detection model.

## Properties

Name | Type
------------ | -------------
`yoloBoxes` | [DetectionLayoutOneOfYoloBoxes](DetectionLayoutOneOfYoloBoxes.md)

## Example

```typescript
import type { DetectionLayout } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "yoloBoxes": null,
} satisfies DetectionLayout

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DetectionLayout
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


