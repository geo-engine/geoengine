
# TimeStepFootprint

Where the data of one time step may exist.

## Properties

Name | Type
------------ | -------------
`time` | [TimeInterval](TimeInterval.md)
`status` | [TimeStepFootprintStatus](TimeStepFootprintStatus.md)
`bounds` | [Array&lt;SpatialPartition2D&gt;](SpatialPartition2D.md)

## Example

```typescript
import type { TimeStepFootprint } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "time": null,
  "status": null,
  "bounds": null,
} satisfies TimeStepFootprint

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TimeStepFootprint
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


