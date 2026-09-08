
# TimeShiftParameters

Time shifts can be expressed either as a relative offset or as a fixed absolute time interval.

## Properties

Name | Type
------------ | -------------
`type` | string
`granularity` | [TimeGranularity](TimeGranularity.md)
`value` | number
`timeInterval` | [TimeInterval](TimeInterval.md)

## Example

```typescript
import type { TimeShiftParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "granularity": null,
  "value": null,
  "timeInterval": null,
} satisfies TimeShiftParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TimeShiftParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


