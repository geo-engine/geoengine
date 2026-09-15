
# TimeShiftParametersAbsolute

If the `type` is `absolute`, you need to specify the following parameters:

## Properties

Name | Type
------------ | -------------
`type` | string
`timeInterval` | [TimeInterval](TimeInterval.md)

## Example

```typescript
import type { TimeShiftParametersAbsolute } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "timeInterval": null,
} satisfies TimeShiftParametersAbsolute

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TimeShiftParametersAbsolute
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


