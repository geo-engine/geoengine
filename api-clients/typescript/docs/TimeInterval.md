
# TimeInterval

Stores time intervals in ms in close-open semantic [start, end)

## Properties

Name | Type
------------ | -------------
`start` | number
`end` | number

## Example

```typescript
import type { TimeInterval } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "start": null,
  "end": null,
} satisfies TimeInterval

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TimeInterval
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


