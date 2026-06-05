
# DataUsage


## Properties

Name | Type
------------ | -------------
`timestamp` | Date
`userId` | string
`computationId` | string
`data` | string
`count` | number

## Example

```typescript
import type { DataUsage } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "timestamp": null,
  "userId": null,
  "computationId": null,
  "data": null,
  "count": null,
} satisfies DataUsage

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DataUsage
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


