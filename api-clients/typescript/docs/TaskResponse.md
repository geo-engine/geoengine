
# TaskResponse

Create a task somewhere and respond with a task id to query the task status.

## Properties

Name | Type
------------ | -------------
`taskId` | string

## Example

```typescript
import type { TaskResponse } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "taskId": null,
} satisfies TaskResponse

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TaskResponse
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


