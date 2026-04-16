
# TaskStatusRunning


## Properties

Name | Type
------------ | -------------
`description` | string
`estimatedTimeRemaining` | string
`info` | any
`pctComplete` | string
`status` | string
`taskType` | string
`timeStarted` | string

## Example

```typescript
import type { TaskStatusRunning } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "description": null,
  "estimatedTimeRemaining": null,
  "info": null,
  "pctComplete": null,
  "status": null,
  "taskType": null,
  "timeStarted": null,
} satisfies TaskStatusRunning

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TaskStatusRunning
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


