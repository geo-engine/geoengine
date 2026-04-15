
# CreateProject


## Properties

Name | Type
------------ | -------------
`bounds` | [STRectangle](STRectangle.md)
`description` | string
`name` | string
`timeStep` | [TimeStep](TimeStep.md)

## Example

```typescript
import type { CreateProject } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "bounds": null,
  "description": null,
  "name": null,
  "timeStep": null,
} satisfies CreateProject

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as CreateProject
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


