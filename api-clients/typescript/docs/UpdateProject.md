
# UpdateProject


## Properties

Name | Type
------------ | -------------
`bounds` | [STRectangle](STRectangle.md)
`description` | string
`id` | string
`layers` | [Array&lt;VecUpdate&gt;](VecUpdate.md)
`name` | string
`plots` | [Array&lt;VecUpdate&gt;](VecUpdate.md)
`timeStep` | [TimeStep](TimeStep.md)

## Example

```typescript
import type { UpdateProject } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "bounds": null,
  "description": null,
  "id": null,
  "layers": null,
  "name": null,
  "plots": null,
  "timeStep": null,
} satisfies UpdateProject

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as UpdateProject
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


