
# Project


## Properties

Name | Type
------------ | -------------
`id` | string
`version` | [ProjectVersion](ProjectVersion.md)
`name` | string
`description` | string
`layers` | [Array&lt;ProjectLayer&gt;](ProjectLayer.md)
`plots` | [Array&lt;Plot&gt;](Plot.md)
`bounds` | [STRectangle](STRectangle.md)
`timeStep` | [TimeStep](TimeStep.md)

## Example

```typescript
import type { Project } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "version": null,
  "name": null,
  "description": null,
  "layers": null,
  "plots": null,
  "bounds": null,
  "timeStep": null,
} satisfies Project

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Project
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


