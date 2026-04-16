
# Project


## Properties

Name | Type
------------ | -------------
`bounds` | [STRectangle](STRectangle.md)
`description` | string
`id` | string
`layers` | [Array&lt;ProjectLayer&gt;](ProjectLayer.md)
`name` | string
`plots` | [Array&lt;Plot&gt;](Plot.md)
`timeStep` | [TimeStep](TimeStep.md)
`version` | [ProjectVersion](ProjectVersion.md)

## Example

```typescript
import type { Project } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "bounds": null,
  "description": null,
  "id": null,
  "layers": null,
  "name": null,
  "plots": null,
  "timeStep": null,
  "version": null,
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


