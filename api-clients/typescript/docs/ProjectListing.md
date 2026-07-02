
# ProjectListing


## Properties

Name | Type
------------ | -------------
`id` | string
`name` | string
`description` | string
`layerNames` | Array&lt;string&gt;
`plotNames` | Array&lt;string&gt;
`changed` | Date

## Example

```typescript
import type { ProjectListing } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "name": null,
  "description": null,
  "layerNames": null,
  "plotNames": null,
  "changed": null,
} satisfies ProjectListing

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ProjectListing
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


