
# Style


## Properties

Name | Type
------------ | -------------
`id` | string
`title` | string
`description` | string
`keywords` | Array&lt;string&gt;
`links` | [Array&lt;Link&gt;](Link.md)

## Example

```typescript
import type { Style } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "title": null,
  "description": null,
  "keywords": null,
  "links": null,
} satisfies Style

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Style
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


