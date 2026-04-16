
# PermissionListing


## Properties

Name | Type
------------ | -------------
`permission` | [Permission](Permission.md)
`resource` | [Resource](Resource.md)
`role` | [Role](Role.md)

## Example

```typescript
import type { PermissionListing } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "permission": null,
  "resource": null,
  "role": null,
} satisfies PermissionListing

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PermissionListing
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


