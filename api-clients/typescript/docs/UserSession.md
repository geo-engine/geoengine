
# UserSession


## Properties

Name | Type
------------ | -------------
`created` | Date
`id` | string
`project` | string
`roles` | Array&lt;string&gt;
`user` | [UserInfo](UserInfo.md)
`validUntil` | Date
`view` | [STRectangle](STRectangle.md)

## Example

```typescript
import type { UserSession } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "created": null,
  "id": null,
  "project": null,
  "roles": null,
  "user": null,
  "validUntil": null,
  "view": null,
} satisfies UserSession

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as UserSession
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


