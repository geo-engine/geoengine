
# UserInfo


## Properties

Name | Type
------------ | -------------
`email` | string
`id` | string
`realName` | string

## Example

```typescript
import type { UserInfo } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "email": null,
  "id": null,
  "realName": null,
} satisfies UserInfo

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as UserInfo
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


