
# UserRegistration


## Properties

Name | Type
------------ | -------------
`email` | string
`password` | string
`realName` | string

## Example

```typescript
import type { UserRegistration } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "email": null,
  "password": null,
  "realName": null,
} satisfies UserRegistration

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as UserRegistration
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


