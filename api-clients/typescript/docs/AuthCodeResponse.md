
# AuthCodeResponse


## Properties

Name | Type
------------ | -------------
`sessionState` | string
`code` | string
`state` | string

## Example

```typescript
import type { AuthCodeResponse } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "sessionState": null,
  "code": null,
  "state": null,
} satisfies AuthCodeResponse

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AuthCodeResponse
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


