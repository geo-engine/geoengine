
# ServerInfo


## Properties

Name | Type
------------ | -------------
`buildDate` | string
`commitHash` | string
`features` | string
`version` | string

## Example

```typescript
import type { ServerInfo } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "buildDate": null,
  "commitHash": null,
  "features": null,
  "version": null,
} satisfies ServerInfo

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ServerInfo
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


