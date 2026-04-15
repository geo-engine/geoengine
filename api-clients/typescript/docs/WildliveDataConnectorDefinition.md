
# WildliveDataConnectorDefinition


## Properties

Name | Type
------------ | -------------
`description` | string
`expiryDate` | Date
`id` | string
`name` | string
`priority` | number
`refreshToken` | string
`type` | string
`user` | string

## Example

```typescript
import type { WildliveDataConnectorDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "description": null,
  "expiryDate": null,
  "id": null,
  "name": null,
  "priority": null,
  "refreshToken": null,
  "type": null,
  "user": null,
} satisfies WildliveDataConnectorDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as WildliveDataConnectorDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


