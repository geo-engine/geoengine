
# WildliveDataConnectorDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`id` | string
`name` | string
`description` | string
`user` | string
`refreshToken` | string
`expiryDate` | Date
`priority` | number

## Example

```typescript
import type { WildliveDataConnectorDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "id": null,
  "name": null,
  "description": null,
  "user": null,
  "refreshToken": null,
  "expiryDate": null,
  "priority": null,
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


