
# ArunaDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`apiToken` | string
`apiUrl` | string
`cacheTtl` | number
`description` | string
`filterLabel` | string
`id` | string
`name` | string
`priority` | number
`projectId` | string
`type` | string

## Example

```typescript
import type { ArunaDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "apiToken": null,
  "apiUrl": null,
  "cacheTtl": null,
  "description": null,
  "filterLabel": null,
  "id": null,
  "name": null,
  "priority": null,
  "projectId": null,
  "type": null,
} satisfies ArunaDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ArunaDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


