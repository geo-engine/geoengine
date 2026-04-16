
# GfbioCollectionsDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`abcdDbConfig` | [DatabaseConnectionConfig](DatabaseConnectionConfig.md)
`cacheTtl` | number
`collectionApiAuthToken` | string
`collectionApiUrl` | string
`description` | string
`name` | string
`pangaeaUrl` | string
`priority` | number
`type` | string

## Example

```typescript
import type { GfbioCollectionsDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "abcdDbConfig": null,
  "cacheTtl": null,
  "collectionApiAuthToken": null,
  "collectionApiUrl": null,
  "description": null,
  "name": null,
  "pangaeaUrl": null,
  "priority": null,
  "type": null,
} satisfies GfbioCollectionsDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GfbioCollectionsDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


