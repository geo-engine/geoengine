
# SentinelS2L2ACogsProviderDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`name` | string
`id` | string
`description` | string
`priority` | number
`apiUrl` | string
`stacApiRetries` | [StacApiRetries](StacApiRetries.md)
`gdalRetries` | number
`cacheTtl` | number
`queryBuffer` | [StacQueryBuffer](StacQueryBuffer.md)

## Example

```typescript
import type { SentinelS2L2ACogsProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "name": null,
  "id": null,
  "description": null,
  "priority": null,
  "apiUrl": null,
  "stacApiRetries": null,
  "gdalRetries": null,
  "cacheTtl": null,
  "queryBuffer": null,
} satisfies SentinelS2L2ACogsProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SentinelS2L2ACogsProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


