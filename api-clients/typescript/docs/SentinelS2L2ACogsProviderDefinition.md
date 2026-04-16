
# SentinelS2L2ACogsProviderDefinition


## Properties

Name | Type
------------ | -------------
`apiUrl` | string
`cacheTtl` | number
`description` | string
`gdalRetries` | number
`id` | string
`name` | string
`priority` | number
`queryBuffer` | [StacQueryBuffer](StacQueryBuffer.md)
`stacApiRetries` | [StacApiRetries](StacApiRetries.md)
`type` | string

## Example

```typescript
import type { SentinelS2L2ACogsProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "apiUrl": null,
  "cacheTtl": null,
  "description": null,
  "gdalRetries": null,
  "id": null,
  "name": null,
  "priority": null,
  "queryBuffer": null,
  "stacApiRetries": null,
  "type": null,
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


