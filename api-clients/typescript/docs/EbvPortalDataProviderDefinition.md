
# EbvPortalDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`name` | string
`description` | string
`priority` | number
`baseUrl` | string
`data` | string
`overviews` | string
`cacheTtl` | number

## Example

```typescript
import type { EbvPortalDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "name": null,
  "description": null,
  "priority": null,
  "baseUrl": null,
  "data": null,
  "overviews": null,
  "cacheTtl": null,
} satisfies EbvPortalDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as EbvPortalDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


