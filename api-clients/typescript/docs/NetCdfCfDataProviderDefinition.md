
# NetCdfCfDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`name` | string
`description` | string
`priority` | number
`data` | string
`overviews` | string
`cacheTtl` | number

## Example

```typescript
import type { NetCdfCfDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "name": null,
  "description": null,
  "priority": null,
  "data": null,
  "overviews": null,
  "cacheTtl": null,
} satisfies NetCdfCfDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as NetCdfCfDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


