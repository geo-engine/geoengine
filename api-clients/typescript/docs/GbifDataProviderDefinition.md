
# GbifDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`autocompleteTimeout` | number
`cacheTtl` | number
`columns` | Array&lt;string&gt;
`dbConfig` | [DatabaseConnectionConfig](DatabaseConnectionConfig.md)
`description` | string
`name` | string
`priority` | number
`type` | string

## Example

```typescript
import type { GbifDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "autocompleteTimeout": null,
  "cacheTtl": null,
  "columns": null,
  "dbConfig": null,
  "description": null,
  "name": null,
  "priority": null,
  "type": null,
} satisfies GbifDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GbifDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


