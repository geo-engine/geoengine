
# CopernicusDataspaceDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`description` | string
`gdalConfig` | Array&lt;Array&lt;string&gt;&gt;
`id` | string
`name` | string
`priority` | number
`s3AccessKey` | string
`s3SecretKey` | string
`s3Url` | string
`stacUrl` | string
`type` | string

## Example

```typescript
import type { CopernicusDataspaceDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "description": null,
  "gdalConfig": null,
  "id": null,
  "name": null,
  "priority": null,
  "s3AccessKey": null,
  "s3SecretKey": null,
  "s3Url": null,
  "stacUrl": null,
  "type": null,
} satisfies CopernicusDataspaceDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as CopernicusDataspaceDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


