
# CopernicusDataspaceDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`name` | string
`description` | string
`id` | string
`stacUrl` | string
`s3Url` | string
`s3AccessKey` | string
`s3SecretKey` | string
`gdalConfig` | Array&lt;Array&lt;string&gt;&gt;
`priority` | number

## Example

```typescript
import type { CopernicusDataspaceDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "name": null,
  "description": null,
  "id": null,
  "stacUrl": null,
  "s3Url": null,
  "s3AccessKey": null,
  "s3SecretKey": null,
  "gdalConfig": null,
  "priority": null,
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


