
# StacDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`apiUrl` | string
`collectionName` | string
`datasets` | [Array&lt;StacProviderDataset&gt;](StacProviderDataset.md)
`description` | string
`id` | string
`name` | string
`priority` | number
`s3Config` | [StacProviderS3Config](StacProviderS3Config.md)
`timeDimension` | [TimeDimension](TimeDimension.md)
`type` | string

## Example

```typescript
import type { StacDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "apiUrl": null,
  "collectionName": null,
  "datasets": null,
  "description": null,
  "id": null,
  "name": null,
  "priority": null,
  "s3Config": null,
  "timeDimension": null,
  "type": null,
} satisfies StacDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StacDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


