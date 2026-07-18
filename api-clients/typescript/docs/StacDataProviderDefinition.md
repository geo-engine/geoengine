
# StacDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`name` | string
`id` | string
`description` | string
`priority` | number
`apiUrl` | string
`collectionName` | string
`s3Config` | [StacProviderS3Config](StacProviderS3Config.md)
`timeDimension` | [TimeDimension](TimeDimension.md)
`datasets` | [Array&lt;StacProviderDataset&gt;](StacProviderDataset.md)

## Example

```typescript
import type { StacDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "name": null,
  "id": null,
  "description": null,
  "priority": null,
  "apiUrl": null,
  "collectionName": null,
  "s3Config": null,
  "timeDimension": null,
  "datasets": null,
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


