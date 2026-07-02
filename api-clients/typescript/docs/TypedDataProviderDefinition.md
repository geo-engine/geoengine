
# TypedDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`type` | string
`id` | string
`name` | string
`description` | string
`priority` | number
`apiUrl` | string
`projectId` | string
`apiToken` | string
`filterLabel` | string
`cacheTtl` | number
`stacUrl` | string
`s3Url` | string
`s3AccessKey` | string
`s3SecretKey` | string
`gdalConfig` | Array&lt;Array&lt;string&gt;&gt;
`collections` | [Array&lt;DatasetLayerListingCollection&gt;](DatasetLayerListingCollection.md)
`baseUrl` | string
`data` | string
`overviews` | string
`vectorSpec` | [EdrVectorSpec](EdrVectorSpec.md)
`discreteVrs` | Array&lt;string&gt;
`provenance` | [Array&lt;Provenance&gt;](Provenance.md)
`dbConfig` | [DatabaseConnectionConfig](DatabaseConnectionConfig.md)
`autocompleteTimeout` | number
`columns` | Array&lt;string&gt;
`collectionApiUrl` | string
`collectionApiAuthToken` | string
`abcdDbConfig` | [DatabaseConnectionConfig](DatabaseConnectionConfig.md)
`pangaeaUrl` | string
`stacApiRetries` | [StacApiRetries](StacApiRetries.md)
`gdalRetries` | number
`queryBuffer` | [StacQueryBuffer](StacQueryBuffer.md)
`collectionName` | string
`s3Config` | [StacProviderS3Config](StacProviderS3Config.md)
`timeDimension` | [TimeDimension](TimeDimension.md)
`datasets` | [Array&lt;StacProviderDataset&gt;](StacProviderDataset.md)
`user` | string
`refreshToken` | string
`expiryDate` | Date

## Example

```typescript
import type { TypedDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "id": null,
  "name": null,
  "description": null,
  "priority": null,
  "apiUrl": null,
  "projectId": null,
  "apiToken": null,
  "filterLabel": null,
  "cacheTtl": null,
  "stacUrl": null,
  "s3Url": null,
  "s3AccessKey": null,
  "s3SecretKey": null,
  "gdalConfig": null,
  "collections": null,
  "baseUrl": null,
  "data": null,
  "overviews": null,
  "vectorSpec": null,
  "discreteVrs": null,
  "provenance": null,
  "dbConfig": null,
  "autocompleteTimeout": null,
  "columns": null,
  "collectionApiUrl": null,
  "collectionApiAuthToken": null,
  "abcdDbConfig": null,
  "pangaeaUrl": null,
  "stacApiRetries": null,
  "gdalRetries": null,
  "queryBuffer": null,
  "collectionName": null,
  "s3Config": null,
  "timeDimension": null,
  "datasets": null,
  "user": null,
  "refreshToken": null,
  "expiryDate": null,
} satisfies TypedDataProviderDefinition

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TypedDataProviderDefinition
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


