
# TypedDataProviderDefinition


## Properties

Name | Type
------------ | -------------
`apiToken` | string
`apiUrl` | string
`cacheTtl` | number
`description` | string
`filterLabel` | string
`id` | string
`name` | string
`priority` | number
`projectId` | string
`type` | string
`gdalConfig` | Array&lt;Array&lt;string&gt;&gt;
`s3AccessKey` | string
`s3SecretKey` | string
`s3Url` | string
`stacUrl` | string
`collections` | [Array&lt;DatasetLayerListingCollection&gt;](DatasetLayerListingCollection.md)
`baseUrl` | string
`data` | string
`overviews` | string
`discreteVrs` | Array&lt;string&gt;
`provenance` | [Array&lt;Provenance&gt;](Provenance.md)
`vectorSpec` | [EdrVectorSpec](EdrVectorSpec.md)
`autocompleteTimeout` | number
`columns` | Array&lt;string&gt;
`dbConfig` | [DatabaseConnectionConfig](DatabaseConnectionConfig.md)
`abcdDbConfig` | [DatabaseConnectionConfig](DatabaseConnectionConfig.md)
`collectionApiAuthToken` | string
`collectionApiUrl` | string
`pangaeaUrl` | string
`gdalRetries` | number
`queryBuffer` | [StacQueryBuffer](StacQueryBuffer.md)
`stacApiRetries` | [StacApiRetries](StacApiRetries.md)
`expiryDate` | Date
`refreshToken` | string
`user` | string

## Example

```typescript
import type { TypedDataProviderDefinition } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "apiToken": null,
  "apiUrl": null,
  "cacheTtl": null,
  "description": null,
  "filterLabel": null,
  "id": null,
  "name": null,
  "priority": null,
  "projectId": null,
  "type": null,
  "gdalConfig": null,
  "s3AccessKey": null,
  "s3SecretKey": null,
  "s3Url": null,
  "stacUrl": null,
  "collections": null,
  "baseUrl": null,
  "data": null,
  "overviews": null,
  "discreteVrs": null,
  "provenance": null,
  "vectorSpec": null,
  "autocompleteTimeout": null,
  "columns": null,
  "dbConfig": null,
  "abcdDbConfig": null,
  "collectionApiAuthToken": null,
  "collectionApiUrl": null,
  "pangaeaUrl": null,
  "gdalRetries": null,
  "queryBuffer": null,
  "stacApiRetries": null,
  "expiryDate": null,
  "refreshToken": null,
  "user": null,
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


