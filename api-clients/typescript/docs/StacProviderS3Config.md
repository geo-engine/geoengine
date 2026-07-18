
# StacProviderS3Config


## Properties

Name | Type
------------ | -------------
`endpoint` | string
`accessKey` | string
`secretKey` | string

## Example

```typescript
import type { StacProviderS3Config } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "endpoint": null,
  "accessKey": null,
  "secretKey": null,
} satisfies StacProviderS3Config

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as StacProviderS3Config
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


