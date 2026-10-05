
# GdalMdMetaData


## Properties

Name | Type
------------ | -------------
`type` | string
`resultDescriptor` | [RasterResultDescriptor](RasterResultDescriptor.md)
`zRole` | [ZRole](ZRole.md)
`wrap` | boolean
`maxZBatchSize` | number

## Example

```typescript
import type { GdalMdMetaData } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "resultDescriptor": null,
  "zRole": null,
  "wrap": null,
  "maxZBatchSize": null,
} satisfies GdalMdMetaData

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalMdMetaData
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


