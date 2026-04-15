
# GdalMetadataMapping


## Properties

Name | Type
------------ | -------------
`sourceKey` | [RasterPropertiesKey](RasterPropertiesKey.md)
`targetKey` | [RasterPropertiesKey](RasterPropertiesKey.md)
`targetType` | [RasterPropertiesEntryType](RasterPropertiesEntryType.md)

## Example

```typescript
import type { GdalMetadataMapping } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "sourceKey": null,
  "targetKey": null,
  "targetType": null,
} satisfies GdalMetadataMapping

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GdalMetadataMapping
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


