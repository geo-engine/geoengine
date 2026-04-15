
# OgrSourceTimeFormat


## Properties

Name | Type
------------ | -------------
`customFormat` | string
`format` | string
`timestampType` | [UnixTimeStampType](UnixTimeStampType.md)

## Example

```typescript
import type { OgrSourceTimeFormat } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "customFormat": null,
  "format": null,
  "timestampType": null,
} satisfies OgrSourceTimeFormat

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OgrSourceTimeFormat
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


