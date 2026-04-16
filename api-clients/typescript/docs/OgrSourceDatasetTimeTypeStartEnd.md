
# OgrSourceDatasetTimeTypeStartEnd


## Properties

Name | Type
------------ | -------------
`endField` | string
`endFormat` | [OgrSourceTimeFormat](OgrSourceTimeFormat.md)
`startField` | string
`startFormat` | [OgrSourceTimeFormat](OgrSourceTimeFormat.md)
`type` | string

## Example

```typescript
import type { OgrSourceDatasetTimeTypeStartEnd } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "endField": null,
  "endFormat": null,
  "startField": null,
  "startFormat": null,
  "type": null,
} satisfies OgrSourceDatasetTimeTypeStartEnd

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OgrSourceDatasetTimeTypeStartEnd
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


