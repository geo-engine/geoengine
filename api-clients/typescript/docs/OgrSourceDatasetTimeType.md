
# OgrSourceDatasetTimeType


## Properties

Name | Type
------------ | -------------
`type` | string
`duration` | [OgrSourceDurationSpec](OgrSourceDurationSpec.md)
`startField` | string
`startFormat` | [OgrSourceTimeFormat](OgrSourceTimeFormat.md)
`endField` | string
`endFormat` | [OgrSourceTimeFormat](OgrSourceTimeFormat.md)
`durationField` | string

## Example

```typescript
import type { OgrSourceDatasetTimeType } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "duration": null,
  "startField": null,
  "startFormat": null,
  "endField": null,
  "endFormat": null,
  "durationField": null,
} satisfies OgrSourceDatasetTimeType

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OgrSourceDatasetTimeType
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


