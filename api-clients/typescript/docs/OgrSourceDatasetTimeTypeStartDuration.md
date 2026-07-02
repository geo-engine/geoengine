
# OgrSourceDatasetTimeTypeStartDuration


## Properties

Name | Type
------------ | -------------
`type` | string
`startField` | string
`startFormat` | [OgrSourceTimeFormat](OgrSourceTimeFormat.md)
`durationField` | string

## Example

```typescript
import type { OgrSourceDatasetTimeTypeStartDuration } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "startField": null,
  "startFormat": null,
  "durationField": null,
} satisfies OgrSourceDatasetTimeTypeStartDuration

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OgrSourceDatasetTimeTypeStartDuration
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


