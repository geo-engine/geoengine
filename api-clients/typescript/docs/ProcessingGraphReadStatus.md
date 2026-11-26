
# ProcessingGraphReadStatus

A processing graph whose queries in the current session wait for reads that their data source rate limits. The reads are retried until they succeed, so its tiles arrive late, not never.

## Properties

Name | Type
------------ | -------------
`processingGraphId` | string
`throttledReads` | number
`throttledForSeconds` | number
`httpStatus` | number
`retryInSeconds` | number

## Example

```typescript
import type { ProcessingGraphReadStatus } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "processingGraphId": null,
  "throttledReads": null,
  "throttledForSeconds": null,
  "httpStatus": null,
  "retryInSeconds": null,
} satisfies ProcessingGraphReadStatus

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ProcessingGraphReadStatus
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


