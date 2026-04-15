
# HistogramBuckets


## Properties

Name | Type
------------ | -------------
`type` | string
`value` | number
`maxNumberOfBuckets` | number

## Example

```typescript
import type { HistogramBuckets } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "value": null,
  "maxNumberOfBuckets": null,
} satisfies HistogramBuckets

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as HistogramBuckets
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


