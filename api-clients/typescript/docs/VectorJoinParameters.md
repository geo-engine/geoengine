
# VectorJoinParameters


## Properties

Name | Type
------------ | -------------
`type` | string
`leftColumn` | string
`rightColumn` | string
`rightColumnSuffix` | string

## Example

```typescript
import type { VectorJoinParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "leftColumn": null,
  "rightColumn": null,
  "rightColumnSuffix": null,
} satisfies VectorJoinParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as VectorJoinParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


