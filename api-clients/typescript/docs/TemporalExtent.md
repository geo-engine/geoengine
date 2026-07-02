
# TemporalExtent

The temporal extent of the features in the collection.

## Properties

Name | Type
------------ | -------------
`interval` | Array&lt;Array&lt;Date | null&gt;&gt;
`trs` | string

## Example

```typescript
import type { TemporalExtent } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "interval": null,
  "trs": null,
} satisfies TemporalExtent

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TemporalExtent
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


