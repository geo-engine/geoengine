
# SpatialExtent

The spatial extent of the features in the collection.

## Properties

Name | Type
------------ | -------------
`bbox` | Array&lt;Array&lt;number&gt;&gt;
`crs` | string

## Example

```typescript
import type { SpatialExtent } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "bbox": null,
  "crs": null,
} satisfies SpatialExtent

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SpatialExtent
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


