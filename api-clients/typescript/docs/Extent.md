
# Extent

The extent of the features in the collection. In the Core only spatial and temporal extents are specified. Extensions may add additional members to represent other extents, for example, thermal or pressure ranges.

## Properties

Name | Type
------------ | -------------
`spatial` | [SpatialExtent](SpatialExtent.md)
`temporal` | [TemporalExtent](TemporalExtent.md)

## Example

```typescript
import type { Extent } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "spatial": null,
  "temporal": null,
} satisfies Extent

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Extent
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


