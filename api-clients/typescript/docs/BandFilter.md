
# BandFilter

The `BandFilter` operator selects bands from a raster source by band names or band indices.  It removes all non-selected bands while preserving the original order of remaining bands.  ## Inputs  The `BandFilter` operator expects exactly one _raster_ input.  ## Errors  The operator returns an error if no bands are selected or if selected band names/indices cannot be mapped to existing input bands.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [BandFilterParameters](BandFilterParameters.md)
`sources` | [SingleRasterSource](SingleRasterSource.md)

## Example

```typescript
import type { BandFilter } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies BandFilter

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as BandFilter
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


