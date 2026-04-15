
# SingleBandRasterColorizer


## Properties

Name | Type
------------ | -------------
`band` | number
`bandColorizer` | [Colorizer](Colorizer.md)
`type` | string

## Example

```typescript
import type { SingleBandRasterColorizer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "band": null,
  "bandColorizer": null,
  "type": null,
} satisfies SingleBandRasterColorizer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SingleBandRasterColorizer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


