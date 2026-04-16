
# Symbology


## Properties

Name | Type
------------ | -------------
`opacity` | number
`rasterColorizer` | [RasterColorizer](RasterColorizer.md)
`type` | string
`fillColor` | [ColorParam](ColorParam.md)
`radius` | [NumberParam](NumberParam.md)
`stroke` | [StrokeParam](StrokeParam.md)
`text` | [TextSymbology](TextSymbology.md)
`autoSimplified` | boolean

## Example

```typescript
import type { Symbology } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "opacity": null,
  "rasterColorizer": null,
  "type": null,
  "fillColor": null,
  "radius": null,
  "stroke": null,
  "text": null,
  "autoSimplified": null,
} satisfies Symbology

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Symbology
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


