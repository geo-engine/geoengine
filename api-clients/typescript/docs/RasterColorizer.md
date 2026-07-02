
# RasterColorizer


## Properties

Name | Type
------------ | -------------
`type` | string
`band` | number
`bandColorizer` | [Colorizer](Colorizer.md)
`redBand` | number
`redMin` | number
`redMax` | number
`redScale` | number
`greenBand` | number
`greenMin` | number
`greenMax` | number
`greenScale` | number
`blueBand` | number
`blueMin` | number
`blueMax` | number
`blueScale` | number
`noDataColor` | Array&lt;number&gt;

## Example

```typescript
import type { RasterColorizer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "band": null,
  "bandColorizer": null,
  "redBand": null,
  "redMin": null,
  "redMax": null,
  "redScale": null,
  "greenBand": null,
  "greenMin": null,
  "greenMax": null,
  "greenScale": null,
  "blueBand": null,
  "blueMin": null,
  "blueMax": null,
  "blueScale": null,
  "noDataColor": null,
} satisfies RasterColorizer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as RasterColorizer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


