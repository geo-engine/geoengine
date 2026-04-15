
# RasterColorizer


## Properties

Name | Type
------------ | -------------
`band` | number
`bandColorizer` | [Colorizer](Colorizer.md)
`type` | string
`blueBand` | number
`blueMax` | number
`blueMin` | number
`blueScale` | number
`greenBand` | number
`greenMax` | number
`greenMin` | number
`greenScale` | number
`noDataColor` | Array&lt;number&gt;
`redBand` | number
`redMax` | number
`redMin` | number
`redScale` | number

## Example

```typescript
import type { RasterColorizer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "band": null,
  "bandColorizer": null,
  "type": null,
  "blueBand": null,
  "blueMax": null,
  "blueMin": null,
  "blueScale": null,
  "greenBand": null,
  "greenMax": null,
  "greenMin": null,
  "greenScale": null,
  "noDataColor": null,
  "redBand": null,
  "redMax": null,
  "redMin": null,
  "redScale": null,
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


