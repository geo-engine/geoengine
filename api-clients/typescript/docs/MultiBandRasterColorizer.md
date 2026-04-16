
# MultiBandRasterColorizer


## Properties

Name | Type
------------ | -------------
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
`type` | string

## Example

```typescript
import type { MultiBandRasterColorizer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
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
  "type": null,
} satisfies MultiBandRasterColorizer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MultiBandRasterColorizer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


