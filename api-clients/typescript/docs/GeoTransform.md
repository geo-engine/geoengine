
# GeoTransform


## Properties

Name | Type
------------ | -------------
`originCoordinate` | [Coordinate2D](Coordinate2D.md)
`xPixelSize` | number
`yPixelSize` | number

## Example

```typescript
import type { GeoTransform } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "originCoordinate": null,
  "xPixelSize": null,
  "yPixelSize": null,
} satisfies GeoTransform

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as GeoTransform
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


