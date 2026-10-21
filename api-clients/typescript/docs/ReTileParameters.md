
# ReTileParameters

Parameters for the `ReTile` operator.

## Properties

Name | Type
------------ | -------------
`tileSize` | Array&lt;number&gt;
`origin` | [Coordinate2D](Coordinate2D.md)

## Example

```typescript
import type { ReTileParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "tileSize": null,
  "origin": null,
} satisfies ReTileParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ReTileParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


