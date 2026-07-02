
# PointSymbology


## Properties

Name | Type
------------ | -------------
`type` | string
`radius` | [NumberParam](NumberParam.md)
`fillColor` | [ColorParam](ColorParam.md)
`stroke` | [StrokeParam](StrokeParam.md)
`text` | [TextSymbology](TextSymbology.md)

## Example

```typescript
import type { PointSymbology } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "radius": null,
  "fillColor": null,
  "stroke": null,
  "text": null,
} satisfies PointSymbology

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PointSymbology
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


