
# PolygonSymbology


## Properties

Name | Type
------------ | -------------
`autoSimplified` | boolean
`fillColor` | [ColorParam](ColorParam.md)
`stroke` | [StrokeParam](StrokeParam.md)
`text` | [TextSymbology](TextSymbology.md)
`type` | string

## Example

```typescript
import type { PolygonSymbology } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "autoSimplified": null,
  "fillColor": null,
  "stroke": null,
  "text": null,
  "type": null,
} satisfies PolygonSymbology

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PolygonSymbology
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


