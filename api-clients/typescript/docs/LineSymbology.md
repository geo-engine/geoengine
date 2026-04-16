
# LineSymbology


## Properties

Name | Type
------------ | -------------
`autoSimplified` | boolean
`stroke` | [StrokeParam](StrokeParam.md)
`text` | [TextSymbology](TextSymbology.md)
`type` | string

## Example

```typescript
import type { LineSymbology } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "autoSimplified": null,
  "stroke": null,
  "text": null,
  "type": null,
} satisfies LineSymbology

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LineSymbology
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


