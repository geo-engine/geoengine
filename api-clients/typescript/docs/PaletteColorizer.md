
# PaletteColorizer


## Properties

Name | Type
------------ | -------------
`type` | string
`colors` | { [key: string]: Array&lt;number&gt;; }
`noDataColor` | Array&lt;number&gt;
`defaultColor` | Array&lt;number&gt;

## Example

```typescript
import type { PaletteColorizer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "colors": null,
  "noDataColor": null,
  "defaultColor": null,
} satisfies PaletteColorizer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PaletteColorizer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


