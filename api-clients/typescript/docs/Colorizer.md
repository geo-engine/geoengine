
# Colorizer

A colorizer specifies a mapping between raster values and an output image There are different variants that perform different kinds of mapping.

## Properties

Name | Type
------------ | -------------
`type` | string
`breakpoints` | [Array&lt;Breakpoint&gt;](Breakpoint.md)
`noDataColor` | Array&lt;number&gt;
`overColor` | Array&lt;number&gt;
`underColor` | Array&lt;number&gt;
`colors` | { [key: string]: Array&lt;number&gt;; }
`defaultColor` | Array&lt;number&gt;

## Example

```typescript
import type { Colorizer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "breakpoints": null,
  "noDataColor": null,
  "overColor": null,
  "underColor": null,
  "colors": null,
  "defaultColor": null,
} satisfies Colorizer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Colorizer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


