
# Fraction

Upscale factor relative to input resolution (`x >= 1`, `y >= 1`).

## Properties

Name | Type
------------ | -------------
`type` | string
`x` | number
`y` | number

## Example

```typescript
import type { Fraction } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "x": null,
  "y": null,
} satisfies Fraction

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Fraction
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


