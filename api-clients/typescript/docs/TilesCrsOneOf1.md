
# TilesCrsOneOf1

An object defining the CRS using the JSON encoding for Well-known text representation of coordinate reference systems 2.0

## Properties

Name | Type
------------ | -------------
`wkt` | { [key: string]: any; }

## Example

```typescript
import type { TilesCrsOneOf1 } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "wkt": null,
} satisfies TilesCrsOneOf1

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TilesCrsOneOf1
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


