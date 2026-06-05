
# TilesCrs

Coordinate Reference System (CRS)

## Properties

Name | Type
------------ | -------------
`uri` | string
`wkt` | { [key: string]: any; }
`referenceSystem` | string

## Example

```typescript
import type { TilesCrs } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "uri": null,
  "wkt": null,
  "referenceSystem": null,
} satisfies TilesCrs

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as TilesCrs
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


