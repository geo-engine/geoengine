
# AttributeSelection

Selects all attributes, i.e., raster bands or vector columns, or the ones with the given names.

## Properties

Name | Type
------------ | -------------
`type` | string
`values` | Set&lt;string&gt;

## Example

```typescript
import type { AttributeSelection } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "values": null,
} satisfies AttributeSelection

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AttributeSelection
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


