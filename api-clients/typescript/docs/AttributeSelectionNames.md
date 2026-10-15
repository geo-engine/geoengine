
# AttributeSelectionNames

Selects the attributes with the given names.  The `values` are the names of the attributes. They must contain at least one name and no duplicates. Raster bands are output in their order in the input raster, regardless of the order of `values`. Vector columns are output in the order of `values`.

## Properties

Name | Type
------------ | -------------
`type` | string
`values` | Set&lt;string&gt;

## Example

```typescript
import type { AttributeSelectionNames } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "values": null,
} satisfies AttributeSelectionNames

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AttributeSelectionNames
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


