
# DatasetListing


## Properties

Name | Type
------------ | -------------
`id` | string
`name` | string
`displayName` | string
`description` | string
`tags` | Array&lt;string&gt;
`sourceOperator` | string
`resultDescriptor` | [TypedResultDescriptor](TypedResultDescriptor.md)
`symbology` | [Symbology](Symbology.md)

## Example

```typescript
import type { DatasetListing } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "name": null,
  "displayName": null,
  "description": null,
  "tags": null,
  "sourceOperator": null,
  "resultDescriptor": null,
  "symbology": null,
} satisfies DatasetListing

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DatasetListing
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


