
# Dataset


## Properties

Name | Type
------------ | -------------
`dataPath` | [DataPath](DataPath.md)
`description` | string
`displayName` | string
`id` | string
`name` | string
`provenance` | [Array&lt;Provenance&gt;](Provenance.md)
`resultDescriptor` | [TypedResultDescriptor](TypedResultDescriptor.md)
`sourceOperator` | string
`symbology` | [Symbology](Symbology.md)
`tags` | Array&lt;string&gt;

## Example

```typescript
import type { Dataset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "dataPath": null,
  "description": null,
  "displayName": null,
  "id": null,
  "name": null,
  "provenance": null,
  "resultDescriptor": null,
  "sourceOperator": null,
  "symbology": null,
  "tags": null,
} satisfies Dataset

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Dataset
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


