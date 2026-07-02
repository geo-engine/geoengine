
# Dataset


## Properties

Name | Type
------------ | -------------
`id` | string
`name` | string
`displayName` | string
`description` | string
`resultDescriptor` | [TypedResultDescriptor](TypedResultDescriptor.md)
`sourceOperator` | string
`symbology` | [Symbology](Symbology.md)
`provenance` | [Array&lt;Provenance&gt;](Provenance.md)
`tags` | Array&lt;string&gt;
`dataPath` | [DataPath](DataPath.md)

## Example

```typescript
import type { Dataset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "name": null,
  "displayName": null,
  "description": null,
  "resultDescriptor": null,
  "sourceOperator": null,
  "symbology": null,
  "provenance": null,
  "tags": null,
  "dataPath": null,
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


