
# AddDataset


## Properties

Name | Type
------------ | -------------
`name` | string
`displayName` | string
`description` | string
`sourceOperator` | string
`symbology` | [Symbology](Symbology.md)
`provenance` | [Array&lt;Provenance&gt;](Provenance.md)
`tags` | Array&lt;string&gt;

## Example

```typescript
import type { AddDataset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": null,
  "displayName": null,
  "description": null,
  "sourceOperator": null,
  "symbology": null,
  "provenance": null,
  "tags": null,
} satisfies AddDataset

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AddDataset
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


