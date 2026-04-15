
# AddDataset


## Properties

Name | Type
------------ | -------------
`description` | string
`displayName` | string
`name` | string
`provenance` | [Array&lt;Provenance&gt;](Provenance.md)
`sourceOperator` | string
`symbology` | [Symbology](Symbology.md)
`tags` | Array&lt;string&gt;

## Example

```typescript
import type { AddDataset } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "description": null,
  "displayName": null,
  "name": null,
  "provenance": null,
  "sourceOperator": null,
  "symbology": null,
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


