
# NumberParam


## Properties

Name | Type
------------ | -------------
`type` | string
`value` | number
`attribute` | string
`defaultValue` | number
`factor` | number

## Example

```typescript
import type { NumberParam } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "value": null,
  "attribute": null,
  "defaultValue": null,
  "factor": null,
} satisfies NumberParam

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as NumberParam
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


