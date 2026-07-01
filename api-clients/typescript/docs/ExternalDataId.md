
# ExternalDataId


## Properties

Name | Type
------------ | -------------
`type` | string
`providerId` | string
`layerId` | string

## Example

```typescript
import type { ExternalDataId } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "providerId": null,
  "layerId": null,
} satisfies ExternalDataId

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ExternalDataId
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


