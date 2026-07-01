
# SearchCapabilities


## Properties

Name | Type
------------ | -------------
`searchTypes` | [SearchTypes](SearchTypes.md)
`autocomplete` | boolean
`filters` | Array&lt;string&gt;

## Example

```typescript
import type { SearchCapabilities } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "searchTypes": null,
  "autocomplete": null,
  "filters": null,
} satisfies SearchCapabilities

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as SearchCapabilities
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


