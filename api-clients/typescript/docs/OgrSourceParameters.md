
# OgrSourceParameters

Parameters for the [`OgrSource`] operator.

## Properties

Name | Type
------------ | -------------
`attributeProjection` | Array&lt;string&gt;
`data` | string

## Example

```typescript
import type { OgrSourceParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "attributeProjection": null,
  "data": null,
} satisfies OgrSourceParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OgrSourceParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


