
# Collections


## Properties

Name | Type
------------ | -------------
`links` | [Array&lt;Link&gt;](Link.md)
`timeStamp` | string
`numberMatched` | number
`numberReturned` | number
`collections` | [Array&lt;Collection&gt;](Collection.md)
`crs` | Array&lt;string&gt;

## Example

```typescript
import type { Collections } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "links": null,
  "timeStamp": null,
  "numberMatched": null,
  "numberReturned": null,
  "collections": null,
  "crs": null,
} satisfies Collections

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Collections
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


