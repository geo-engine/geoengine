
# Conformance

The Conformance declaration states the conformance classes from standards or community specifications, identified by a URI, that the API conforms to. Clients can but are not required to use this information. Accessing the Conformance declaration using HTTP GET returns the list of URIs of conformance classes implemented by the server.

## Properties

Name | Type
------------ | -------------
`conformsTo` | Array&lt;string&gt;

## Example

```typescript
import type { Conformance } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "conformsTo": null,
} satisfies Conformance

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as Conformance
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


