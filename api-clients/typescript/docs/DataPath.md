
# DataPath

A data path is a reference to a location where data is stored. It can be a volume, an upload, or an external source. This information is used when turning a relative file path of a `Dataset` file into an absolute file path on the server.

## Properties

Name | Type
------------ | -------------
`volume` | string
`upload` | string

## Example

```typescript
import type { DataPath } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "volume": null,
  "upload": null,
} satisfies DataPath

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as DataPath
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


