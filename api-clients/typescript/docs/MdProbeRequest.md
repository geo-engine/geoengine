
# MdProbeRequest

Which MD arrays of a file (or file set) to probe.

## Properties

Name | Type
------------ | -------------
`dataPath` | [DataPath](DataPath.md)
`files` | Array&lt;string&gt;
`arrayName` | string
`group` | string
`variablesAsBands` | boolean
`maxZBatchSize` | number
`cacheTtl` | number
`forceBandRole` | boolean

## Example

```typescript
import type { MdProbeRequest } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "dataPath": null,
  "files": null,
  "arrayName": null,
  "group": null,
  "variablesAsBands": null,
  "maxZBatchSize": null,
  "cacheTtl": null,
  "forceBandRole": null,
} satisfies MdProbeRequest

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MdProbeRequest
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


