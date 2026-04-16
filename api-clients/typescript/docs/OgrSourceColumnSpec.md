
# OgrSourceColumnSpec


## Properties

Name | Type
------------ | -------------
`bool` | Array&lt;string&gt;
`datetime` | Array&lt;string&gt;
`_float` | Array&lt;string&gt;
`formatSpecifics` | [FormatSpecifics](FormatSpecifics.md)
`_int` | Array&lt;string&gt;
`rename` | { [key: string]: string; }
`text` | Array&lt;string&gt;
`x` | string
`y` | string

## Example

```typescript
import type { OgrSourceColumnSpec } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "bool": null,
  "datetime": null,
  "_float": null,
  "formatSpecifics": null,
  "_int": null,
  "rename": null,
  "text": null,
  "x": null,
  "y": null,
} satisfies OgrSourceColumnSpec

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as OgrSourceColumnSpec
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


