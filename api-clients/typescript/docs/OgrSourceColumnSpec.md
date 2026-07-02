
# OgrSourceColumnSpec


## Properties

Name | Type
------------ | -------------
`formatSpecifics` | [FormatSpecifics](FormatSpecifics.md)
`x` | string
`y` | string
`_int` | Array&lt;string&gt;
`_float` | Array&lt;string&gt;
`text` | Array&lt;string&gt;
`bool` | Array&lt;string&gt;
`datetime` | Array&lt;string&gt;
`rename` | { [key: string]: string; }

## Example

```typescript
import type { OgrSourceColumnSpec } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "formatSpecifics": null,
  "x": null,
  "y": null,
  "_int": null,
  "_float": null,
  "text": null,
  "bool": null,
  "datetime": null,
  "rename": null,
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


