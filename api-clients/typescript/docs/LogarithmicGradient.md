
# LogarithmicGradient


## Properties

Name | Type
------------ | -------------
`breakpoints` | [Array&lt;Breakpoint&gt;](Breakpoint.md)
`noDataColor` | Array&lt;number&gt;
`overColor` | Array&lt;number&gt;
`type` | string
`underColor` | Array&lt;number&gt;

## Example

```typescript
import type { LogarithmicGradient } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "breakpoints": null,
  "noDataColor": null,
  "overColor": null,
  "type": null,
  "underColor": null,
} satisfies LogarithmicGradient

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as LogarithmicGradient
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


