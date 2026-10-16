
# MdVariable

One variable of an MD dataset: its name and the role of each of its source dimensions.

## Properties

Name | Type
------------ | -------------
`name` | string
`axisMapping` | [Array&lt;MdAxisMapping&gt;](MdAxisMapping.md)

## Example

```typescript
import type { MdVariable } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": null,
  "axisMapping": null,
} satisfies MdVariable

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as MdVariable
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


