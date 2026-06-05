
# AddLayer


## Properties

Name | Type
------------ | -------------
`name` | string
`description` | string
`workflow` | [Workflow](Workflow.md)
`symbology` | [Symbology](Symbology.md)
`properties` | Array&lt;Array&lt;string&gt;&gt;
`metadata` | { [key: string]: string; }

## Example

```typescript
import type { AddLayer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "name": Example Layer,
  "description": Example layer description,
  "workflow": null,
  "symbology": null,
  "properties": null,
  "metadata": null,
} satisfies AddLayer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as AddLayer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


