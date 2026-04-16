
# UpdateLayer


## Properties

Name | Type
------------ | -------------
`description` | string
`metadata` | { [key: string]: string; }
`name` | string
`properties` | Array&lt;Array&lt;string&gt;&gt;
`symbology` | [Symbology](Symbology.md)
`workflow` | [Workflow](Workflow.md)

## Example

```typescript
import type { UpdateLayer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "description": Example layer description,
  "metadata": null,
  "name": Example Layer,
  "properties": null,
  "symbology": null,
  "workflow": null,
} satisfies UpdateLayer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as UpdateLayer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


