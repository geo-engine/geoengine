
# ProjectLayer

A layer or plot of a project.  It is also a layer of the projects data connector, with the same `id`. Only the visibility is specific to the project.

## Properties

Name | Type
------------ | -------------
`id` | string
`name` | string
`description` | string
`processingGraph` | string
`symbology` | [Symbology](Symbology.md)
`properties` | Array&lt;Array&lt;string&gt;&gt;
`metadata` | { [key: string]: string; }
`visibility` | [LayerVisibility](LayerVisibility.md)
`source` | [ProviderLayerId](ProviderLayerId.md)

## Example

```typescript
import type { ProjectLayer } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "id": null,
  "name": null,
  "description": null,
  "processingGraph": null,
  "symbology": null,
  "properties": null,
  "metadata": null,
  "visibility": null,
  "source": null,
} satisfies ProjectLayer

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ProjectLayer
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


