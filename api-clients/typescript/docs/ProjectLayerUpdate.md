
# ProjectLayerUpdate

A layer or plot that is added to or changed in a project.  Without `id`, it is a new layer and the server assigns an id. With `id`, it replaces the layer of the project with this id, so layers can also be moved.

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
import type { ProjectLayerUpdate } from '@geoengine/api-client'

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
} satisfies ProjectLayerUpdate

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ProjectLayerUpdate
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


