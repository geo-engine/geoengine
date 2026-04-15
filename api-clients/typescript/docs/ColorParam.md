
# ColorParam


## Properties

Name | Type
------------ | -------------
`color` | Array&lt;number&gt;
`type` | string
`attribute` | string
`colorizer` | [Colorizer](Colorizer.md)

## Example

```typescript
import type { ColorParam } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "color": null,
  "type": null,
  "attribute": null,
  "colorizer": null,
} satisfies ColorParam

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as ColorParam
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


