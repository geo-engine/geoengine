
# BoxPlotParameters

The parameter spec for [`BoxPlot`].  ## Vector Data  In the case of vector data, the operator generates one box for each of the selected numerical attributes. The operator returns an error if one of the selected attributes is not numeric.  ## Raster Data  For raster data, the operator generates one box for each input raster. 

## Properties

Name | Type
------------ | -------------
`columnNames` | Array&lt;string&gt;

## Example

```typescript
import type { BoxPlotParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "columnNames": null,
} satisfies BoxPlotParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as BoxPlotParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


