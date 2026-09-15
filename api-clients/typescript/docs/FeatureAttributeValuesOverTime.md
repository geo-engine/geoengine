
# FeatureAttributeValuesOverTime

The `FeatureAttributeValuesOverTime` is a _plot operator_ that computes a multi-line plot for feature attribute values over time. For distinguishing features, the data requires an id column. The output is a plot in Vega-Lite specification.  ```mermaid xychart-beta     title \"Selected Regional Trends\"     x-axis [\"Jan 05\", \"Feb 02\", \"Mar 02\", \"Mar 30\"]     y-axis \"Value\" 0 --> 250     line \"Papenburg\" [118, 201, 198, 225]     line \"Wismar\" [185, 158, 177, 213]     line \"Frederikshavn\" [46, 50, 103, 124]     line \"Helsingor\" [44, 110, 112, 128] ```  For instance, you want to plot the NDVI values of a feature collection of trees. Then, you can use a multi-line plot to visualize the trees by their id.  ## Errors  The operator returns an error if the selected columns ( `idColumn` and `valueColumn`) do not exist or `valueColumn` is not numeric.  ## Notes  The operator processes a maximum of `20` different ids. After recognizing more than `20` different ids, the operator ignores the rest. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [FeatureAttributeValuesOverTimeParameters](FeatureAttributeValuesOverTimeParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { FeatureAttributeValuesOverTime } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies FeatureAttributeValuesOverTime

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as FeatureAttributeValuesOverTime
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


