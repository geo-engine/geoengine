
# VisualPointClusteringParameters


## Properties

Name | Type
------------ | -------------
`minRadiusPx` | number
`deltaPx` | number
`resolution` | number
`radiusColumn` | string
`countColumn` | string
`columnAggregates` | [{ [key: string]: AttributeAggregateDef; }](AttributeAggregateDef.md)

## Example

```typescript
import type { VisualPointClusteringParameters } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "minRadiusPx": null,
  "deltaPx": null,
  "resolution": null,
  "radiusColumn": null,
  "countColumn": null,
  "columnAggregates": null,
} satisfies VisualPointClusteringParameters

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as VisualPointClusteringParameters
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


