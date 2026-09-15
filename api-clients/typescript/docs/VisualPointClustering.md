
# VisualPointClustering

The `VisualPointClustering` is a clustering operator for point collections that removes clutter and preserves the spatial structure of the input. The output is a point collection with a count and radius attribute. The operator utilizes the input resolution of the query to determine when points, being displayed as circles, would overlap. Moreover, it allows aggregating non-geo attributes to preserve the other columns of the input. For more information on the algorithm, cf. the paper [Beilschmidt, C. et al.: A Linear-Time Algorithm for the Aggregation and Visualization of Big Spatial Point Data. SIGSPATIAL/GIS 2017: 73:1-73:4](https://doi.org/10.1145/3139958.3140037).  An exemplary use case for this operator is the visualization of point data in an online map application. There, you can use this operator as the final step of the workflow to cluster the points and display them as circles. These circles then pose a decluttered view of the data, e.g., via a WFS endpoint.  ## Errors  If the source value `vector` is not a point collection, an error is thrown.  If multiple columns in `columnAggregates` have the same names, an error is thrown. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [VisualPointClusteringParameters](VisualPointClusteringParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { VisualPointClustering } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies VisualPointClustering

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as VisualPointClustering
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


