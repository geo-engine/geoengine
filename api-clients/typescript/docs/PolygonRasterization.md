
# PolygonRasterization

The `PolygonRasterization` operator creates a raster from a polygon vector source.  A pixel is covered by a polygon if its center lies inside the polygon (holes are respected). Pixels that are not covered by any polygon are NO DATA.  The value of the covered pixels is - a constant (`1` by default), - the number of polygons that cover the pixel (`count`), or - the value of a numeric column (`attribute`), aggregated over all polygons that cover the pixel.   Features with a null value are skipped, unless `ignoreNoData` is `false`, which makes all pixels they cover NO DATA.   `first`, `last`, `min` and `max` keep the measurement of the column, e.g., its classes.   `sum`, `mean` and `medianEstimate` create new values, so a classification becomes unitless.   The median is exact for up to five values and estimated by the P² algorithm for more.  The time slices of the output raster are - `featureTimes` (default): every start and end time of a feature is a slice boundary,   i.e., the slices are the intervals between consecutive boundaries.   For example, features valid during `[0, 10)` and `[5, 20)` yield the slices `[0, 5)`, `[5, 10)` and `[10, 20)`.   Each slice contains exactly the features that are valid during it, and slices without features are NO DATA.   This needs to scan the times of all features of the source for each query, so prefer `regular` for large data. - `regular`: slices of a fixed time step, each containing all features whose time intersects it. - `ignore`: one slice that is valid forever and contains all features.  ## Clipping a raster with polygons  Rasterize the polygons on the grid of the raster and mask the raster with the NO DATA pixels of the result, using a `RasterStacker` and an `Expression` like `if B IS NODATA { NODATA } else { A }`. Both rasters need - the same resolution and origin modulo the resolution, e.g., the default origin `(0, 0)` for most grids, - the same data type, i.e., set `outputType` to the data type of the raster, and - the same regular time steps, i.e., use the `regular` time of the raster, or slices that the stacker can align.  ## Errors  If the source is not a polygon source, the column does not exist or is not numeric, or the constant value cannot be represented by the output type, an error will be thrown.

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [PolygonRasterizationParameters](PolygonRasterizationParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { PolygonRasterization } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies PolygonRasterization

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as PolygonRasterization
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


