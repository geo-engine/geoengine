
# VectorExpression

The `VectorExpression` operator performs a feature-wise expression function on a feature collection of a vector source. The expression is specified as a user-defined script in a very simple language. The output is a feature collection with the result of the expression and with time intervals that are the same as for the inputs. Users can either add a new column or replace the geometry column with the outputs of the expression. Internally, the expression is evaluated using floating-point numbers.  An example usage scenario is to calculate a population density from an `area` and a `population_size` column. The expression uses a feature collection with two columns, referred to with their column names `area` and a `population_size`, and calculates the formula `area / population_size`. The output feature collection contains the result of the density expression in a new column.  Another example is to calculate the centroid of a polygon geometry. The expression uses a feature collection with a geometry column and calculates the formula `centroid(geom)`. The output feature collection contains the result of the centroid expression replacing the original geometries.  ## Types  The following describes the types used in the parameters.  ### Expression  Expressions are simple scripts to perform feature-wise computations. One can refer to the columns with their name, e.g., `area` and a `population_size`. Furthermore, expressions can check with `A IS NODATA`, `B IS NODATA`, etc. for empty or NO DATA values. Finally, the value `NODATA` can be used to output empty or NO DATA.  Users can think of this implicit function signature for, e.g., two inputs:  ```rust fn (A: f64, B: f64) -> f64 ```  As a start, expressions contain algebraic operations and mathematical functions.  ```rust (A + B) / 2 ```  In addition, branches can be used to check for conditions.  ```rust if A IS NODATA {     B } else {     A } ```  To generate more complex expressions, it is possible to have variable assignments.  ```rust let mean = (A + B) / 2; let coefficient = 0.357; mean * coefficient ```  Note, that all assignments are separated by semicolons. However, the last expression must be without a semicolon.  #### Numbers  Function calls can be used to access utility functions.  ```rust max(A, 0) ```  Currently, the following functions are available:  - `abs(a)`: absolute value - `min(a, b)`, `min(a, b, c)`: minimum value - `max(a, b)`, `max(a, b, c)`: maximum value - `sqrt(a)`: square root - `ln(a)`: natural logarithm - `log10(a)`: base 10 logarithm - `cos(a)`, `sin(a)`, `tan(a)`, `acos(a)`, `asin(a)`, `atan(a)`: trigonometric functions - `pi()`, `e()`: mathematical constants - `round(a)`, `ceil(a)`, `floor(a)`: rounding functions - `mod(a, b)`: division remainder - `to_degrees(a)`, `to_radians(a)`: conversion to degrees or radians  #### Geometries  Geometries can be referred to using the `geometryColumnName`, which is `geom` by default. There are several functions to work with geometries:  - `centroid(geom)`: returns the centroid of the geometry - `area(geom)`: returns the area of the geometry  An example expression to calculate the centroid of a geometry is:  ```rust centroid(geom) ```  ## Inputs  The `VectorExpression` operator expects one vector input with at most 8 bands.  | Parameter | Type                 | | --------- | -------------------- | | `vector`  | `SingleVectorSource` |  ## Errors  The parsing of the expression can fail if there are, e.g., syntax errors. 

## Properties

Name | Type
------------ | -------------
`type` | string
`params` | [VectorExpressionParameters](VectorExpressionParameters.md)
`sources` | [SingleVectorSource](SingleVectorSource.md)

## Example

```typescript
import type { VectorExpression } from '@geoengine/api-client'

// TODO: Update the object below with actual values
const example = {
  "type": null,
  "params": null,
  "sources": null,
} satisfies VectorExpression

console.log(example)

// Convert the instance to a JSON string
const exampleJSON: string = JSON.stringify(example)
console.log(exampleJSON)

// Parse the JSON string back to an object
const exampleParsed = JSON.parse(exampleJSON) as VectorExpression
console.log(exampleParsed)
```

[[Back to top]](#) [[Back to API list]](../README.md#api-endpoints) [[Back to Model list]](../README.md#models) [[Back to README]](../README.md)


