---
title: Raster Expression
---

The `Expression` operator performs a pixel-wise mathematical expression on one or more bands of a raster source.
The expression is specified as a user-defined script in a very simple language.
The output is a raster time series with the result of the expression and with time intervals that are the same as for the inputs.
Users can specify an output data type.
Internally, the expression is evaluated using floating-point numbers.

An example usage scenario is to calculate NDVI for a red and a near-infrared raster channel.
The expression uses a raster source with two bands, referred to as A and B, and calculates the formula `(A - B) / (A + B)`.
When the temporal resolution is months, our output NDVI will also be a monthly time series.

## Types

The following describes the types used in the parameters.

### Expression

Expressions are simple scripts to perform pixel-wise computations.
One can refer to the raster inputs as `A` for the first raster band, `B` for the second, and so on.
Furthermore, expressions can check with `A IS NODATA`, `B IS NODATA`, etc. for NO DATA values.
This is important if `mapNoData` is set to true.
Otherwise, NO DATA values are mapped automatically to the output NO DATA value.
Finally, the value `NODATA` can be used to output NO DATA.

Users can think of this implicit function signature for, e.g., two inputs:

```rust
fn (A: f64, B: f64) -> f64
```

As a start, expressions contain algebraic operations and mathematical functions.

```rust
(A + B) / 2
```

In addition, branches can be used to check for conditions.

```rust
if A IS NODATA {
    B
} else {
    A
}
```

Function calls can be used to access utility functions.

```rust
max(A, 0)
```

Currently, the following functions are available:

- `abs(a)`: absolute value
- `min(a, b)`, `min(a, b, c)`: minimum value
- `max(a, b)`, `max(a, b, c)`: maximum value
- `sqrt(a)`: square root
- `ln(a)`: natural logarithm
- `log10(a)`: base 10 logarithm
- `cos(a)`, `sin(a)`, `tan(a)`, `acos(a)`, `asin(a)`, `atan(a)`: trigonometric functions
- `pi()`, `e()`: mathematical constants
- `round(a)`, `ceil(a)`, `floor(a)`: rounding functions
- `mod(a, b)`: division remainder
- `to_degrees(a)`, `to_radians(a)`: conversion to degrees or radians

To generate more complex expressions, it is possible to have variable assignments.

```rust
let mean = (A + B) / 2;
let coefficient = 0.357;
mean * coefficient
```

Note, that all assignments are separated by semicolons.
However, the last expression must be without a semicolon.

## Parameters

| Name       | Type                 | Description                                                                                                 | Examples              |
| ---------- | -------------------- | ----------------------------------------------------------------------------------------------------------- | --------------------- |
| expression | string               | Expression script<br><br>Example: `"(A - B) / (A + B)"`                                                     | `"(A - B) / (A + B)"` |
| mapNoData  | boolean              | Should NO DATA values be mapped with the `expression`? Otherwise, they are mapped automatically to NO DATA. | `true`                |
| outputBand | RasterBandDescriptor | Description about the output                                                                                |                       |
| outputType | RasterDataType       | A raster data type for the output                                                                           |                       |

## Sources

| Name   | Type           | Description                            |
| ------ | -------------- | -------------------------------------- |
| raster | RasterOperator | An operator that produces raster data. |

## Examples

```json
{
    "type": "Expression",
    "params": {
        "expression": "(A - B) / (A + B)",
        "outputType": "F32",
        "outputBand": {
            "name": "NDVI",
            "measurement": {
                "type": "unitless"
            }
        },
        "mapNoData": true
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
}
```
