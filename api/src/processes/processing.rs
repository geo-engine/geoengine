use crate::parameters::{RasterBandDescriptor, RasterDataType};
use geoengine_macros::type_tag;
use geoengine_operators::processing::ExpressionParams as OperatorsExpressionParamsStruct;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// # Raster Expression
///
/// The `Expression` operator performs a pixel-wise mathematical expression on one or more bands of a raster source.
/// The expression is specified as a user-defined script in a very simple language.
/// The output is a raster time series with the result of the expression and with time intervals that are the same as for the inputs.
/// Users can specify an output data type.
/// Internally, the expression is evaluated using floating-point numbers.
///
/// An example usage scenario is to calculate NDVI for a red and a near-infrared raster channel.
/// The expression uses a raster source with two bands, referred to as A and B, and calculates the formula `(A - B) / (A + B)`.
/// When the temporal resolution is months, our output NDVI will also be a monthly time series.
///
/// ## Types
///
/// The following describes the types used in the parameters.
///
/// ### Expression
///
/// Expressions are simple scripts to perform pixel-wise computations.
/// One can refer to the raster inputs as `A` for the first raster band, `B` for the second, and so on.
/// Furthermore, expressions can check with `A IS NODATA`, `B IS NODATA`, etc. for NO DATA values.
/// This is important if `mapNoData` is set to true.
/// Otherwise, NO DATA values are mapped automatically to the output NO DATA value.
/// Finally, the value `NODATA` can be used to output NO DATA.
///
/// Users can think of this implicit function signature for, e.g., two inputs:
///
/// ```Rust
/// fn (A: f64, B: f64) -> f64
/// ```
///
/// As a start, expressions contain algebraic operations and mathematical functions.
///
/// ```Rust
/// (A + B) / 2
/// ```
///
/// In addition, branches can be used to check for conditions.
///
/// ```Rust
/// if A IS NODATA {
///     B
/// } else {
///     A
/// }
/// ```
///
/// Function calls can be used to access utility functions.
///
/// ```Rust
/// max(A, 0)
/// ```
///
/// Currently, the following functions are available:
///
/// - `abs(a)`: absolute value
/// - `min(a, b)`, `min(a, b, c)`: minimum value
/// - `max(a, b)`, `max(a, b, c)`: maximum value
/// - `sqrt(a)`: square root
/// - `ln(a)`: natural logarithm
/// - `log10(a)`: base 10 logarithm
/// - `cos(a)`, `sin(a)`, `tan(a)`, `acos(a)`, `asin(a)`, `atan(a)`: trigonometric functions
/// - `pi()`, `e()`: mathematical constants
/// - `round(a)`, `ceil(a)`, `floor(a)`: rounding functions
/// - `mod(a, b)`: division remainder
/// - `to_degrees(a)`, `to_radians(a)`: conversion to degrees or radians
///
/// To generate more complex expressions, it is possible to have variable assignments.
///
/// ```Rust
/// let mean = (A + B) / 2;
/// let coefficient = 0.357;
/// mean * coefficient
/// ```
///
/// Note, that all assignments are separated by semicolons.
/// However, the last expression must be without a semicolon.
#[type_tag(value = "Expression")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Expression {
    pub params: ExpressionParameters,
}

/// ## Types
///
/// The following describes the types used in the parameters.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionParameters {
    /// Expression script
    ///
    /// Example: `"(A - B) / (A + B)"`
    pub expression: String,
    /// A raster data type for the output
    pub output_type: RasterDataType,
    /// Description about the output
    pub output_band: Option<RasterBandDescriptor>,
    /// Should NO DATA values be mapped with the `expression`? Otherwise, they are mapped automatically to NO DATA.
    pub map_no_data: bool,
}

impl TryFrom<Expression> for OperatorsExpressionParamsStruct {
    type Error = anyhow::Error;

    fn try_from(value: Expression) -> Result<Self, Self::Error> {
        Ok(OperatorsExpressionParamsStruct {
            expression: value.params.expression,
            output_type: value.params.output_type.into(),
            output_band: value.params.output_band.map(Into::into),
            map_no_data: value.params.map_no_data,
        })
    }
}

// TODO: OpenAPI and conversions for other operators:
//  - RasterVectorJoin

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_converts_expressions() {
        let api = Expression {
            r#type: Default::default(),
            params: ExpressionParameters {
                expression: "2 * A + B".to_string(),
                output_type: RasterDataType::F32,
                output_band: None,
                map_no_data: true,
            },
        };

        let ops = OperatorsExpressionParamsStruct::try_from(api).expect("conversion failed");

        assert_eq!(ops.expression, "2 * A + B");
        assert_eq!(
            ops.output_type,
            geoengine_datatypes::raster::RasterDataType::F32
        );
        assert!(ops.output_band.is_none());
        assert!(ops.map_no_data);
    }
}
