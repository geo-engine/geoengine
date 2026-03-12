use crate::{
    api::model::processing_graphs::source_parameters::{
        MultipleRasterOrSingleVectorSource, SingleRasterOrVectorSource,
    },
    string_token,
};
use geoengine_macros::{api_operator, type_tag};
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The `Histogram` is a _plot operator_ that computes a histogram plot either over attributes of a vector dataset or values of a raster source.
/// The output is a plot in [Vega-Lite](https://vega.github.io/vega-lite/) specification.
///
/// For instance, you want to plot the data distribution of numeric attributes of a feature collection.
/// Then you can use a histogram with a suitable number of buckets to visualize and assess this.
///
/// ## Errors
///
/// The operator returns an error if the selected column (`columnName`) does not exist or is not numeric.
///
/// ## Notes
///
/// If `bounds` or `buckets` are not defined, the operator will determine these values by itself which requires processing the data twice.
///
/// If the `buckets` parameter is set to `squareRootChoiceRule`, the operator estimates it using the square root of the number of elements in the data.
///
#[api_operator(examples(json!({
    "type": "Histogram",
    "params": {
        "columnName": "foobar",
        "bounds": {
            "min": 5.0,
            "max": 10.0
        },
        "buckets": {
            "type": "number",
            "value": 15
        },
        "interactive": false
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
})))]
pub struct Histogram {
    pub params: HistogramParameters,
    pub sources: SingleRasterOrVectorSource,
}

/// The parameter spec for `Histogram`
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistogramParameters {
    /// Name of the (numeric) vector attribute or raster band to compute the histogram on.
    #[schema(examples("temperature"))]
    pub attribute_name: String,
    /// If `data`, it computes the bounds of the underlying data. If `values`, one can specify custom bounds.
    #[schema(examples(json!({ "min": 0.0, "max": 20.0 }), "data"))]
    pub bounds: HistogramBounds,
    /// The number of buckets. The value can be specified or calculated.
    #[schema(examples(json!({ "type": "number", "value": 20 })))]
    pub buckets: HistogramBuckets,
    /// Flag, if the histogram should have user interactions for a range selection. It is `false` by default.
    #[serde(default)]
    #[schema(examples(true))]
    pub interactive: bool,
}

string_token!(Data, "data");

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum HistogramBounds {
    Data(Data),
    Values {
        #[schema(value_type = f64)]
        min: NotNan<f64>,
        #[schema(value_type = f64)]
        max: NotNan<f64>,
    },
}

fn default_max_number_of_buckets() -> u8 {
    20
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum HistogramBuckets {
    #[serde(rename_all = "camelCase")]
    Number { value: u8 },
    #[serde(rename_all = "camelCase")]
    SquareRootChoiceRule {
        #[serde(default = "default_max_number_of_buckets")]
        max_number_of_buckets: u8,
    },
}

impl TryFrom<Histogram> for geoengine_operators::plot::Histogram {
    type Error = anyhow::Error;
    fn try_from(value: Histogram) -> Result<Self, Self::Error> {
        let params = geoengine_operators::plot::HistogramParams {
            attribute_name: value.params.attribute_name,
            bounds: match value.params.bounds {
                HistogramBounds::Data(_) => {
                    geoengine_operators::plot::HistogramBounds::Data(Default::default())
                }
                HistogramBounds::Values { min, max } => {
                    geoengine_operators::plot::HistogramBounds::Values {
                        min: *min,
                        max: *max,
                    }
                }
            },
            buckets: match value.params.buckets {
                HistogramBuckets::Number { value } => {
                    geoengine_operators::plot::HistogramBuckets::Number { value }
                }
                HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets,
                } => geoengine_operators::plot::HistogramBuckets::SquareRootChoiceRule {
                    max_number_of_buckets,
                },
            },
            interactive: value.params.interactive,
        };
        let sources = value.sources.try_into()?;
        Ok(Self { params, sources })
    }
}

/// The `Statistics` operator is a _plot operator_ that computes count statistics over
///
/// - a selection of numerical columns of a single vector dataset, or
/// - multiple raster datasets.
///
/// The output is a JSON description.
///
/// For instance, you want to get an overview of a raster data source.
/// Then, you can use this operator to get basic count statistics.
///
/// ## Vector Data
///
/// In the case of vector data, the operator generates one statistic for each of the selected numerical attributes.
/// The operator returns an error if one of the selected attributes is not numeric.
///
/// ## Raster Data
///
/// For raster data, the operator generates one statistic for each input raster.
///
/// ## Inputs
///
/// The operator consumes exactly one _vector_ or multiple _raster_ operators.
///
/// | Parameter | Type                                 |
/// | --------- | ------------------------------------ |
/// | `source`  | `MultipleRasterOrSingleVectorSource` |
///
/// ## Errors
///
/// The operator returns an error in the following cases.
///
/// - Vector data: The `attribute` for one of the given `columnNames` is not numeric.
/// - Vector data: The `attribute` for one of the given `columnNames` does not exist.
/// - Raster data: The length of the `columnNames` parameter does not match the number of input rasters.
///
/// ### Example Output
///
/// ```json
/// {
///   "A": {
///     "valueCount": 6,
///     "validCount": 6,
///     "min": 1.0,
///     "max": 6.0,
///     "mean": 3.5,
///     "stddev": 1.707,
///     "percentiles": [
///       {
///         "percentile": 0.25,
///         "value": 2.0
///       },
///       {
///         "percentile": 0.5,
///         "value": 3.5
///       },
///       {
///         "percentile": 0.75,
///         "value": 5.0
///       }
///     ]
///   }
/// }
/// ```
///
#[api_operator(examples(json!({
    "type": "Statistics",
    "params": {
        "columnNames": ["A"],
        "percentiles": [0.25, 0.5, 0.75]
    },
    "sources": {
        "source": [{
            "type": "GdalSource",
            "params": {
            "data": "ndvi"
            }
        }]
    }
})))]
pub struct Statistics {
    pub params: StatisticsParameters,
    pub sources: MultipleRasterOrSingleVectorSource,
}

/// The parameter spec for `Statistics`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatisticsParameters {
    /// # Vector data
    /// The names of the attributes to generate statistics for.
    ///
    /// # Raster data
    /// _Optional_: An alias for each input source.
    /// The operator will automatically name the rasters `Raster-1`, `Raster-2`, … if this parameter is empty.
    /// If aliases are given, the number of aliases must match the number of input rasters.
    /// Otherwise an error is returned.
    #[schema(examples(json!(["x", "y"])))]
    #[serde(default)]
    pub column_names: Vec<String>,
    /// The percentiles to compute for each attribute.
    #[serde(default)]
    #[schema(value_type = Vec<f64>, examples(json!([0.25, 0.5, 0.75])))]
    pub percentiles: Vec<NotNan<f64>>,
}

impl TryFrom<Statistics> for geoengine_operators::plot::Statistics {
    type Error = anyhow::Error;
    fn try_from(value: Statistics) -> Result<Self, Self::Error> {
        let params = geoengine_operators::plot::StatisticsParams {
            column_names: value.params.column_names,
            percentiles: value.params.percentiles.clone(),
        };
        let sources = value.sources.try_into()?;
        Ok(Self { params, sources })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::processing_graphs::parameters::SpatialBoundsDerive;
    use crate::api::model::processing_graphs::source::{
        MockPointSource, MockPointSourceParameters,
    };
    use crate::api::model::processing_graphs::source_parameters::{
        MultipleRasterOrSingleVectorOperator, MultipleRasterOrSingleVectorSource,
        SingleRasterOrVectorOperator, SingleRasterOrVectorSource,
    };
    use crate::api::model::{datatypes::Coordinate2D, processing_graphs::VectorOperator};
    use ordered_float::NotNan;

    #[test]
    fn it_converts_statistics_operators() {
        let stats = Statistics {
            r#type: Default::default(),
            params: StatisticsParameters {
                column_names: vec!["A".to_string()],
                percentiles: vec![NotNan::new(0.5).unwrap()],
            },
            sources: MultipleRasterOrSingleVectorSource {
                source: MultipleRasterOrSingleVectorOperator::Vector(
                    VectorOperator::MockPointSource(MockPointSource {
                        r#type: Default::default(),
                        params: MockPointSourceParameters {
                            points: vec![Coordinate2D { x: 1.0, y: 2.0 }],
                            spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                        },
                    }),
                ),
            },
        };

        let operators: geoengine_operators::plot::Statistics =
            stats.try_into().expect("conversion failed");

        assert_eq!(operators.params.column_names, vec!["A".to_string()]);
        assert_eq!(operators.params.percentiles.len(), 1);
        assert_eq!(
            operators.params.percentiles[0].clone(),
            NotNan::new(0.5).unwrap()
        );
    }

    #[test]
    fn it_converts_histogram_operators() {
        let hist = Histogram {
            r#type: Default::default(),
            params: HistogramParameters {
                attribute_name: "temperature".to_string(),
                bounds: HistogramBounds::Data(Data),
                buckets: HistogramBuckets::Number { value: 10 },
                interactive: false,
            },
            sources: SingleRasterOrVectorSource {
                source: SingleRasterOrVectorOperator::Vector(VectorOperator::MockPointSource(
                    MockPointSource {
                        r#type: Default::default(),
                        params: MockPointSourceParameters {
                            points: vec![Coordinate2D { x: 1.0, y: 2.0 }],
                            spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                        },
                    },
                )),
            },
        };

        let operators: geoengine_operators::plot::Histogram =
            hist.try_into().expect("conversion failed");

        assert_eq!(operators.params.attribute_name, "temperature");
        if let geoengine_operators::plot::HistogramBounds::Data(_) = &operators.params.bounds {
        } else {
            panic!("expected Data bounds");
        }

        if let geoengine_operators::plot::HistogramBuckets::Number { value } =
            &operators.params.buckets
        {
            assert_eq!(*value, 10);
        } else {
            panic!("expected Number buckets");
        }
    }
}
