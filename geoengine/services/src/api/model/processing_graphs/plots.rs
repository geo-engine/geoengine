use crate::api::model::processing_graphs::source_parameters::{
    MultipleRasterOrSingleVectorSource, SingleVectorOrRasterSource,
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
    pub sources: SingleVectorOrRasterSource,
}

/// The parameter spec for `Histogram`
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistogramParameters {
    /// Name of the (numeric) vector attribute or raster band to compute the histogram on.
    #[schema(examples("temperature"))]
    pub column_name: String,
    /// If `data`, it computes the bounds of the underlying data.
    /// If `{ "min": ..., "max": ... }`, one can specify custom bounds.
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
pub enum Data {
    #[default]
    #[serde(rename = "data")]
    Data,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(untagged)]
pub enum HistogramBounds {
    Data(Data),
    Values(HistogramBoundsValues),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
pub struct HistogramBoundsValues {
    #[schema(value_type = f64)]
    pub min: NotNan<f64>,
    #[schema(value_type = f64)]
    pub max: NotNan<f64>,
}

fn default_max_number_of_buckets() -> u8 {
    20
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum HistogramBuckets {
    Number(HistogramBucketsNumber),
    SquareRootChoiceRule(HistogramBucketsSquareRootChoiceRule),
}

#[type_tag(value = "number")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistogramBucketsNumber {
    pub value: u8,
}

#[type_tag(value = "squareRootChoiceRule")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistogramBucketsSquareRootChoiceRule {
    #[serde(default = "default_max_number_of_buckets")]
    pub max_number_of_buckets: u8,
}

impl TryFrom<Histogram> for geoengine_operators::plot::Histogram {
    type Error = anyhow::Error;
    fn try_from(value: Histogram) -> Result<Self, Self::Error> {
        let params = geoengine_operators::plot::HistogramParams {
            attribute_name: value.params.column_name,
            bounds: match value.params.bounds {
                HistogramBounds::Data(_) => {
                    geoengine_operators::plot::HistogramBounds::Data(Default::default())
                }
                HistogramBounds::Values(HistogramBoundsValues { min, max }) => {
                    geoengine_operators::plot::HistogramBounds::Values {
                        min: *min,
                        max: *max,
                    }
                }
            },
            buckets: match value.params.buckets {
                HistogramBuckets::Number(HistogramBucketsNumber { r#type: _, value }) => {
                    geoengine_operators::plot::HistogramBuckets::Number { value }
                }
                HistogramBuckets::SquareRootChoiceRule(HistogramBucketsSquareRootChoiceRule {
                    r#type: _,
                    max_number_of_buckets,
                }) => geoengine_operators::plot::HistogramBuckets::SquareRootChoiceRule {
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
    use crate::api::model::processing_graphs::{
        GdalSource, GdalSourceParameters, PlotOperator, RasterOperator, VectorOperator,
        source::{MockPointSource, MockPointSourceParameters, OgrSource, OgrSourceParameters},
        source_parameters::{
            MultipleRasterOrSingleVectorOperator, MultipleRasterOrSingleVectorSource,
            SingleRasterOrVectorOperator, SingleVectorOrRasterSource,
        },
    };
    use crate::api::model::{
        datatypes::Coordinate2D, processing_graphs::parameters::SpatialBoundsDerive,
    };
    use geoengine_operators::engine::PlotOperator as OperatorsPlotOperatorTrait;
    use ordered_float::NotNan;

    // ---------------------------------------------------------------------------
    // Histogram
    // ---------------------------------------------------------------------------

    #[test]
    fn it_parses_histogram_api_example() {
        let example = serde_json::json!({
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
        });

        let parsed: Histogram = serde_json::from_value(example).expect("example must parse");

        assert_eq!(parsed.params.column_name, "foobar");
        assert!(matches!(parsed.params.bounds, HistogramBounds::Values(_)));
    }

    #[test]
    fn it_parses_histogram_data_bounds() {
        let example = serde_json::json!({
            "type": "Histogram",
            "params": {
                "columnName": "temperature",
                "bounds": "data",
                "buckets": {
                    "type": "number",
                    "value": 10
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
        });

        let parsed: Histogram =
            serde_json::from_value(example).expect(r#""data" bounds must parse"#);

        assert_eq!(parsed.params.column_name, "temperature");
        assert!(matches!(parsed.params.bounds, HistogramBounds::Data(_)));
    }

    #[test]
    fn it_serializes_histogram_data_bounds() {
        let histogram = Histogram {
            r#type: Default::default(),
            params: HistogramParameters {
                column_name: "temperature".to_string(),
                bounds: HistogramBounds::Data(Data::Data),
                buckets: HistogramBuckets::Number(HistogramBucketsNumber {
                    r#type: Default::default(),
                    value: 10,
                }),
                interactive: false,
            },
            sources: SingleVectorOrRasterSource {
                vector: SingleRasterOrVectorOperator::Vector(VectorOperator::OgrSource(
                    OgrSource {
                        r#type: Default::default(),
                        params: OgrSourceParameters {
                            data: "ndvi".to_string(),
                            attribute_projection: None,
                        },
                    },
                )),
            },
        };

        let json = serde_json::to_value(&histogram).expect("must serialize");
        assert_eq!(
            json["params"]["bounds"],
            serde_json::json!("data"),
            "bounds should serialize to the string \"data\""
        );

        // round-trip: deserialize back and check variant
        let round_tripped: Histogram = serde_json::from_value(json).expect("round-trip must parse");
        assert!(matches!(
            round_tripped.params.bounds,
            HistogramBounds::Data(_)
        ));
    }

    #[test]
    fn it_serializes_histogram_values_bounds() {
        let histogram = Histogram {
            r#type: Default::default(),
            params: HistogramParameters {
                column_name: "foobar".to_string(),
                bounds: HistogramBounds::Values(HistogramBoundsValues {
                    min: NotNan::new(5.0).unwrap(),
                    max: NotNan::new(10.0).unwrap(),
                }),
                buckets: HistogramBuckets::Number(HistogramBucketsNumber {
                    r#type: Default::default(),
                    value: 15,
                }),
                interactive: false,
            },
            sources: SingleVectorOrRasterSource {
                vector: SingleRasterOrVectorOperator::Vector(VectorOperator::OgrSource(
                    OgrSource {
                        r#type: Default::default(),
                        params: OgrSourceParameters {
                            data: "ndvi".to_string(),
                            attribute_projection: None,
                        },
                    },
                )),
            },
        };

        let json = serde_json::to_value(&histogram).expect("must serialize");
        assert_eq!(
            json["params"]["bounds"],
            serde_json::json!({"min": 5.0, "max": 10.0}),
            "bounds should serialize to {{min, max}} object"
        );

        // round-trip
        let round_tripped: Histogram = serde_json::from_value(json).expect("round-trip must parse");
        assert!(matches!(
            round_tripped.params.bounds,
            HistogramBounds::Values(_)
        ));
    }

    #[test]
    fn it_converts_histogram_example_to_operator() {
        let histogram = Histogram {
            r#type: Default::default(),
            params: HistogramParameters {
                column_name: "foobar".to_string(),
                bounds: HistogramBounds::Values(HistogramBoundsValues {
                    min: NotNan::new(5.0).unwrap(),
                    max: NotNan::new(10.0).unwrap(),
                }),
                buckets: HistogramBuckets::Number(HistogramBucketsNumber {
                    r#type: Default::default(),
                    value: 15,
                }),
                interactive: false,
            },
            sources: SingleVectorOrRasterSource {
                vector: SingleRasterOrVectorOperator::Vector(VectorOperator::OgrSource(
                    OgrSource {
                        r#type: Default::default(),
                        params: OgrSourceParameters {
                            data: "ndvi".to_string(),
                            attribute_projection: None,
                        },
                    },
                )),
            },
        };

        let plot_operator = PlotOperator::Histogram(histogram);
        Box::<dyn OperatorsPlotOperatorTrait>::try_from(plot_operator)
            .map(|_| ())
            .expect("histogram with OgrSource must convert to operator");
    }

    #[test]
    fn it_converts_histogram_operators() {
        let hist = Histogram {
            r#type: Default::default(),
            params: HistogramParameters {
                column_name: "temperature".to_string(),
                bounds: HistogramBounds::Data(Data::Data),
                buckets: HistogramBuckets::Number(HistogramBucketsNumber {
                    r#type: Default::default(),
                    value: 10,
                }),
                interactive: false,
            },
            sources: SingleVectorOrRasterSource {
                vector: SingleRasterOrVectorOperator::Vector(VectorOperator::MockPointSource(
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
        assert!(matches!(
            operators.params.bounds,
            geoengine_operators::plot::HistogramBounds::Data(_)
        ));
        assert!(matches!(
            operators.params.buckets,
            geoengine_operators::plot::HistogramBuckets::Number { value: 10 }
        ));
    }

    // ---------------------------------------------------------------------------
    // Statistics
    // ---------------------------------------------------------------------------

    #[test]
    fn it_parses_statistics_api_example() {
        let example = serde_json::json!({
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
        });

        let parsed: Statistics = serde_json::from_value(example).expect("example must parse");

        assert_eq!(parsed.params.column_names, vec!["A".to_string()]);
        assert_eq!(parsed.params.percentiles.len(), 3);
    }

    #[test]
    fn it_converts_statistics_example_to_operator() {
        let statistics = Statistics {
            r#type: Default::default(),
            params: StatisticsParameters {
                column_names: vec!["A".to_string()],
                percentiles: vec![
                    NotNan::new(0.25).unwrap(),
                    NotNan::new(0.5).unwrap(),
                    NotNan::new(0.75).unwrap(),
                ],
            },
            sources: MultipleRasterOrSingleVectorSource {
                source: MultipleRasterOrSingleVectorOperator::Raster(vec![
                    RasterOperator::GdalSource(GdalSource {
                        r#type: Default::default(),
                        params: GdalSourceParameters {
                            data: "ndvi".to_string(),
                            overview_level: None,
                        },
                    }),
                ]),
            },
        };

        let plot_operator = PlotOperator::Statistics(statistics);
        Box::<dyn OperatorsPlotOperatorTrait>::try_from(plot_operator)
            .map(|_| ())
            .expect("statistics with GdalSource must convert to operator");
    }

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
}
