use crate::api::model::processing_graphs::source_parameters::{
    MultipleRasterOrSingleVectorSource, SingleRasterOrVectorSource, SingleRasterSource,
    SingleVectorSource,
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

/// The `BoxPlot` is a _plot operator_ that computes a box plot over
///
/// - a selection of numerical columns of a single vector dataset, or
/// - multiple raster datasets.
///
/// Thereby, the operator considers all data in the given query rectangle.
///
/// The boxes of the plot span the 1st and 3rd quartile and highlight the median.
/// The whiskers indicate the minimum and maximum values of the corresponding attribute or raster.
///
/// ## Errors
///
/// The operator returns an error in the following cases.
///
/// - Vector data: The `attribute` for one of the given `columnNames` is not numeric.
/// - Vector data: The `attribute` for one of the given `columnNames` does not exist.
/// - Raster data: The length of the `columnNames` parameter does not match the number of input rasters.
///
/// ## Notes
///
/// If your dataset contains `infinite` or `NAN` values, they are ignored for the computation.
/// Moreover, if your dataset contains more than `10.000`values (which is likely for rasters),
/// the median and quartiles are estimated using the P^2 algorithm described in:
///
/// R. Jain and I. Chlamtac, The P^2 algorithm for dynamic calculation of quantiles and
/// histograms without storing observations, Communications of the ACM,
/// Volume 28 (October), Number 10, 1985, p. 1076-1085.
/// <https://www.cse.wustl.edu/~jain/papers/ftp/psqr.pdf>
#[api_operator(
    examples(
        json!({
            "type": "BoxPlot",
            "params": {
                "columnNames": ["x", "y"]
            },
            "sources": {
                "source": {
                    "type": "OgrSource",
                    "params": {
                        "data": "ndvi"
                    }
                }
            }
        }),
        json!({
            "type": "BoxPlot",
            "params": {
                "columnNames": ["A", "B"]
            },
            "sources": {
                "source": [
                    {
                        "type": "GdalSource",
                        "params": {
                            "data": "ndvi"
                        }
                    },
                    {
                        "type": "GdalSource",
                        "params": {
                            "data": "temperature"
                        }
                    }
                ]
            }
        })
    )
)]
pub struct BoxPlot {
    pub params: BoxPlotParameters,
    pub sources: MultipleRasterOrSingleVectorSource,
}

/// The parameter spec for [`BoxPlot`].
///
/// ## Vector Data
///
/// In the case of vector data, the operator generates one box for each of the selected numerical attributes.
/// The operator returns an error if one of the selected attributes is not numeric.
///
/// ## Raster Data
///
/// For raster data, the operator generates one box for each input raster.
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BoxPlotParameters {
    /// ## Vector Data
    /// The names of the attributes to generate boxes for.
    ///
    /// ## Raster Data
    /// _Optional_: An alias for each input source.
    /// The operator will automatically name the boxes `Raster-1`, `Raster-2`, ... if this parameter is empty.
    /// If aliases are given, the number of aliases must match the number of input rasters.
    /// Otherwise an error is returned.
    #[serde(default)]
    #[schema(examples(json!(["temperature", "humidity"])))]
    pub column_names: Vec<String>,
}

impl TryFrom<BoxPlot> for geoengine_operators::plot::BoxPlot {
    type Error = anyhow::Error;
    fn try_from(value: BoxPlot) -> Result<Self, Self::Error> {
        let params = geoengine_operators::plot::BoxPlotParams {
            column_names: value.params.column_names,
        };
        let sources = value.sources.try_into()?;
        Ok(Self { params, sources })
    }
}

/// The `ClassHistogram` is a _plot operator_ that computes a histogram plot either over categorical attributes of a vector dataset or categorical values of a raster source.
/// The output is a plot in [Vega-Lite](https://vega.github.io/vega-lite/) specification.
///
/// For instance, you want to plot the frequencies of the classes of a categorical attribute of a feature collection.
/// Then you can use a class histogram to visualize and assess this.
///
/// ## Errors
///
/// The operator returns an error if…
///
/// - the selected column (`columnName`) does not exist or is not numeric,
/// - the source is a raster and the property `columnName` is set, or
/// - the input [`Measurement`](../datatypes/measurement) is not categorical.
///
/// The operator returns an error if
///
/// ## Notes
///
/// The operator only uses values of the categorical [`Measurement`](../datatypes/measurement).
/// It ignores missing or no-data values and values that are not covered by the [`Measurement`](../datatypes/measurement).
///
#[api_operator(examples(json!({
    "type": "ClassHistogram",
    "params": {
        "columnName": "class"
    },
    "sources": {
        "source": {
            "type": "OgrSource",
            "params": {
                "data": "landcover"
            }
        }
    }
})))]
pub struct ClassHistogram {
    pub params: ClassHistogramParameters,
    pub sources: SingleRasterOrVectorSource,
}

/// The parameter spec for `ClassHistogram`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClassHistogramParameters {
    /// The name of the attribute making up the x-axis of the histogram.
    /// Must be set for a vector sources, must not be set for rasters.
    #[schema(examples("class"))]
    pub column_name: Option<String>,
}

impl TryFrom<ClassHistogram> for geoengine_operators::plot::ClassHistogram {
    type Error = anyhow::Error;
    fn try_from(value: ClassHistogram) -> Result<Self, Self::Error> {
        let params = geoengine_operators::plot::ClassHistogramParams {
            column_name: value.params.column_name,
        };
        let sources = value.sources.try_into()?;
        Ok(Self { params, sources })
    }
}

/// The `FeatureAttributeValuesOverTime` is a _plot operator_ that computes a multi-line plot for feature attribute values over time.
/// For distinguishing features, the data requires an id column.
/// The output is a plot in Vega-Lite specification.
///
/// ```mermaid
/// xychart-beta
///     title "Selected Regional Trends"
///     x-axis ["Jan 05", "Feb 02", "Mar 02", "Mar 30"]
///     y-axis "Value" 0 --> 250
///     line "Papenburg" [118, 201, 198, 225]
///     line "Wismar" [185, 158, 177, 213]
///     line "Frederikshavn" [46, 50, 103, 124]
///     line "Helsingor" [44, 110, 112, 128]
/// ```
///
/// For instance, you want to plot the NDVI values of a feature collection of trees.
/// Then, you can use a multi-line plot to visualize the trees by their id.
///
/// ## Errors
///
/// The operator returns an error if the selected columns ( `idColumn` and `valueColumn`) do not exist or `valueColumn` is not numeric.
///
/// ## Notes
///
/// The operator processes a maximum of `20` different ids.
/// After recognizing more than `20` different ids, the operator ignores the rest.
///
#[api_operator(examples(json!({
    "type": "FeatureAttributeValuesOverTime",
    "params": {
        "idColumn": "id",
        "valueColumn": "temperature"
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "temperatures"
            }
        }
    }
})))]
pub struct FeatureAttributeValuesOverTime {
    pub params: FeatureAttributeValuesOverTimeParameters,
    pub sources: SingleVectorSource,
}

/// The parameter spec for `FeatureAttributeValuesOverTime`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FeatureAttributeValuesOverTimeParameters {
    /// The column name of the `id` attribute (one line per `id`.)
    #[schema(examples("id"))]
    pub id_column: String,
    /// The column name of the `value` attribute (y-axis values).
    #[schema(examples("temperature"))]
    pub value_column: String,
}

impl TryFrom<FeatureAttributeValuesOverTime>
    for geoengine_operators::plot::FeatureAttributeValuesOverTime
{
    type Error = anyhow::Error;
    fn try_from(value: FeatureAttributeValuesOverTime) -> Result<Self, Self::Error> {
        let params = geoengine_operators::plot::FeatureAttributeValuesOverTimeParams {
            id_column: value.params.id_column,
            value_column: value.params.value_column,
        };
        let sources = value.sources.try_into()?;
        Ok(Self { params, sources })
    }
}

/// The `MeanRasterPixelValuesOverTime` is a _plot operator_ that computes a time series plot of mean raster values.
///
/// For each time step in the raster time series, it computes one mean value.
/// The output is a plot in Vega-Lite specification.
///
/// For instance, you want to plot the mean temperature of a monthly raster time series.
/// Then, you can use this operator to generate a time series plot.
#[api_operator(examples(json!({
    "type": "MeanRasterPixelValuesOverTime",
    "params": {
        "timePosition": "start",
        "area": true
    },
    "sources": {
        "raster": {
            "type": "GdalSource",
            "params": {
                "data": "ndvi"
            }
        }
    }
})))]
pub struct MeanRasterPixelValuesOverTime {
    pub params: MeanRasterPixelValuesOverTimeParameters,
    pub sources: SingleRasterSource,
}

/// The parameter spec for `MeanRasterPixelValuesOverTime`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MeanRasterPixelValuesOverTimeParameters {
    /// Where should the x-axis (time) tick be positioned? At either time start, time end or in the center.
    #[schema(examples("start"))]
    pub time_position: MeanRasterPixelValuesOverTimePosition,
    /// Whether to fill the area under the curve. Defaults to `true`.
    #[serde(default = "default_true")]
    #[schema(examples(false))]
    pub area: bool,
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum MeanRasterPixelValuesOverTimePosition {
    Start,
    Center,
    End,
}

impl TryFrom<MeanRasterPixelValuesOverTime>
    for geoengine_operators::plot::MeanRasterPixelValuesOverTime
{
    type Error = anyhow::Error;
    fn try_from(value: MeanRasterPixelValuesOverTime) -> Result<Self, Self::Error> {
        let params = geoengine_operators::plot::MeanRasterPixelValuesOverTimeParams {
            time_position: match value.params.time_position {
                MeanRasterPixelValuesOverTimePosition::Start => {
                    geoengine_operators::plot::MeanRasterPixelValuesOverTimePosition::Start
                }
                MeanRasterPixelValuesOverTimePosition::Center => {
                    geoengine_operators::plot::MeanRasterPixelValuesOverTimePosition::Center
                }
                MeanRasterPixelValuesOverTimePosition::End => {
                    geoengine_operators::plot::MeanRasterPixelValuesOverTimePosition::End
                }
            },
            area: value.params.area,
        };
        let sources = value.sources.try_into()?;
        Ok(Self { params, sources })
    }
}

/// The `PieChart` is a _plot operator_ that computes a pie chart for a given vector dataset.
/// Moreover, the operator considers all data in the given query rectangle.
///
/// There are multiple variants on how to compute the slices of the pie chart.
/// In addition, it is possible to compute a donut chart instead of a standard pie chart.
///
/// ## Errors
///
/// The operator returns an error in the following cases.
///
/// - The `attribute` for the given `columnName` does not exist.
/// - The number of slices is too large: If the number of slices is greater than `32`, the operator returns an error.
///
/// ## Notes
///
/// If the attribute has a [`Measurement`](../datatypes/measurement) of type `Classification`, the operator uses the class name instead of the raw value.
///
#[api_operator(examples(json!({
    "type": "PieChart",
    "params": {
        "type": "count",
        "columnName": "class",
        "donut": false
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "land_use"
            }
        }
    }
})))]
pub struct PieChart {
    pub params: PieChartParameters,
    pub sources: SingleVectorSource,
}

/// The type of aggregation that is used to create the slices of the pie chart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum PieChartParameters {
    /// Count the distinct values of a single attribute.
    Count(PieChartCountType),
}

#[type_tag(value = "count")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PieChartCountType {
    /// The names of the attribute to generate pies for.  
    #[schema(example = "name")]
    pub column_name: String,
    /// Whether to render the chart as a donut.
    #[serde(default)]
    pub donut: bool,
}

impl TryFrom<PieChart> for geoengine_operators::plot::PieChart {
    type Error = anyhow::Error;
    fn try_from(value: PieChart) -> Result<Self, Self::Error> {
        let params = match value.params {
            PieChartParameters::Count(PieChartCountType {
                r#type: _,
                column_name,
                donut,
            }) => geoengine_operators::plot::PieChartParams::Count { column_name, donut },
        };
        let sources = value.sources.try_into()?;
        Ok(Self { params, sources })
    }
}

/// The `ScatterPlot` is a _plot operator_ that computes a scatter plot over two attributes of a vector dataset.
/// Thereby, the operator considers all data in the given query rectangle.
///
/// In case of more than `500` points to plot, the representation changes from a regular scatter plot
/// to a 2D Histogram with buckets determined from the underlying data.
///
/// ## Errors
///
/// The operator returns an error if one of the selected columns does not exist or is not numeric.
///
/// ## Notes
///
/// If your dataset contains `infinite` or `NAN` values, they are ignored for the computation. Moreover, if
/// your dataset contains more than `10.000` values, the buckets of the histogram are generated based on
/// those `10.000` values. Later values outside those bounds are ignored.
///
#[api_operator(examples(json!({
    "type": "ScatterPlot",
    "params": {
        "columnX": "temperature",
        "columnY": "humidity"
    },
    "sources": {
        "vector": {
            "type": "OgrSource",
            "params": {
                "data": "stations"
            }
        }
    }
})))]
pub struct ScatterPlot {
    pub params: ScatterPlotParameters,
    pub sources: SingleVectorSource,
}

/// The parameter spec for `ScatterPlot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScatterPlotParameters {
    /// The name of the attribute making up the x-axis of the plot.
    #[schema(example = "temperature")]
    pub column_x: String,
    /// The name of the attribute making up the y-axis of the plot.
    #[schema(example = "humidity")]
    pub column_y: String,
}

impl TryFrom<ScatterPlot> for geoengine_operators::plot::ScatterPlot {
    type Error = anyhow::Error;
    fn try_from(value: ScatterPlot) -> Result<Self, Self::Error> {
        let params = geoengine_operators::plot::ScatterPlotParams {
            column_x: value.params.column_x,
            column_y: value.params.column_y,
        };
        let sources = value.sources.try_into()?;
        Ok(Self { params, sources })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::{
        datatypes::Coordinate2D, processing_graphs::parameters::SpatialBoundsDerive,
    };
    use crate::api::model::{
        datatypes::NamedData,
        processing_graphs::{
            GdalSource, GdalSourceParameters, PlotOperator, RasterOperator, VectorOperator,
            source::{MockPointSource, MockPointSourceParameters, OgrSource, OgrSourceParameters},
            source_parameters::{
                MultipleRasterOrSingleVectorOperator, MultipleRasterOrSingleVectorSource,
                SingleRasterOrVectorOperator, SingleRasterOrVectorSource,
            },
        },
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
            sources: SingleRasterOrVectorSource {
                source: SingleRasterOrVectorOperator::Vector(VectorOperator::OgrSource(
                    OgrSource {
                        r#type: Default::default(),
                        params: OgrSourceParameters {
                            data: NamedData::with_system_name("ndvi"),
                            attribute_projection: None,
                            attribute_filters: None,
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
            sources: SingleRasterOrVectorSource {
                source: SingleRasterOrVectorOperator::Vector(VectorOperator::OgrSource(
                    OgrSource {
                        r#type: Default::default(),
                        params: OgrSourceParameters {
                            data: NamedData::with_system_name("ndvi"),
                            attribute_projection: None,
                            attribute_filters: None,
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
            sources: SingleRasterOrVectorSource {
                source: SingleRasterOrVectorOperator::Vector(VectorOperator::OgrSource(
                    OgrSource {
                        r#type: Default::default(),
                        params: OgrSourceParameters {
                            data: NamedData::with_system_name("ndvi"),
                            attribute_projection: None,
                            attribute_filters: None,
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
                            data: NamedData::with_system_name("ndvi"),
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

    #[test]
    fn it_converts_box_plot_example_to_operator() {
        let box_plot = BoxPlot {
            r#type: Default::default(),
            params: BoxPlotParameters {
                column_names: vec!["temperature".to_string(), "humidity".to_string()],
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

        let plot_operator = PlotOperator::BoxPlot(box_plot);
        Box::<dyn OperatorsPlotOperatorTrait>::try_from(plot_operator)
            .map(|_| ())
            .expect("box plot with MockPointSource must convert to operator");
    }

    #[test]
    fn it_converts_class_histogram_example_to_operator() {
        let class_histogram = ClassHistogram {
            r#type: Default::default(),
            params: ClassHistogramParameters {
                column_name: Some("class".to_string()),
            },
            sources: SingleRasterOrVectorSource {
                source: SingleRasterOrVectorOperator::Vector(VectorOperator::OgrSource(
                    OgrSource {
                        r#type: Default::default(),
                        params: OgrSourceParameters {
                            data: NamedData::with_system_name("landcover"),
                            attribute_projection: None,
                            attribute_filters: None,
                        },
                    },
                )),
            },
        };

        let plot_operator = PlotOperator::ClassHistogram(class_histogram);
        Box::<dyn OperatorsPlotOperatorTrait>::try_from(plot_operator)
            .map(|_| ())
            .expect("class histogram with OgrSource must convert to operator");
    }

    #[test]
    fn it_converts_feature_attribute_values_over_time_example_to_operator() {
        let feature_plot = FeatureAttributeValuesOverTime {
            r#type: Default::default(),
            params: FeatureAttributeValuesOverTimeParameters {
                id_column: "id".to_string(),
                value_column: "temperature".to_string(),
            },
            sources: SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 1.0, y: 2.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            },
        };

        let plot_operator = PlotOperator::FeatureAttributeValuesOverTime(feature_plot);
        Box::<dyn OperatorsPlotOperatorTrait>::try_from(plot_operator)
            .map(|_| ())
            .expect("feature attribute over time must convert to operator");
    }

    #[test]
    fn it_converts_mean_raster_pixel_values_over_time_example_to_operator() {
        let mean_raster = MeanRasterPixelValuesOverTime {
            r#type: Default::default(),
            params: MeanRasterPixelValuesOverTimeParameters {
                time_position: MeanRasterPixelValuesOverTimePosition::Center,
                area: true,
            },
            sources: SingleRasterSource {
                raster: RasterOperator::GdalSource(GdalSource {
                    r#type: Default::default(),
                    params: GdalSourceParameters {
                        data: NamedData::with_system_name("temperature"),
                        overview_level: None,
                    },
                }),
            },
        };

        let plot_operator = PlotOperator::MeanRasterPixelValuesOverTime(mean_raster);
        Box::<dyn OperatorsPlotOperatorTrait>::try_from(plot_operator)
            .map(|_| ())
            .expect("mean raster pixel values over time must convert to operator");
    }

    #[test]
    fn it_converts_pie_chart_example_to_operator() {
        let pie_chart = PieChart {
            r#type: Default::default(),
            params: PieChartParameters::Count(PieChartCountType {
                r#type: Default::default(),
                column_name: "land_use".to_string(),
                donut: false,
            }),
            sources: SingleVectorSource {
                vector: VectorOperator::OgrSource(OgrSource {
                    r#type: Default::default(),
                    params: OgrSourceParameters {
                        data: NamedData::with_system_name("classes"),
                        attribute_projection: None,
                        attribute_filters: None,
                    },
                }),
            },
        };

        let plot_operator = PlotOperator::PieChart(pie_chart);
        Box::<dyn OperatorsPlotOperatorTrait>::try_from(plot_operator)
            .map(|_| ())
            .expect("pie chart with OgrSource must convert to operator");
    }

    #[test]
    fn it_converts_scatter_plot_example_to_operator() {
        let scatter_plot = ScatterPlot {
            r#type: Default::default(),
            params: ScatterPlotParameters {
                column_x: "temperature".to_string(),
                column_y: "humidity".to_string(),
            },
            sources: SingleVectorSource {
                vector: VectorOperator::MockPointSource(MockPointSource {
                    r#type: Default::default(),
                    params: MockPointSourceParameters {
                        points: vec![Coordinate2D { x: 1.0, y: 2.0 }],
                        spatial_bounds: SpatialBoundsDerive::Derive(Default::default()),
                    },
                }),
            },
        };

        let plot_operator = PlotOperator::ScatterPlot(scatter_plot);
        Box::<dyn OperatorsPlotOperatorTrait>::try_from(plot_operator)
            .map(|_| ())
            .expect("scatter plot with MockPointSource must convert to operator");
    }
}
