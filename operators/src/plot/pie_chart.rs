use crate::engine::{QueryProcessor, SingleVectorSource};
use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedVectorOperator, Operator, OperatorName,
    PlotOperator, PlotQueryProcessor, PlotResultDescriptor, QueryContext, TypedPlotQueryProcessor,
    TypedVectorQueryProcessor,
};
use crate::error::Error;
use crate::util::Result;
use async_trait::async_trait;
use futures::StreamExt;
use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::plots::{Plot, PlotData};
use geoengine_datatypes::primitives::{FeatureDataRef, Measurement, VectorQueryRectangle};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use std::collections::HashMap;


pub const PIE_CHART_OPERATOR_NAME: &str = "PieChart";

/// If the number of slices in the result is greater than this, the operator will fail.
pub const MAX_NUMBER_OF_SLICES: usize = 32;

/// A pie chart plot about a column of a vector input.
pub type PieChart = Operator<PieChartParams, SingleVectorSource>;

impl OperatorName for PieChart {
    const TYPE_NAME: &'static str = PIE_CHART_OPERATOR_NAME;
}

/// The parameter spec for `PieChart`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum PieChartParams {
    /// Count the distinct values of a column
    #[serde(rename_all = "camelCase")]
    Count {
        /// Name of the (numeric) attribute to compute the histogram on. Fails if set for rasters.
        column_name: String,
        /// Whether to display the pie chart as a normal pie chart or as a donut chart.
        /// Defaults to `false`.
        #[serde(default)]
        donut: bool,
    },
    // TODO: another useful method would be `Sum` which sums up all values of a column A for a group column B
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for PieChart {
    async fn _initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let vector_source = self.sources.vector.initialize(context).await?;

        let in_desc = vector_source.result_descriptor().clone();

        match self.params {
            PieChartParams::Count { column_name, donut } => {
                let  Some(column_measurement) = in_desc
                    .column_measurement(&column_name) else {
                        return Err(Error::ColumnDoesNotExist {
                            column: column_name,
                        });
                    };

                let mut column_label = column_measurement.to_string();
                if column_label.is_empty() {
                    // in case of `Measurement::Unitless`
                    column_label = column_name.clone();
                }

                let class_mapping =
                    if let Measurement::Classification(measurement) = &column_measurement {
                        Some(measurement.classes.clone())
                    } else {
                        None
                    };

                Ok(InitializedCountPieChart::new(
                    vector_source,
                    in_desc.into(),
                    column_name.clone(),
                    column_label,
                    class_mapping,
                    donut,
                )
                .boxed())
            }
        }
    }

    span_fn!(PieChart);
}

/// The initialization of `Histogram`
pub struct InitializedCountPieChart<Op> {
    source: Op,
    result_descriptor: PlotResultDescriptor,
    column_name: String,
    column_label: String,
    class_mapping: Option<HashMap<u8, String>>,
    donut: bool,
}

impl<Op> InitializedCountPieChart<Op> {
    pub fn new(
        source: Op,
        result_descriptor: PlotResultDescriptor,
        column_name: String,
        column_label: String,
        class_mapping: Option<HashMap<u8, String>>,
        donut: bool,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            column_name,
            column_label,
            class_mapping,
            donut,
        }
    }
}

impl InitializedPlotOperator for InitializedCountPieChart<Box<dyn InitializedVectorOperator>> {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let processor = CountPieChartVectorQueryProcessor {
            input: self.source.query_processor()?,
            column_label: self.column_label.clone(),
            column_name: self.column_name.clone(),
            class_mapping: self.class_mapping.clone(),
            donut: self.donut,
        };

        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }
}

/// A query processor that calculates the Histogram about its vector inputs.
pub struct CountPieChartVectorQueryProcessor {
    input: TypedVectorQueryProcessor,
    column_label: String,
    column_name: String,
    class_mapping: Option<HashMap<u8, String>>,
    donut: bool,
}

#[async_trait]
impl PlotQueryProcessor for CountPieChartVectorQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        PIE_CHART_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        self.process(query, ctx).await
    }
}

/// Creates an iterator over all values as string
/// Null-values are empty strings.
pub fn feature_data_strings_iter<'f>(
    feature_data: &'f FeatureDataRef,
    class_mapping: Option<&'f HashMap<u8, String>>,
) -> Box<dyn Iterator<Item = String> + 'f> {
    match (feature_data, class_mapping) {
        (FeatureDataRef::Category(feature_data_ref), Some(class_mapping)) => {
            return Box::new(feature_data_ref.as_ref().iter().map(|v| {
                class_mapping
                    .get(v)
                    .map_or_else(String::new, ToString::to_string)
            }));
        }
        (FeatureDataRef::Int(feature_data_ref), Some(class_mapping)) => {
            return Box::new(feature_data_ref.as_ref().iter().map(|v| {
                class_mapping
                    .get(&(*v as u8))
                    .map_or_else(String::new, ToString::to_string)
            }));
        }
        (FeatureDataRef::Float(feature_data_ref), Some(class_mapping)) => {
            return Box::new(feature_data_ref.as_ref().iter().map(|v| {
                class_mapping
                    .get(&(*v as u8))
                    .map_or_else(String::new, ToString::to_string)
            }));
        }
        _ => {
            // no special treatment for other types
        }
    }

    feature_data.strings_iter()
}

impl CountPieChartVectorQueryProcessor {
    async fn process<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<<CountPieChartVectorQueryProcessor as PlotQueryProcessor>::OutputFormat> {
        let mut slices: HashMap<String, f64> = HashMap::new();

        // TODO: parallelize

        call_on_generic_vector_processor!(&self.input, processor => {
            let mut query = processor.query(query, ctx).await?;

            while let Some(collection) = query.next().await {
                let collection = collection?;

                let feature_data = collection.data(&self.column_name)?;

                let feature_data_strings = feature_data_strings_iter(&feature_data, self.class_mapping.as_ref());

                for v in feature_data_strings {
                    if v.is_empty() {
                        continue; // ignore no data
                    }

                    *slices.entry(v).or_insert(0.0) += 1.0;
                }

                if slices.len() > MAX_NUMBER_OF_SLICES {
                    return Err(PieChartError::TooManySlices.into());
                }
            }
        });

        // TODO: display NO-DATA count?

        let bar_chart = geoengine_datatypes::plots::PieChart::new(
            slices.into_iter().collect(),
            self.column_label.clone(),
            self.donut,
        )?;
        let chart = bar_chart.to_vega_embeddable(false)?;

        Ok(chart)
    }
}

#[derive(Debug, Snafu, Clone, PartialEq, Eq)]
#[snafu(
    visibility(pub(crate)),
    context(suffix(false)), // disables default `Snafu` suffix
    module(error),
)]
pub enum PieChartError {
    #[snafu(display(
        "The number of slices is too high. Maximum is {}.",
        MAX_NUMBER_OF_SLICES
    ))]
    TooManySlices,
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, StaticMetaData, VectorColumnInfo,
        VectorOperator, VectorResultDescriptor,
    };
    use crate::mock::MockFeatureCollectionSource;
    use crate::source::{
        AttributeFilter, OgrSource, OgrSourceColumnSpec, OgrSourceDataset,
        OgrSourceDatasetTimeType, OgrSourceErrorSpec, OgrSourceParameters,
    };
    use crate::test_data;
    use geoengine_datatypes::dataset::{DataId, DatasetId};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, FeatureDataType, NoGeometry, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::util::Identifier;
    use geoengine_datatypes::{
        collections::{DataCollection, VectorDataType},
        primitives::MultiPoint,
    };
    use serde_json::json;

    #[test]
    fn serialization() {
        let pie_chart = PieChart {
            params: PieChartParams::Count {
                column_name: "my_column".to_string(),
                donut: false,
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "PieChart",
            "params": {
                "type": "count",
                "columnName": "my_column",
            },
            "sources": {
                "vector": {
                    "type": "MockFeatureCollectionSourceMultiPoint",
                    "params": {
                        "collections": [],
                        "spatialReference": "EPSG:4326",
                        "measurements": {},
                    }
                }
            }
        })
        .to_string();

        let deserialized: PieChart = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, pie_chart.params);
    }

    #[tokio::test]
    async fn vector_data_with_classification() {
        let measurement = Measurement::classification(
            "foo".to_string(),
            [
                (1, "A".to_string()),
                (2, "B".to_string()),
                (3, "C".to_string()),
            ]
            .into_iter()
            .collect(),
        );

        let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
            vec![
                DataCollection::from_slices(
                    &[] as &[NoGeometry],
                    &[TimeInterval::default(); 8],
                    &[("foo", FeatureData::Int(vec![1, 1, 2, 2, 3, 3, 1, 2]))],
                )
                .unwrap(),
                DataCollection::from_slices(
                    &[] as &[NoGeometry],
                    &[TimeInterval::default(); 4],
                    &[("foo", FeatureData::Int(vec![1, 1, 2, 3]))],
                )
                .unwrap(),
            ],
            [("foo".to_string(), measurement)].into_iter().collect(),
        )
        .boxed();

        let pie_chart = PieChart {
            params: PieChartParams::Count {
                column_name: "foo".to_string(),
                donut: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = pie_chart
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::PieChart::new(
                [
                    ("A".to_string(), 5.),
                    ("B".to_string(), 4.),
                    ("C".to_string(), 3.),
                ]
                .into(),
                "foo".to_string(),
                false,
            )
            .unwrap()
            .to_vega_embeddable(false)
            .unwrap()
        );
    }

    #[tokio::test]
    async fn vector_data_with_nulls() {
        let measurement = Measurement::continuous("foo".to_string(), None);

        let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
            vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 6],
                &[(
                    "foo",
                    FeatureData::NullableFloat(vec![
                        Some(1.),
                        Some(2.),
                        None,
                        Some(1.),
                        None,
                        Some(3.),
                    ]),
                )],
            )
            .unwrap()],
            [("foo".to_string(), measurement)].into_iter().collect(),
        )
        .boxed();

        let pie_chart = PieChart {
            params: PieChartParams::Count {
                column_name: "foo".to_string(),
                donut: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = pie_chart
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::PieChart::new(
                [
                    ("1".to_string(), 2.),
                    ("2".to_string(), 1.),
                    ("3".to_string(), 1.),
                ]
                .into(),
                "foo".to_string(),
                false,
            )
            .unwrap()
            .to_vega_embeddable(false)
            .unwrap()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn text_attribute() {
        let dataset_id = DatasetId::new();

        let mut execution_context = MockExecutionContext::test_default();
        execution_context.add_meta_data::<_, _, VectorQueryRectangle>(
            DataId::Internal { dataset_id },
            Box::new(StaticMetaData {
                loading_info: OgrSourceDataset {
                    file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
                    layer_name: "ne_10m_ports".to_string(),
                    data_type: Some(VectorDataType::MultiPoint),
                    time: OgrSourceDatasetTimeType::None,
                    default_geometry: None,
                    columns: Some(OgrSourceColumnSpec {
                        format_specifics: None,
                        x: String::new(),
                        y: None,
                        int: vec!["natlscale".to_string()],
                        float: vec!["scalerank".to_string()],
                        text: vec![
                            "featurecla".to_string(),
                            "name".to_string(),
                            "website".to_string(),
                        ],
                        bool: vec![],
                        datetime: vec![],
                        rename: None,
                    }),
                    force_ogr_time_filter: false,
                    force_ogr_spatial_filter: false,
                    on_error: OgrSourceErrorSpec::Ignore,
                    sql_query: None,
                    attribute_query: None,
                },
                result_descriptor: VectorResultDescriptor {
                    data_type: VectorDataType::MultiPoint,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    columns: [
                        (
                            "natlscale".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Float,
                                measurement: Measurement::Unitless,
                            },
                        ),
                        (
                            "scalerank".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Int,
                                measurement: Measurement::Unitless,
                            },
                        ),
                        (
                            "featurecla".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless,
                            },
                        ),
                        (
                            "name".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless,
                            },
                        ),
                        (
                            "website".to_string(),
                            VectorColumnInfo {
                                data_type: FeatureDataType::Text,
                                measurement: Measurement::Unitless,
                            },
                        ),
                    ]
                    .iter()
                    .cloned()
                    .collect(),
                    time: None,
                    bbox: None,
                },
                phantom: Default::default(),
            }),
        );

        let pie_chart = PieChart {
            params: PieChartParams::Count {
                column_name: "name".to_string(),
                donut: false,
            },
            sources: OgrSource {
                params: OgrSourceParameters {
                    data: dataset_id.into(),
                    attribute_projection: None,
                    attribute_filters: None,
                },
            }
            .boxed()
            .into(),
        };

        let query_processor = pie_chart
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap_err();

        assert_eq!(
            result.to_string(),
            "PieChart: The number of slices is too high. Maximum is 32."
        );

        let pie_chart = PieChart {
            params: PieChartParams::Count {
                column_name: "name".to_string(),
                donut: false,
            },
            sources: OgrSource {
                params: OgrSourceParameters {
                    data: dataset_id.into(),
                    attribute_projection: None,
                    attribute_filters: Some(vec![AttributeFilter {
                        attribute: "name".to_string(),
                        ranges: vec![("E".to_string()..="F".to_string()).into()],
                        keep_nulls: false,
                    }]),
                },
            }
            .boxed()
            .into(),
        };

        let query_processor = pie_chart
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::PieChart::new(
                [
                    ("Esquimalt".to_string(), 1.),
                    ("Eckernforde".to_string(), 1.),
                    ("Escanaba".to_string(), 1.),
                    ("Esperance".to_string(), 1.),
                    ("Eden".to_string(), 1.),
                    ("Esmeraldas".to_string(), 1.),
                    ("Europoort".to_string(), 1.),
                    ("Elat".to_string(), 1.),
                    ("Emden".to_string(), 1.),
                    ("Esbjerg".to_string(), 1.),
                    ("Ensenada".to_string(), 1.),
                    ("East London".to_string(), 1.),
                    ("Erie".to_string(), 1.),
                    ("Eureka".to_string(), 1.),
                ]
                .into(),
                "name".to_string(),
                false,
            )
            .unwrap()
            .to_vega_embeddable(false)
            .unwrap()
        );
    }

    #[tokio::test]
    async fn empty_feature_collection() {
        let measurement = Measurement::classification(
            "foo".to_string(),
            [(1, "A".to_string())].into_iter().collect(),
        );

        let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
            vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[] as &[TimeInterval],
                &[("foo", FeatureData::Float(vec![]))],
            )
            .unwrap()],
            [("foo".to_string(), measurement)].into_iter().collect(),
        )
        .boxed();

        let pie_chart = PieChart {
            params: PieChartParams::Count {
                column_name: "foo".to_string(),
                donut: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = pie_chart
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::PieChart::new(
                Default::default(),
                "foo".to_string(),
                false,
            )
            .unwrap()
            .to_vega_embeddable(false)
            .unwrap()
        );
    }

    #[tokio::test]
    async fn feature_collection_with_one_feature() {
        let measurement = Measurement::classification(
            "foo".to_string(),
            [(5, "A".to_string())].into_iter().collect(),
        );

        let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
            vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default()],
                &[("foo", FeatureData::Float(vec![5.0]))],
            )
            .unwrap()],
            [("foo".to_string(), measurement)].into_iter().collect(),
        )
        .boxed();

        let pie_chart = PieChart {
            params: PieChartParams::Count {
                column_name: "foo".to_string(),
                donut: false,
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = pie_chart
            .boxed()
            .initialize(&execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                VectorQueryRectangle {
                    spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
                        .unwrap(),
                    time_interval: TimeInterval::default(),
                    spatial_resolution: SpatialResolution::one(),
                },
                &MockQueryContext::new(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            geoengine_datatypes::plots::PieChart::new(
                [("A".to_string(), 1.),].into(),
                "foo".to_string(),
                false,
            )
            .unwrap()
            .to_vega_embeddable(false)
            .unwrap()
        );
    }
}
