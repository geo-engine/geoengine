use crate::engine::{CreateSpan, QueryProcessor, SingleVectorSource};
use crate::error;
use crate::error::Error;
use crate::util::Result;
use crate::{
    engine::{
        ExecutionContext, InitializedPlotOperator, InitializedRasterOperator,
        InitializedVectorOperator, Operator, OperatorName, PlotOperator, PlotQueryProcessor,
        PlotResultDescriptor, QueryContext, SingleRasterOrVectorSource, TypedPlotQueryProcessor,
        TypedRasterQueryProcessor, TypedVectorQueryProcessor,
    },
    util::input::RasterOrVectorOperator,
};
use async_trait::async_trait;
use futures::StreamExt;
use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::plots::{BarChart, Plot, PlotData};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, ClassificationMeasurement, ContinuousMeasurement,
    FeatureDataRef, FeatureDataType, Measurement, VectorQueryRectangle,
};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use std::collections::HashMap;
use std::fmt::Display;
use tracing::{span, Level};

pub const PIE_CHART_OPERATOR_NAME: &str = "PieChart";

/// If the number of pies in the result is greater than this, the operator will fail.
pub const MAX_NUMBER_OF_PIES: usize = 32;

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
                let Some(column_data_type) = in_desc
                    .column_data_type(&column_name) else {
                        return Err(Error::ColumnDoesNotExist {
                            column: column_name,
                        });
                    };

                let Some(column_measurement) = in_desc
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

impl CountPieChartVectorQueryProcessor {
    async fn process<'p>(
        &'p self,
        query: VectorQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<<CountPieChartVectorQueryProcessor as PlotQueryProcessor>::OutputFormat> {
        let mut pies: HashMap<String, f64> = HashMap::new();

        // TODO: parallelize

        call_on_generic_vector_processor!(&self.input, processor => {
            let mut query = processor.query(query, ctx).await?;

            while let Some(collection) = query.next().await {
                let collection = collection?;

                let feature_data = collection.data(&self.column_name)?;

                match feature_data {
                    FeatureDataRef::Category(_) => todo!(),
                    FeatureDataRef::Int(_) => todo!(),
                    FeatureDataRef::Float(_) => todo!(),
                    FeatureDataRef::Text(_) => todo!(),
                    FeatureDataRef::Bool(_) => todo!(),
                    FeatureDataRef::DateTime(_) => todo!(),
                }

                for v in feature_data.float_options_iter() {
                    match v {
                        None => (), // ignore no data
                        Some(index) => if let Some(count) = class_counts.get_mut(&(index as u8)) {
                            *count += 1;
                        },
                        // else… ignore values that are not in the class list
                    }

                }

            }
        });

        // TODO: display NO-DATA count?

        let bar_chart = geoengine_datatypes::plots::PieChart::new(
            pies.into_iter().collect(),
            self.column_label.clone(),
            self.donut,
        )?;
        let chart = bar_chart.to_vega_embeddable(false)?;

        Ok(chart)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, RasterOperator,
        RasterResultDescriptor, StaticMetaData, VectorColumnInfo, VectorOperator,
        VectorResultDescriptor,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use crate::source::{
        OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
    };
    use crate::test_data;
    use geoengine_datatypes::dataset::{DataId, DatasetId};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, DateTime, FeatureData, NoGeometry, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        Grid2D, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::util::Identifier;
    use geoengine_datatypes::{
        collections::{DataCollection, VectorDataType},
        primitives::MultiPoint,
    };
    use serde_json::json;

    // #[test]
    // fn serialization() {
    //     let histogram = PieChart {
    //         params: PieChartParams {
    //             column_name: Some("foobar".to_string()),
    //         },
    //         sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
    //             .boxed()
    //             .into(),
    //     };

    //     let serialized = json!({
    //         "type": "ClassHistogram",
    //         "params": {
    //             "columnName": "foobar",
    //         },
    //         "sources": {
    //             "source": {
    //                 "type": "MockFeatureCollectionSourceMultiPoint",
    //                 "params": {
    //                     "collections": [],
    //                     "spatialReference": "EPSG:4326",
    //                     "measurements": {},
    //                 }
    //             }
    //         }
    //     })
    //     .to_string();

    //     let deserialized: PieChart = serde_json::from_str(&serialized).unwrap();

    //     assert_eq!(deserialized.params, histogram.params);
    // }

    // #[tokio::test]
    // async fn column_name_for_raster_source() {
    //     let histogram = PieChart {
    //         params: PieChartParams {
    //             column_name: Some("foo".to_string()),
    //         },
    //         sources: mock_raster_source().into(),
    //     };

    //     let execution_context = MockExecutionContext::test_default();

    //     assert!(histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .is_err());
    // }

    // fn mock_raster_source() -> Box<dyn RasterOperator> {
    //     MockRasterSource {
    //         params: MockRasterSourceParams {
    //             data: vec![RasterTile2D::new_with_tile_info(
    //                 TimeInterval::default(),
    //                 TileInformation {
    //                     global_geo_transform: TestDefault::test_default(),
    //                     global_tile_position: [0, 0].into(),
    //                     tile_size_in_pixels: [3, 2].into(),
    //                 },
    //                 Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
    //                     .unwrap()
    //                     .into(),
    //             )],
    //             result_descriptor: RasterResultDescriptor {
    //                 data_type: RasterDataType::U8,
    //                 spatial_reference: SpatialReference::epsg_4326().into(),
    //                 measurement: Measurement::classification(
    //                     "test-class".to_string(),
    //                     [
    //                         (1, "A".to_string()),
    //                         (2, "B".to_string()),
    //                         (3, "C".to_string()),
    //                         (4, "D".to_string()),
    //                         (5, "E".to_string()),
    //                         (6, "F".to_string()),
    //                     ]
    //                     .into_iter()
    //                     .collect(),
    //                 ),
    //                 time: None,
    //                 bbox: None,
    //                 resolution: None,
    //             },
    //         },
    //     }
    //     .boxed()
    // }

    // #[tokio::test]
    // async fn simple_raster() {
    //     let tile_size_in_pixels = [3, 2].into();
    //     let tiling_specification = TilingSpecification {
    //         origin_coordinate: [0.0, 0.0].into(),
    //         tile_size_in_pixels,
    //     };
    //     let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

    //     let histogram = PieChart {
    //         params: PieChartParams { column_name: None },
    //         sources: mock_raster_source().into(),
    //     };

    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();

    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
    //                 time_interval: TimeInterval::default(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::new(ChunkByteSize::MIN),
    //         )
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         result,
    //         BarChart::new(
    //             [
    //                 ("A".to_string(), 1),
    //                 ("B".to_string(), 1),
    //                 ("C".to_string(), 1),
    //                 ("D".to_string(), 1),
    //                 ("E".to_string(), 1),
    //                 ("F".to_string(), 1),
    //             ]
    //             .into_iter()
    //             .collect(),
    //             "test-class".to_string(),
    //             "Frequency".to_string()
    //         )
    //         .to_vega_embeddable(true)
    //         .unwrap()
    //     );
    // }

    // #[tokio::test]
    // async fn vector_data() {
    //     let measurement = Measurement::classification(
    //         "foo".to_string(),
    //         [
    //             (1, "A".to_string()),
    //             (2, "B".to_string()),
    //             (3, "C".to_string()),
    //         ]
    //         .into_iter()
    //         .collect(),
    //     );

    //     let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
    //         vec![
    //             DataCollection::from_slices(
    //                 &[] as &[NoGeometry],
    //                 &[TimeInterval::default(); 8],
    //                 &[("foo", FeatureData::Int(vec![1, 1, 2, 2, 3, 3, 1, 2]))],
    //             )
    //             .unwrap(),
    //             DataCollection::from_slices(
    //                 &[] as &[NoGeometry],
    //                 &[TimeInterval::default(); 4],
    //                 &[("foo", FeatureData::Int(vec![1, 1, 2, 3]))],
    //             )
    //             .unwrap(),
    //         ],
    //         [("foo".to_string(), measurement)].into_iter().collect(),
    //     )
    //     .boxed();

    //     let histogram = PieChart {
    //         params: PieChartParams {
    //             column_name: Some("foo".to_string()),
    //         },
    //         sources: vector_source.into(),
    //     };

    //     let execution_context = MockExecutionContext::test_default();

    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();

    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
    //                     .unwrap(),
    //                 time_interval: TimeInterval::default(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::new(ChunkByteSize::MIN),
    //         )
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         result,
    //         BarChart::new(
    //             [
    //                 ("A".to_string(), 5),
    //                 ("B".to_string(), 4),
    //                 ("C".to_string(), 3),
    //             ]
    //             .into_iter()
    //             .collect(),
    //             "foo".to_string(),
    //             "Frequency".to_string()
    //         )
    //         .to_vega_embeddable(true)
    //         .unwrap()
    //     );
    // }

    // #[tokio::test]
    // async fn vector_data_with_nulls() {
    //     let measurement = Measurement::classification(
    //         "foo".to_string(),
    //         [
    //             (1, "A".to_string()),
    //             (2, "B".to_string()),
    //             (4, "C".to_string()),
    //         ]
    //         .into_iter()
    //         .collect(),
    //     );

    //     let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
    //         vec![DataCollection::from_slices(
    //             &[] as &[NoGeometry],
    //             &[TimeInterval::default(); 6],
    //             &[(
    //                 "foo",
    //                 FeatureData::NullableFloat(vec![
    //                     Some(1.),
    //                     Some(2.),
    //                     None,
    //                     Some(4.),
    //                     None,
    //                     Some(5.),
    //                 ]),
    //             )],
    //         )
    //         .unwrap()],
    //         [("foo".to_string(), measurement)].into_iter().collect(),
    //     )
    //     .boxed();

    //     let histogram = PieChart {
    //         params: PieChartParams {
    //             column_name: Some("foo".to_string()),
    //         },
    //         sources: vector_source.into(),
    //     };

    //     let execution_context = MockExecutionContext::test_default();

    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();

    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
    //                     .unwrap(),
    //                 time_interval: TimeInterval::default(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::new(ChunkByteSize::MIN),
    //         )
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         result,
    //         BarChart::new(
    //             [
    //                 ("A".to_string(), 1),
    //                 ("B".to_string(), 1),
    //                 ("C".to_string(), 1),
    //             ]
    //             .into_iter()
    //             .collect(),
    //             "foo".to_string(),
    //             "Frequency".to_string()
    //         )
    //         .to_vega_embeddable(true)
    //         .unwrap()
    //     );
    // }

    // #[tokio::test]
    // #[allow(clippy::too_many_lines)]
    // async fn text_attribute() {
    //     let dataset_id = DatasetId::new();

    //     let workflow = serde_json::json!({
    //         "type": "Histogram",
    //         "params": {
    //             "columnName": "featurecla",
    //         },
    //         "sources": {
    //             "source": {
    //                 "type": "OgrSource",
    //                 "params": {
    //                     "data": {
    //                         "type": "internal",
    //                         "datasetId": dataset_id
    //                     },
    //                     "attributeProjection": null
    //                 },
    //             }
    //         }
    //     });
    //     let histogram: PieChart = serde_json::from_value(workflow).unwrap();

    //     let mut execution_context = MockExecutionContext::test_default();
    //     execution_context.add_meta_data::<_, _, VectorQueryRectangle>(
    //         DataId::Internal { dataset_id },
    //         Box::new(StaticMetaData {
    //             loading_info: OgrSourceDataset {
    //                 file_name: test_data!("vector/data/ne_10m_ports/ne_10m_ports.shp").into(),
    //                 layer_name: "ne_10m_ports".to_string(),
    //                 data_type: Some(VectorDataType::MultiPoint),
    //                 time: OgrSourceDatasetTimeType::None,
    //                 default_geometry: None,
    //                 columns: Some(OgrSourceColumnSpec {
    //                     format_specifics: None,
    //                     x: String::new(),
    //                     y: None,
    //                     int: vec!["natlscale".to_string()],
    //                     float: vec!["scalerank".to_string()],
    //                     text: vec![
    //                         "featurecla".to_string(),
    //                         "name".to_string(),
    //                         "website".to_string(),
    //                     ],
    //                     bool: vec![],
    //                     datetime: vec![],
    //                     rename: None,
    //                 }),
    //                 force_ogr_time_filter: false,
    //                 force_ogr_spatial_filter: false,
    //                 on_error: OgrSourceErrorSpec::Ignore,
    //                 sql_query: None,
    //                 attribute_query: None,
    //             },
    //             result_descriptor: VectorResultDescriptor {
    //                 data_type: VectorDataType::MultiPoint,
    //                 spatial_reference: SpatialReference::epsg_4326().into(),
    //                 columns: [
    //                     (
    //                         "natlscale".to_string(),
    //                         VectorColumnInfo {
    //                             data_type: FeatureDataType::Float,
    //                             measurement: Measurement::Unitless,
    //                         },
    //                     ),
    //                     (
    //                         "scalerank".to_string(),
    //                         VectorColumnInfo {
    //                             data_type: FeatureDataType::Int,
    //                             measurement: Measurement::Unitless,
    //                         },
    //                     ),
    //                     (
    //                         "featurecla".to_string(),
    //                         VectorColumnInfo {
    //                             data_type: FeatureDataType::Text,
    //                             measurement: Measurement::Unitless,
    //                         },
    //                     ),
    //                     (
    //                         "name".to_string(),
    //                         VectorColumnInfo {
    //                             data_type: FeatureDataType::Text,
    //                             measurement: Measurement::Unitless,
    //                         },
    //                     ),
    //                     (
    //                         "website".to_string(),
    //                         VectorColumnInfo {
    //                             data_type: FeatureDataType::Text,
    //                             measurement: Measurement::Unitless,
    //                         },
    //                     ),
    //                 ]
    //                 .iter()
    //                 .cloned()
    //                 .collect(),
    //                 time: None,
    //                 bbox: None,
    //             },
    //             phantom: Default::default(),
    //         }),
    //     );

    //     if let Err(Error::InvalidOperatorSpec { reason }) =
    //         histogram.boxed().initialize(&execution_context).await
    //     {
    //         assert_eq!(reason, "column `featurecla` must be numerical");
    //     } else {
    //         panic!("we currently don't support text features, but this went through");
    //     }
    // }

    // #[tokio::test]
    // async fn no_data_raster() {
    //     let tile_size_in_pixels = [3, 2].into();
    //     let tiling_specification = TilingSpecification {
    //         origin_coordinate: [0.0, 0.0].into(),
    //         tile_size_in_pixels,
    //     };
    //     let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

    //     let measurement = Measurement::classification(
    //         "foo".to_string(),
    //         [(1, "A".to_string())].into_iter().collect(),
    //     );

    //     let histogram = PieChart {
    //         params: PieChartParams { column_name: None },
    //         sources: MockRasterSource {
    //             params: MockRasterSourceParams {
    //                 data: vec![RasterTile2D::new_with_tile_info(
    //                     TimeInterval::default(),
    //                     TileInformation {
    //                         global_geo_transform: TestDefault::test_default(),
    //                         global_tile_position: [0, 0].into(),
    //                         tile_size_in_pixels,
    //                     },
    //                     Grid2D::new(tile_size_in_pixels, vec![0, 0, 0, 0, 0, 0])
    //                         .unwrap()
    //                         .into(),
    //                 )],
    //                 result_descriptor: RasterResultDescriptor {
    //                     data_type: RasterDataType::U8,
    //                     spatial_reference: SpatialReference::epsg_4326().into(),
    //                     measurement,
    //                     time: None,
    //                     bbox: None,
    //                     resolution: None,
    //                 },
    //             },
    //         }
    //         .boxed()
    //         .into(),
    //     };

    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();

    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
    //                 time_interval: TimeInterval::default(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::new(ChunkByteSize::MIN),
    //         )
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         result,
    //         BarChart::new(
    //             [("A".to_string(), 0)].into_iter().collect(),
    //             "foo".to_string(),
    //             "Frequency".to_string()
    //         )
    //         .to_vega_embeddable(true)
    //         .unwrap()
    //     );
    // }

    // #[tokio::test]
    // async fn empty_feature_collection() {
    //     let measurement = Measurement::classification(
    //         "foo".to_string(),
    //         [(1, "A".to_string())].into_iter().collect(),
    //     );

    //     let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
    //         vec![DataCollection::from_slices(
    //             &[] as &[NoGeometry],
    //             &[] as &[TimeInterval],
    //             &[("foo", FeatureData::Float(vec![]))],
    //         )
    //         .unwrap()],
    //         [("foo".to_string(), measurement)].into_iter().collect(),
    //     )
    //     .boxed();

    //     let histogram = PieChart {
    //         params: PieChartParams {
    //             column_name: Some("foo".to_string()),
    //         },
    //         sources: vector_source.into(),
    //     };

    //     let execution_context = MockExecutionContext::test_default();

    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();

    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
    //                     .unwrap(),
    //                 time_interval: TimeInterval::default(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::new(ChunkByteSize::MIN),
    //         )
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         result,
    //         BarChart::new(
    //             [("A".to_string(), 0)].into_iter().collect(),
    //             "foo".to_string(),
    //             "Frequency".to_string()
    //         )
    //         .to_vega_embeddable(true)
    //         .unwrap()
    //     );
    // }

    // #[tokio::test]
    // async fn feature_collection_with_one_feature() {
    //     let measurement = Measurement::classification(
    //         "foo".to_string(),
    //         [(5, "A".to_string())].into_iter().collect(),
    //     );

    //     let vector_source = MockFeatureCollectionSource::with_collections_and_measurements(
    //         vec![DataCollection::from_slices(
    //             &[] as &[NoGeometry],
    //             &[TimeInterval::default()],
    //             &[("foo", FeatureData::Float(vec![5.0]))],
    //         )
    //         .unwrap()],
    //         [("foo".to_string(), measurement)].into_iter().collect(),
    //     )
    //     .boxed();

    //     let histogram = PieChart {
    //         params: PieChartParams {
    //             column_name: Some("foo".to_string()),
    //         },
    //         sources: vector_source.into(),
    //     };

    //     let execution_context = MockExecutionContext::test_default();

    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();

    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((-180., -90.).into(), (180., 90.).into())
    //                     .unwrap(),
    //                 time_interval: TimeInterval::default(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::new(ChunkByteSize::MIN),
    //         )
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         result,
    //         BarChart::new(
    //             [("A".to_string(), 1)].into_iter().collect(),
    //             "foo".to_string(),
    //             "Frequency".to_string()
    //         )
    //         .to_vega_embeddable(true)
    //         .unwrap()
    //     );
    // }

    // #[tokio::test]
    // async fn single_value_raster_stream() {
    //     let tile_size_in_pixels = [3, 2].into();
    //     let tiling_specification = TilingSpecification {
    //         origin_coordinate: [0.0, 0.0].into(),
    //         tile_size_in_pixels,
    //     };
    //     let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

    //     let measurement = Measurement::classification(
    //         "foo".to_string(),
    //         [(4, "D".to_string())].into_iter().collect(),
    //     );

    //     let histogram = PieChart {
    //         params: PieChartParams { column_name: None },
    //         sources: MockRasterSource {
    //             params: MockRasterSourceParams {
    //                 data: vec![RasterTile2D::new_with_tile_info(
    //                     TimeInterval::default(),
    //                     TileInformation {
    //                         global_geo_transform: TestDefault::test_default(),
    //                         global_tile_position: [0, 0].into(),
    //                         tile_size_in_pixels,
    //                     },
    //                     Grid2D::new(tile_size_in_pixels, vec![4; 6]).unwrap().into(),
    //                 )],
    //                 result_descriptor: RasterResultDescriptor {
    //                     data_type: RasterDataType::U8,
    //                     spatial_reference: SpatialReference::epsg_4326().into(),
    //                     measurement,
    //                     time: None,
    //                     bbox: None,
    //                     resolution: None,
    //                 },
    //             },
    //         }
    //         .boxed()
    //         .into(),
    //     };

    //     let query_processor = histogram
    //         .boxed()
    //         .initialize(&execution_context)
    //         .await
    //         .unwrap()
    //         .query_processor()
    //         .unwrap()
    //         .json_vega()
    //         .unwrap();

    //     let result = query_processor
    //         .plot_query(
    //             VectorQueryRectangle {
    //                 spatial_bounds: BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
    //                 time_interval: TimeInterval::new_instant(DateTime::new_utc(
    //                     2013, 12, 1, 12, 0, 0,
    //                 ))
    //                 .unwrap(),
    //                 spatial_resolution: SpatialResolution::one(),
    //             },
    //             &MockQueryContext::new(ChunkByteSize::MIN),
    //         )
    //         .await
    //         .unwrap();

    //     assert_eq!(
    //         result,
    //         BarChart::new(
    //             [("D".to_string(), 6)].into_iter().collect(),
    //             "foo".to_string(),
    //             "Frequency".to_string()
    //         )
    //         .to_vega_embeddable(true)
    //         .unwrap()
    //     );
    // }
}
