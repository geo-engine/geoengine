use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedPlotOperator, InitializedRasterOperator,
    InitializedVectorOperator, Operator, OperatorName, PlotOperator, PlotQueryProcessor,
    PlotResultDescriptor, QueryContext, SingleRasterOrVectorSource, TypedPlotQueryProcessor,
    TypedRasterQueryProcessor, TypedVectorQueryProcessor,
};
use crate::engine::{QueryProcessor, WorkflowOperatorPath};
use crate::error;
use crate::error::Error;
use crate::optimization::OptimizationError;
use crate::util::input::RasterOrVectorOperator;
use crate::util::Result;
use async_trait::async_trait;
use futures::StreamExt;
use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::plots::{BarChart, Plot, PlotData};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, ClassificationMeasurement, ColumnSelection,
    FeatureDataType, Measurement, PlotQueryRectangle, RasterQueryRectangle, SpatialResolution,
    VectorQueryRectangle,
};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use std::collections::HashMap;

pub const CLASS_HISTOGRAM_OPERATOR_NAME: &str = "ClassHistogram";

/// A class histogram plot about either a raster or a vector input.
///
/// For vector inputs, it calculates the histogram on one of its attributes.
///
pub type ClassHistogram = Operator<ClassHistogramParams, SingleRasterOrVectorSource>;

impl OperatorName for ClassHistogram {
    const TYPE_NAME: &'static str = "ClassHistogram";
}

/// The parameter spec for `Histogram`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassHistogramParams {
    /// Name of the (numeric) attribute to compute the histogram on. Fails if set for rasters.
    pub column_name: Option<String>,
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for ClassHistogram {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let name = CanonicOperatorName::from(&self);

        Ok(match self.sources.source {
            RasterOrVectorOperator::Raster(raster_source) => {
                ensure!(
                    self.params.column_name.is_none(),
                    error::InvalidOperatorSpec {
                        reason: "Histogram on raster input must not have `columnName` field set"
                            .to_string(),
                    }
                );

                let raster_source = raster_source
                    .initialize(path.clone_and_append(0), context)
                    .await?;

                let in_desc = raster_source.result_descriptor();

                ensure!(
                    in_desc.bands.len() == 1,
                    crate::error::OperatorDoesNotSupportMultiBandsSourcesYet {
                        operator: ClassHistogram::TYPE_NAME
                    }
                );

                let source_measurement = match &in_desc.bands[0].measurement {
                    Measurement::Classification(measurement) => measurement.clone(),
                    _ => {
                        return Err(Error::InvalidOperatorSpec {
                            reason: "Source measurement mut be classification".to_string(),
                        })
                    }
                };

                InitializedClassHistogram::new(
                    name,
                    PlotResultDescriptor {
                        spatial_reference: in_desc.spatial_reference,
                        time: in_desc.time,
                        // converting `SpatialPartition2D` to `BoundingBox2D` is ok here, because is makes the covered area only larger
                        bbox: Some(in_desc.spatial_bounds().as_bbox()),
                    },
                    self.params.column_name,
                    source_measurement,
                    raster_source,
                )
                .boxed()
            }
            RasterOrVectorOperator::Vector(vector_source) => {
                let column_name =
                    self.params
                        .column_name
                        .as_ref()
                        .context(error::InvalidOperatorSpec {
                            reason: "Histogram on vector input is missing `columnName` field"
                                .to_string(),
                        })?;

                let vector_source = vector_source
                    .initialize(path.clone_and_append(0), context)
                    .await?;

                match vector_source
                    .result_descriptor()
                    .column_data_type(column_name)
                {
                    None => {
                        return Err(Error::ColumnDoesNotExist {
                            column: column_name.to_string(),
                        });
                    }
                    Some(FeatureDataType::Text | FeatureDataType::DateTime) => {
                        return Err(Error::InvalidOperatorSpec {
                            reason: format!("column `{column_name}` must be numerical"),
                        });
                    }
                    Some(
                        FeatureDataType::Int
                        | FeatureDataType::Float
                        | FeatureDataType::Bool
                        | FeatureDataType::Category,
                    ) => {
                        // okay
                    }
                }

                let in_desc = vector_source.result_descriptor().clone();

                let source_measurement = match in_desc.column_measurement(column_name) {
                    Some(Measurement::Classification(measurement)) => measurement.clone(),
                    _ => {
                        return Err(Error::InvalidOperatorSpec {
                            reason: "Source measurement mut be classification".to_string(),
                        })
                    }
                };

                InitializedClassHistogram::new(
                    name,
                    in_desc.into(),
                    self.params.column_name,
                    source_measurement,
                    vector_source,
                )
                .boxed()
            }
        })
    }

    span_fn!(ClassHistogram);
}

/// The initialization of `Histogram`
pub struct InitializedClassHistogram<Op> {
    name: CanonicOperatorName,
    result_descriptor: PlotResultDescriptor,
    source_measurement: ClassificationMeasurement,
    source: Op,
    column_name: Option<String>,
}

impl<Op> InitializedClassHistogram<Op> {
    pub fn new(
        name: CanonicOperatorName,
        result_descriptor: PlotResultDescriptor,
        column_name: Option<String>,
        source_measurement: ClassificationMeasurement,
        source: Op,
    ) -> Self {
        Self {
            name,
            result_descriptor,
            source_measurement,
            source,
            column_name,
        }
    }
}

impl InitializedPlotOperator for InitializedClassHistogram<Box<dyn InitializedRasterOperator>> {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let processor = ClassHistogramRasterQueryProcessor {
            input: self.source.query_processor()?,
            measurement: self.source_measurement.clone(),
        };

        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn PlotOperator>, OptimizationError> {
        Ok(ClassHistogram {
            params: ClassHistogramParams {
                column_name: self.column_name.clone(),
            },
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Raster(self.source.optimize(target_resolution)?),
            },
        }
        .boxed())
    }
}

impl InitializedPlotOperator for InitializedClassHistogram<Box<dyn InitializedVectorOperator>> {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        let processor = ClassHistogramVectorQueryProcessor {
            input: self.source.query_processor()?,
            column_name: self.column_name.clone().unwrap_or_default(),
            measurement: self.source_measurement.clone(),
        };

        Ok(TypedPlotQueryProcessor::JsonVega(processor.boxed()))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn PlotOperator>, OptimizationError> {
        Ok(ClassHistogram {
            params: ClassHistogramParams {
                column_name: self.column_name.clone(),
            },
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Vector(self.source.optimize(target_resolution)?),
            },
        }
        .boxed())
    }
}

/// A query processor that calculates the Histogram about its raster inputs.
pub struct ClassHistogramRasterQueryProcessor {
    input: TypedRasterQueryProcessor,
    measurement: ClassificationMeasurement,
}

/// A query processor that calculates the Histogram about its vector inputs.
pub struct ClassHistogramVectorQueryProcessor {
    input: TypedVectorQueryProcessor,
    column_name: String,
    measurement: ClassificationMeasurement,
}

#[async_trait]
impl PlotQueryProcessor for ClassHistogramRasterQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        CLASS_HISTOGRAM_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: PlotQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        self.process(query, ctx).await
    }
}

#[async_trait]
impl PlotQueryProcessor for ClassHistogramVectorQueryProcessor {
    type OutputFormat = PlotData;

    fn plot_type(&self) -> &'static str {
        CLASS_HISTOGRAM_OPERATOR_NAME
    }

    async fn plot_query<'p>(
        &'p self,
        query: PlotQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        self.process(query, ctx).await
    }
}

impl ClassHistogramRasterQueryProcessor {
    async fn process<'p>(
        &'p self,
        query: PlotQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<<ClassHistogramRasterQueryProcessor as PlotQueryProcessor>::OutputFormat> {
        let mut class_counts: HashMap<u8, u64> = self
            .measurement
            .classes
            .keys()
            .map(|key| (*key, 0))
            .collect();

        let rd = self.input.result_descriptor();

        let raster_query_rect = RasterQueryRectangle::with_spatial_query_and_geo_transform(
            &query,
            rd.tiling_grid_definition(ctx.tiling_specification())
                .tiling_geo_transform(),
            BandSelection::first(),
        );

        call_on_generic_raster_processor!(&self.input, processor => {
            let mut query = processor.query(raster_query_rect, ctx).await?;

            while let Some(tile) = query.next().await {
                match tile?.grid_array {
                    geoengine_datatypes::raster::GridOrEmpty::Grid(g) => {
                        g.masked_element_deref_iterator().for_each(|value_option| {
                            if let Some(v) = value_option {
                                if let Some(count) = class_counts.get_mut(&v.as_()) {
                                    *count += 1;
                                }
                            }
                        });
                    },
                    geoengine_datatypes::raster::GridOrEmpty::Empty(_) => (), // ignore no data,
                }
            }
        });

        // TODO: display NO-DATA count?

        let bar_chart = BarChart::new(
            class_counts
                .into_iter()
                .map(|(class, count)| {
                    (
                        self.measurement
                            .classes
                            .get(&class)
                            .cloned()
                            .unwrap_or_default(),
                        count,
                    )
                })
                .collect(),
            Measurement::Classification(self.measurement.clone()).to_string(),
            "Frequency".to_string(),
        );
        let chart = bar_chart.to_vega_embeddable(false)?;

        Ok(chart)
    }
}

impl ClassHistogramVectorQueryProcessor {
    async fn process<'p>(
        &'p self,
        query: PlotQueryRectangle,
        ctx: &'p dyn QueryContext,
    ) -> Result<<ClassHistogramRasterQueryProcessor as PlotQueryProcessor>::OutputFormat> {
        let mut class_counts: HashMap<u8, u64> = self
            .measurement
            .classes
            .keys()
            .map(|key| (*key, 0))
            .collect();

        let query = VectorQueryRectangle::new(
            query.spatial_query(),
            query.time_interval,
            ColumnSelection::all(), // TODO: figure out why this is a vector query?
        );

        call_on_generic_vector_processor!(&self.input, processor => {
            let mut query = processor.query(query, ctx).await?;

            while let Some(collection) = query.next().await {
                let collection = collection?;

                let feature_data = collection.data(&self.column_name).expect("checked in param");

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

        let bar_chart = BarChart::new(
            class_counts
                .into_iter()
                .map(|(class, count)| {
                    (
                        self.measurement
                            .classes
                            .get(&class)
                            .cloned()
                            .unwrap_or_default(),
                        count,
                    )
                })
                .collect(),
            Measurement::Classification(self.measurement.clone()).to_string(),
            "Frequency".to_string(),
        );
        let chart = bar_chart.to_vega_embeddable(false)?;

        Ok(chart)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::engine::{
        ChunkByteSize, MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors,
        RasterOperator, RasterResultDescriptor, SpatialGridDescriptor, StaticMetaData,
        VectorColumnInfo, VectorOperator, VectorResultDescriptor,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use crate::source::{
        OgrSourceColumnSpec, OgrSourceDataset, OgrSourceDatasetTimeType, OgrSourceErrorSpec,
    };
    use crate::test_data;
    use geoengine_datatypes::dataset::{DataId, DatasetId, NamedData};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Coordinate2D, DateTime, FeatureData, NoGeometry, PlotSeriesSelection,
        TimeInterval, VectorQueryRectangle,
    };
    use geoengine_datatypes::primitives::{CacheHint, CacheTtlSeconds};
    use geoengine_datatypes::raster::{
        BoundedGrid, GeoTransform, Grid2D, GridShape2D, RasterDataType, RasterTile2D,
        TileInformation, TilingSpecification,
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
        let histogram = ClassHistogram {
            params: ClassHistogramParams {
                column_name: Some("foobar".to_string()),
            },
            sources: MockFeatureCollectionSource::<MultiPoint>::multiple(vec![])
                .boxed()
                .into(),
        };

        let serialized = json!({
            "type": "ClassHistogram",
            "params": {
                "columnName": "foobar",
            },
            "sources": {
                "source": {
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

        let deserialized: ClassHistogram = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, histogram.params);
    }

    #[tokio::test]
    async fn column_name_for_raster_source() {
        let histogram = ClassHistogram {
            params: ClassHistogramParams {
                column_name: Some("foo".to_string()),
            },
            sources: mock_raster_source().into(),
        };

        let execution_context = MockExecutionContext::test_default();

        assert!(histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .is_err());
    }

    fn mock_raster_source() -> Box<dyn RasterOperator> {
        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                    },
                    0,
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                        .unwrap()
                        .into(),
                    CacheHint::default(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                        GridShape2D::new_2d(3, 2).bounding_box(),
                    ),
                    bands: RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
                        "bands".into(),
                        Measurement::classification(
                            "test-class".to_string(),
                            [
                                (1, "A".to_string()),
                                (2, "B".to_string()),
                                (3, "C".to_string()),
                                (4, "D".to_string()),
                                (5, "E".to_string()),
                                (6, "F".to_string()),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                    )])
                    .unwrap(),
                },
            },
        }
        .boxed()
    }

    #[tokio::test]
    async fn simple_raster() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            tile_size_in_pixels,
        };
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let histogram = ClassHistogram {
            params: ClassHistogramParams { column_name: None },
            sources: mock_raster_source().into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                PlotQueryRectangle::with_bounds(
                    BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BarChart::new(
                [
                    ("A".to_string(), 1),
                    ("B".to_string(), 1),
                    ("C".to_string(), 1),
                    ("D".to_string(), 1),
                    ("E".to_string(), 1),
                    ("F".to_string(), 1),
                ]
                .into_iter()
                .collect(),
                "test-class".to_string(),
                "Frequency".to_string()
            )
            .to_vega_embeddable(true)
            .unwrap()
        );
    }

    #[tokio::test]
    async fn vector_data() {
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

        let histogram = ClassHistogram {
            params: ClassHistogramParams {
                column_name: Some("foo".to_string()),
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                PlotQueryRectangle::with_bounds(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BarChart::new(
                [
                    ("A".to_string(), 5),
                    ("B".to_string(), 4),
                    ("C".to_string(), 3),
                ]
                .into_iter()
                .collect(),
                "foo".to_string(),
                "Frequency".to_string()
            )
            .to_vega_embeddable(true)
            .unwrap()
        );
    }

    #[tokio::test]
    async fn vector_data_with_nulls() {
        let measurement = Measurement::classification(
            "foo".to_string(),
            [
                (1, "A".to_string()),
                (2, "B".to_string()),
                (4, "C".to_string()),
            ]
            .into_iter()
            .collect(),
        );

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
                        Some(4.),
                        None,
                        Some(5.),
                    ]),
                )],
            )
            .unwrap()],
            [("foo".to_string(), measurement)].into_iter().collect(),
        )
        .boxed();

        let histogram = ClassHistogram {
            params: ClassHistogramParams {
                column_name: Some("foo".to_string()),
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                PlotQueryRectangle::with_bounds(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BarChart::new(
                [
                    ("A".to_string(), 1),
                    ("B".to_string(), 1),
                    ("C".to_string(), 1),
                ]
                .into_iter()
                .collect(),
                "foo".to_string(),
                "Frequency".to_string()
            )
            .to_vega_embeddable(true)
            .unwrap()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn text_attribute() {
        let dataset_id = DatasetId::new();
        let dataset_name = NamedData::with_system_name("ne_10m_ports");

        let workflow = serde_json::json!({
            "type": "Histogram",
            "params": {
                "columnName": "featurecla",
            },
            "sources": {
                "source": {
                    "type": "OgrSource",
                    "params": {
                        "data": dataset_name.clone(),
                        "attributeProjection": null
                    },
                }
            }
        });
        let histogram: ClassHistogram = serde_json::from_value(workflow).unwrap();

        let mut execution_context = MockExecutionContext::test_default();
        execution_context.add_meta_data::<_, _, VectorQueryRectangle>(
            DataId::Internal { dataset_id },
            dataset_name.clone(),
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
                    cache_ttl: CacheTtlSeconds::default(),
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

        if let Err(Error::InvalidOperatorSpec { reason }) = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
        {
            assert_eq!(reason, "column `featurecla` must be numerical");
        } else {
            panic!("we currently don't support text features, but this went through");
        }
    }

    #[tokio::test]
    async fn no_data_raster() {
        let tile_size_in_pixels = GridShape2D::new_2d(3, 2);

        let bands = RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
            "band".into(),
            Measurement::Classification(ClassificationMeasurement {
                measurement: "foo".to_string(),
                classes: [(1, "A".to_string())].into_iter().collect(),
            }),
        )])
        .unwrap();

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                tile_size_in_pixels.bounding_box(),
            ),
            bands,
        };
        let tiling_specification = TilingSpecification::new(tile_size_in_pixels);

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let histogram = ClassHistogram {
            params: ClassHistogramParams { column_name: None },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        0,
                        Grid2D::new(tile_size_in_pixels, vec![0, 0, 0, 0, 0, 0])
                            .unwrap()
                            .into(),
                        CacheHint::default(),
                    )],
                    result_descriptor,
                },
            }
            .boxed()
            .into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                PlotQueryRectangle::with_bounds(
                    BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BarChart::new(
                [("A".to_string(), 0)].into_iter().collect(),
                "foo".to_string(),
                "Frequency".to_string()
            )
            .to_vega_embeddable(true)
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

        let histogram = ClassHistogram {
            params: ClassHistogramParams {
                column_name: Some("foo".to_string()),
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                PlotQueryRectangle::with_bounds(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BarChart::new(
                [("A".to_string(), 0)].into_iter().collect(),
                "foo".to_string(),
                "Frequency".to_string()
            )
            .to_vega_embeddable(true)
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

        let histogram = ClassHistogram {
            params: ClassHistogramParams {
                column_name: Some("foo".to_string()),
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::test_default();

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                PlotQueryRectangle::with_bounds(
                    BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap(),
                    TimeInterval::default(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BarChart::new(
                [("A".to_string(), 1)].into_iter().collect(),
                "foo".to_string(),
                "Frequency".to_string()
            )
            .to_vega_embeddable(true)
            .unwrap()
        );
    }

    #[tokio::test]
    async fn single_value_raster_stream() {
        let tile_size_in_pixels = GridShape2D::new_2d(3, 2);

        let bands = RasterBandDescriptors::new(vec![RasterBandDescriptor::new(
            "band".into(),
            Measurement::Classification(ClassificationMeasurement {
                measurement: "foo".to_string(),
                classes: [(4, "D".to_string())].into_iter().collect(),
            }),
        )])
        .unwrap();

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                tile_size_in_pixels.bounding_box(),
            ),
            bands,
        };
        let tiling_specification = TilingSpecification::new(tile_size_in_pixels);
        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let histogram = ClassHistogram {
            params: ClassHistogramParams { column_name: None },
            sources: MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        0,
                        Grid2D::new(tile_size_in_pixels, vec![4; 6]).unwrap().into(),
                        CacheHint::default(),
                    )],
                    result_descriptor,
                },
            }
            .boxed()
            .into(),
        };

        let query_processor = histogram
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .json_vega()
            .unwrap();

        let result = query_processor
            .plot_query(
                PlotQueryRectangle::with_bounds(
                    BoundingBox2D::new((0., -3.).into(), (2., 0.).into()).unwrap(),
                    TimeInterval::new_instant(DateTime::new_utc(2013, 12, 1, 12, 0, 0)).unwrap(),
                    PlotSeriesSelection::all(),
                ),
                &execution_context.mock_query_context(ChunkByteSize::MIN),
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            BarChart::new(
                [("D".to_string(), 6)].into_iter().collect(),
                "foo".to_string(),
                "Frequency".to_string()
            )
            .to_vega_embeddable(true)
            .unwrap()
        );
    }
}
