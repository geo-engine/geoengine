use crate::engine::{
    ExecutionContext, InitializedMultiRasterOrVectorOperator, InitializedPlotOperator,
    InitializedRasterOperator, InitializedSources, InitializedVectorOperator,
    MultipleRasterOrSingleVectorSource, Operator, OperatorName, PlotOperator, PlotQueryProcessor,
    PlotResultDescriptor, QueryContext, QueryProcessor, TypedPlotQueryProcessor,
    TypedRasterQueryProcessor, TypedVectorQueryProcessor, WorkflowOperatorPath,
};
use crate::error;
use crate::error::Error;
use crate::util::number_statistics::NumberStatistics;
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::select_all;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::collections::FeatureCollectionInfos;
use geoengine_datatypes::primitives::{
    partitions_extent, time_interval_extent, AxisAlignedRectangle, BoundingBox2D,
    PlotQueryRectangle, VectorQueryRectangle,
};
use geoengine_datatypes::raster::ConvertDataTypeParallel;
use geoengine_datatypes::raster::{GridOrEmpty, GridSize};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashMap;

pub const STATISTICS_OPERATOR_NAME: &str = "Statistics";

/// A plot that outputs basic statistics about its inputs
///
/// Does currently not use a weighted computations, so it assumes equally weighted
/// time steps in the sources.
pub type Statistics = Operator<StatisticsParams, MultipleRasterOrSingleVectorSource>;

impl OperatorName for Statistics {
    const TYPE_NAME: &'static str = "Statistics";
}

/// The parameter spec for `Statistics`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatisticsParams {
    /// Names of the (numeric) attributes to compute the statistics on.
    #[serde(default)]
    pub column_names: Vec<String>,
}

#[typetag::serde]
#[async_trait]
impl PlotOperator for Statistics {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let initialized_sources = self.sources.initialize_sources(path, context).await?;

        match initialized_sources.source {
            InitializedMultiRasterOrVectorOperator::Raster(rasters) => {
                ensure!( self.params.column_names.is_empty() || self.params.column_names.len() == rasters.len(),
                    error::InvalidOperatorSpec {
                        reason: "Statistics on raster data must either contain a name/alias for every input ('column_names' parameter) or no names at all."
                            .to_string(),
                });

                let output_names = if self.params.column_names.is_empty() {
                    (1..=rasters.len())
                        .map(|i| format!("Raster-{i}"))
                        .collect::<Vec<_>>()
                } else {
                    self.params.column_names.clone()
                };

                let in_descriptors = rasters
                    .iter()
                    .map(InitializedRasterOperator::result_descriptor)
                    .collect::<Vec<_>>();

                if rasters.len() > 1 {
                    let srs = in_descriptors[0].spatial_reference;
                    ensure!(
                        in_descriptors.iter().all(|d| d.spatial_reference == srs),
                        error::AllSourcesMustHaveSameSpatialReference
                    );
                }

                let time = time_interval_extent(in_descriptors.iter().map(|d| d.time));
                let bbox = partitions_extent(in_descriptors.iter().map(|d| d.bbox));

                let initialized_operator = InitializedStatistics::new(
                    PlotResultDescriptor {
                        spatial_reference: rasters.get(0).map_or_else(
                            || SpatialReferenceOption::Unreferenced,
                            |r| r.result_descriptor().spatial_reference,
                        ),
                        time,
                        bbox: bbox
                            .and_then(|p| BoundingBox2D::new(p.lower_left(), p.upper_right()).ok()),
                    },
                    output_names,
                    rasters,
                );

                Ok(initialized_operator.boxed())
            }
            InitializedMultiRasterOrVectorOperator::Vector(initialized_vector) => {
                let in_descriptor = initialized_vector.result_descriptor();

                let column_names = if self.params.column_names.is_empty() {
                    in_descriptor
                        .columns
                        .clone()
                        .into_iter()
                        .filter(|(_, info)| info.data_type.is_numeric())
                        .map(|(name, _)| name)
                        .collect()
                } else {
                    for cn in &self.params.column_names {
                        match in_descriptor.column_data_type(cn.as_str()) {
                            Some(column) if !column.is_numeric() => {
                                return Err(Error::InvalidOperatorSpec {
                                    reason: format!("Column '{cn}' is not numeric."),
                                });
                            }
                            Some(_) => {
                                // OK
                            }
                            None => {
                                return Err(Error::ColumnDoesNotExist {
                                    column: cn.to_string(),
                                });
                            }
                        }
                    }
                    self.params.column_names.clone()
                };

                let initialized_operator = InitializedStatistics::new(
                    PlotResultDescriptor {
                        spatial_reference: in_descriptor.spatial_reference,
                        time: in_descriptor.time,
                        bbox: in_descriptor.bbox,
                    },
                    column_names,
                    initialized_vector,
                );

                Ok(initialized_operator.boxed())
            }
        }
    }

    span_fn!(Statistics);
}

/// The initialization of `Statistics`
pub struct InitializedStatistics<Op> {
    result_descriptor: PlotResultDescriptor,
    column_names: Vec<String>,
    source: Op,
}

impl<Op> InitializedStatistics<Op> {
    pub fn new(
        result_descriptor: PlotResultDescriptor,
        column_names: Vec<String>,
        source: Op,
    ) -> Self {
        Self {
            result_descriptor,
            column_names,
            source,
        }
    }
}

impl InitializedPlotOperator for InitializedStatistics<Box<dyn InitializedVectorOperator>> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        Ok(TypedPlotQueryProcessor::JsonPlain(
            StatisticsVectorQueryProcessor {
                vector: self.source.query_processor()?,
                column_names: self.column_names.clone(),
            }
            .boxed(),
        ))
    }
}

impl InitializedPlotOperator for InitializedStatistics<Vec<Box<dyn InitializedRasterOperator>>> {
    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        Ok(TypedPlotQueryProcessor::JsonPlain(
            StatisticsRasterQueryProcessor {
                rasters: self
                    .source
                    .iter()
                    .map(InitializedRasterOperator::query_processor)
                    .collect::<Result<Vec<_>>>()?,
                column_names: self.column_names.clone(),
            }
            .boxed(),
        ))
    }
}

/// A query processor that calculates the statistics about its vector input.
pub struct StatisticsVectorQueryProcessor {
    vector: TypedVectorQueryProcessor,
    column_names: Vec<String>,
}

#[async_trait]
impl PlotQueryProcessor for StatisticsVectorQueryProcessor {
    type OutputFormat = serde_json::Value;

    fn plot_type(&self) -> &'static str {
        STATISTICS_OPERATOR_NAME
    }

    async fn plot_query<'a>(
        &'a self,
        query: PlotQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let mut number_statistics: HashMap<String, NumberStatistics> = self
            .column_names
            .iter()
            .map(|column| (column.clone(), NumberStatistics::default()))
            .collect();

        call_on_generic_vector_processor!(&self.vector, processor => {
            let mut query = processor.query(query, ctx).await?;

            while let Some(collection) = query.next().await {
                let collection = collection?;

                for (column, stats) in &mut number_statistics {
                    match collection.data(column) {
                        Ok(data) => data.float_options_iter().for_each(
                            | value | {
                                match value {
                                    Some(v) => stats.add(v),
                                    None => stats.add_no_data()
                                }
                            }
                        ),
                        Err(_) => stats.add_no_data_batch(collection.len())
                    }
                }
            }
        });

        let output: HashMap<String, StatisticsOutput> = number_statistics
            .iter()
            .map(|(column, number_statistics)| {
                (column.clone(), StatisticsOutput::from(number_statistics))
            })
            .collect();
        serde_json::to_value(output).map_err(Into::into)
    }
}

/// A query processor that calculates the statistics about its raster inputs.
pub struct StatisticsRasterQueryProcessor {
    rasters: Vec<TypedRasterQueryProcessor>,
    column_names: Vec<String>,
}

#[async_trait]
impl PlotQueryProcessor for StatisticsRasterQueryProcessor {
    type OutputFormat = serde_json::Value;

    fn plot_type(&self) -> &'static str {
        STATISTICS_OPERATOR_NAME
    }

    async fn plot_query<'a>(
        &'a self,
        query: VectorQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<Self::OutputFormat> {
        let mut queries = Vec::with_capacity(self.rasters.len());
        let q = query.into();
        for (i, raster_processor) in self.rasters.iter().enumerate() {
            queries.push(
                call_on_generic_raster_processor!(raster_processor, processor => {
                    processor.query(q, ctx).await?
                             .and_then(move |tile| crate::util::spawn_blocking_with_thread_pool(ctx.thread_pool().clone(), move || (i, tile.convert_data_type_parallel()) ).map_err(Into::into))
                             .boxed()
                }),
            );
        }

        let number_statistics = vec![NumberStatistics::default(); self.rasters.len()];

        select_all(queries)
            .fold(
                Ok(number_statistics),
                |number_statistics: Result<Vec<NumberStatistics>>, enumerated_raster_tile| async move {
                    let mut number_statistics = number_statistics?;
                    let (i, raster_tile) = enumerated_raster_tile?;
                    match raster_tile.grid_array {
                        GridOrEmpty::Grid(g) => process_raster(&mut number_statistics[i], g.masked_element_deref_iterator()),
                        GridOrEmpty::Empty(n) => number_statistics[i].add_no_data_batch(n.number_of_elements())
                    }

                    Ok(number_statistics)
                },
            )
            .map(|number_statistics| {
                let output: HashMap<String, StatisticsOutput> = number_statistics?.iter().enumerate().map(|(i, stat)| (self.column_names[i].clone(), StatisticsOutput::from(stat))).collect();
                serde_json::to_value(output).map_err(Into::into)
            })
            .await
    }
}

fn process_raster<I>(number_statistics: &mut NumberStatistics, data: I)
where
    I: Iterator<Item = Option<f64>>,
{
    for value_option in data {
        if let Some(value) = value_option {
            number_statistics.add(value);
        } else {
            number_statistics.add_no_data();
        }
    }
}

/// The statistics summary output type for each raster input/vector input column
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatisticsOutput {
    pub value_count: usize,
    pub valid_count: usize,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: f64,
}

impl From<&NumberStatistics> for StatisticsOutput {
    fn from(number_statistics: &NumberStatistics) -> Self {
        Self {
            value_count: number_statistics.count() + number_statistics.nan_count(),
            valid_count: number_statistics.count(),
            min: number_statistics.min(),
            max: number_statistics.max(),
            mean: number_statistics.mean(),
            stddev: number_statistics.std_dev(),
        }
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::collections::DataCollection;
    use geoengine_datatypes::util::test::TestDefault;
    use serde_json::json;

    use super::*;
    use crate::engine::VectorOperator;
    use crate::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, RasterOperator,
        RasterResultDescriptor,
    };
    use crate::mock::{MockFeatureCollectionSource, MockRasterSource, MockRasterSourceParams};
    use crate::util::input::MultiRasterOrVectorOperator::Raster;
    use geoengine_datatypes::primitives::{
        BoundingBox2D, FeatureData, Measurement, NoGeometry, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        Grid2D, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;

    #[test]
    fn serialization() {
        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec![],
            },
            sources: MultipleRasterOrSingleVectorSource {
                source: Raster(vec![]),
            },
        };

        let serialized = json!({
            "type": "Statistics",
            "params": {},
            "sources": {
                "source": [],
            },
        })
        .to_string();

        let deserialized: Statistics = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, statistics.params);
    }

    #[tokio::test]
    async fn empty_raster_input() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec![],
            },
            sources: vec![].into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_plain().unwrap();

        let result = processor
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

        assert_eq!(result.to_string(), json!({}).to_string());
    }

    #[tokio::test]
    async fn single_raster_implicit_name() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels,
                    },
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                        .unwrap()
                        .into(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    time: None,
                    bbox: None,
                    resolution: None,
                },
            },
        }
        .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec![],
            },
            sources: vec![raster_source].into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_plain().unwrap();

        let result = processor
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
            result.to_string(),
            json!({
                "Raster-1": {
                    "valueCount": 66_246, // 362*183 Note: this is caused by the inclusive nature of the bounding box. Since the right and lower bounds are included this wraps to a new row/column of tiles. In this test the tiles are 3x2 pixels in size.
                    "validCount": 6,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.5,
                    "stddev": 1.707_825_127_659_933,
                }
            })
            .to_string()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn two_rasters_implicit_names() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let raster_source = vec![
            MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                            .unwrap()
                            .into(),
                    )],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed(),
            MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12])
                            .unwrap()
                            .into(),
                    )],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed(),
        ];

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec![],
            },
            sources: raster_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_plain().unwrap();

        let result = processor
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
            result.to_string(),
            json!({
                "Raster-1": {
                    "valueCount": 66_246, // 362*183 Note: this is caused by the inclusive nature of the bounding box. Since the right and lower bounds are included this wraps to a new row/column of tiles. In this test the tiles are 3x2 pixels in size.
                    "validCount": 6,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.5,
                    "stddev": 1.707_825_127_659_933
                },
                "Raster-2": {
                    "valueCount": 66_246, // 362*183 Note: this is caused by the inclusive nature of the bounding box. Since the right and lower bounds are included this wraps to a new row/column of tiles. In this test the tiles are 3x2 pixels in size.
                    "validCount": 6,
                    "min": 7.0,
                    "max": 12.0,
                    "mean": 9.5,
                    "stddev": 1.707_825_127_659_933
                },
            })
            .to_string()
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn two_rasters_explicit_names() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let raster_source = vec![
            MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                            .unwrap()
                            .into(),
                    )],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed(),
            MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12])
                            .unwrap()
                            .into(),
                    )],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed(),
        ];

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec!["A".to_string(), "B".to_string()],
            },
            sources: raster_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_plain().unwrap();

        let result = processor
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
            result.to_string(),
            json!({
                "A": {
                    "valueCount": 66_246, // 362*183 Note: this is caused by the inclusive nature of the bounding box. Since the right and lower bounds are included this wraps to a new row/column of tiles. In this test the tiles are 3x2 pixels in size.
                    "validCount": 6,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.5,
                    "stddev": 1.707_825_127_659_933
                },
                "B": {
                    "valueCount": 66_246, // 362*183 Note: this is caused by the inclusive nature of the bounding box. Since the right and lower bounds are included this wraps to a new row/column of tiles. In this test the tiles are 3x2 pixels in size.
                    "validCount": 6,
                    "min": 7.0,
                    "max": 12.0,
                    "mean": 9.5,
                    "stddev": 1.707_825_127_659_933
                },
            })
            .to_string()
        );
    }

    #[tokio::test]
    async fn two_rasters_explicit_names_incomplete() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let raster_source = vec![
            MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6])
                            .unwrap()
                            .into(),
                    )],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed(),
            MockRasterSource {
                params: MockRasterSourceParams {
                    data: vec![RasterTile2D::new_with_tile_info(
                        TimeInterval::default(),
                        TileInformation {
                            global_geo_transform: TestDefault::test_default(),
                            global_tile_position: [0, 0].into(),
                            tile_size_in_pixels,
                        },
                        Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12])
                            .unwrap()
                            .into(),
                    )],
                    result_descriptor: RasterResultDescriptor {
                        data_type: RasterDataType::U8,
                        spatial_reference: SpatialReference::epsg_4326().into(),
                        measurement: Measurement::Unitless,
                        time: None,
                        bbox: None,
                        resolution: None,
                    },
                },
            }
            .boxed(),
        ];

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec!["A".to_string()],
            },
            sources: raster_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await;

        assert!(
            matches!(statistics, Err(error::Error::InvalidOperatorSpec{reason}) if reason == *"Statistics on raster data must either contain a name/alias for every input ('column_names' parameter) or no names at all.")
        );
    }

    #[tokio::test]
    async fn vector_no_column() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[
                    (
                        "foo",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            None,
                            Some(3.0),
                            None,
                            Some(f64::NAN),
                            Some(6.0),
                            Some(f64::NAN),
                        ]),
                    ),
                    (
                        "bar",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            Some(2.0),
                            None,
                            None,
                            Some(5.0),
                            Some(f64::NAN),
                            Some(f64::NAN),
                        ]),
                    ),
                ],
            )
            .unwrap()])
            .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec![],
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_plain().unwrap();

        let result = processor
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
            result.to_string(),
            json!({
                "foo": {
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.333_333_333_333_333,
                    "stddev": 2.054_804_667_656_325_6
                },
                "bar": {
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 5.0,
                    "mean": 2.666_666_666_666_667,
                    "stddev": 1.699_673_171_197_595
                },
            })
            .to_string()
        );
    }

    #[tokio::test]
    async fn vector_single_column() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[
                    (
                        "foo",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            None,
                            Some(3.0),
                            None,
                            Some(f64::NAN),
                            Some(6.0),
                            Some(f64::NAN),
                        ]),
                    ),
                    (
                        "bar",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            Some(2.0),
                            None,
                            None,
                            Some(5.0),
                            Some(f64::NAN),
                            Some(f64::NAN),
                        ]),
                    ),
                ],
            )
            .unwrap()])
            .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec!["foo".to_string()],
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_plain().unwrap();

        let result = processor
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
            result.to_string(),
            json!({
                "foo": {
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.333_333_333_333_333,
                    "stddev": 2.054_804_667_656_325_6
                },
            })
            .to_string()
        );
    }

    #[tokio::test]
    async fn vector_two_columns() {
        let tile_size_in_pixels = [3, 2].into();
        let tiling_specification = TilingSpecification {
            origin_coordinate: [0.0, 0.0].into(),
            tile_size_in_pixels,
        };

        let vector_source =
            MockFeatureCollectionSource::multiple(vec![DataCollection::from_slices(
                &[] as &[NoGeometry],
                &[TimeInterval::default(); 7],
                &[
                    (
                        "foo",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            None,
                            Some(3.0),
                            None,
                            Some(f64::NAN),
                            Some(6.0),
                            Some(f64::NAN),
                        ]),
                    ),
                    (
                        "bar",
                        FeatureData::NullableFloat(vec![
                            Some(1.0),
                            Some(2.0),
                            None,
                            None,
                            Some(5.0),
                            Some(f64::NAN),
                            Some(f64::NAN),
                        ]),
                    ),
                ],
            )
            .unwrap()])
            .boxed();

        let statistics = Statistics {
            params: StatisticsParams {
                column_names: vec!["foo".to_string(), "bar".to_string()],
            },
            sources: vector_source.into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
            .await
            .unwrap();

        let processor = statistics.query_processor().unwrap().json_plain().unwrap();

        let result = processor
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
            result.to_string(),
            json!({
                "foo": {
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 6.0,
                    "mean": 3.333_333_333_333_333,
                    "stddev": 2.054_804_667_656_325_6
                },
                "bar": {
                    "valueCount": 7,
                    "validCount": 3,
                    "min": 1.0,
                    "max": 5.0,
                    "mean": 2.666_666_666_666_667,
                    "stddev": 1.699_673_171_197_595
                },
            })
            .to_string()
        );
    }
}
