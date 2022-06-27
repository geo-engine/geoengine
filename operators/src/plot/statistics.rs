use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedRasterOperator, MultipleRasterSources,
    Operator, PlotOperator, PlotQueryProcessor, PlotResultDescriptor, QueryContext, QueryProcessor,
    TypedPlotQueryProcessor, TypedRasterQueryProcessor,
};
use crate::error;
use crate::util::number_statistics::NumberStatistics;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::join_all;
use futures::stream::select_all;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use geoengine_datatypes::primitives::{
    partitions_extent, time_interval_extent, AxisAlignedRectangle, BoundingBox2D,
    VectorQueryRectangle,
};
use geoengine_datatypes::raster::ConvertDataTypeParallel;
use geoengine_datatypes::raster::{GridOrEmpty, GridSize};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use serde::{Deserialize, Serialize};
use snafu::ensure;

pub const STATISTICS_OPERATOR_NAME: &str = "Statistics";

/// A plot that outputs basic statistics about its inputs
///
/// Does currently not use a weighted computations, so it assumes equally weighted
/// time steps in the sources.
// TODO: implement operator also for vector data
pub type Statistics = Operator<StatisticsParams, MultipleRasterSources>;

/// The parameter spec for `Statistics`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatisticsParams {}

#[typetag::serde]
#[async_trait]
impl PlotOperator for Statistics {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedPlotOperator>> {
        let rasters = join_all(
            self.sources
                .rasters
                .into_iter()
                .map(|s| s.initialize(context)),
        )
        .await;
        let rasters = rasters.into_iter().collect::<Result<Vec<_>>>()?;

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

        let initialized_operator = InitializedStatistics {
            result_descriptor: PlotResultDescriptor {
                spatial_reference: rasters.get(0).map_or_else(
                    || SpatialReferenceOption::Unreferenced,
                    |r| r.result_descriptor().spatial_reference,
                ),
                time,
                bbox: bbox.and_then(|p| BoundingBox2D::new(p.lower_left(), p.upper_right()).ok()),
            },
            rasters,
        };

        Ok(initialized_operator.boxed())
    }
}

/// The initialization of `Statistics`
pub struct InitializedStatistics {
    result_descriptor: PlotResultDescriptor,
    rasters: Vec<Box<dyn InitializedRasterOperator>>,
}

impl InitializedPlotOperator for InitializedStatistics {
    fn query_processor(&self) -> Result<TypedPlotQueryProcessor> {
        Ok(TypedPlotQueryProcessor::JsonPlain(
            StatisticsQueryProcessor {
                rasters: self
                    .rasters
                    .iter()
                    .map(InitializedRasterOperator::query_processor)
                    .collect::<Result<Vec<_>>>()?,
            }
            .boxed(),
        ))
    }

    fn result_descriptor(&self) -> &PlotResultDescriptor {
        &self.result_descriptor
    }
}

/// A query processor that calculates the statistics about its inputs.
pub struct StatisticsQueryProcessor {
    rasters: Vec<TypedRasterQueryProcessor>,
}

#[async_trait]
impl PlotQueryProcessor for StatisticsQueryProcessor {
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
        for (i, raster_processor) in self.rasters.iter().enumerate() {
            queries.push(
                call_on_generic_raster_processor!(raster_processor, processor => {
                    processor.query(query.into(), ctx).await?
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
                let output: Vec<StatisticsOutput> = number_statistics?.iter().map(StatisticsOutput::from).collect();
                serde_json::to_value(&output).map_err(Into::into)
            })
            .await
    }
}

#[allow(clippy::float_cmp)] // allow since NO DATA is a specific value
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

/// The statistics summary output type for each raster input
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatisticsOutput {
    pub pixel_count: usize,
    pub nan_count: usize,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: f64,
}

impl From<&NumberStatistics> for StatisticsOutput {
    fn from(number_statistics: &NumberStatistics) -> Self {
        Self {
            pixel_count: number_statistics.count(),
            nan_count: number_statistics.nan_count(),
            min: number_statistics.min(),
            max: number_statistics.max(),
            mean: number_statistics.mean(),
            stddev: number_statistics.std_dev(),
        }
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::util::test::TestDefault;
    use serde_json::json;

    use super::*;
    use crate::engine::{
        ChunkByteSize, MockExecutionContext, MockQueryContext, RasterOperator,
        RasterResultDescriptor,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Measurement, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{
        Grid2D, RasterDataType, RasterTile2D, TileInformation, TilingSpecification,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;

    #[test]
    fn serialization() {
        let statistics = Statistics {
            params: StatisticsParams {},
            sources: MultipleRasterSources { rasters: vec![] },
        };

        let serialized = json!({
            "type": "Statistics",
            "params": {},
            "sources": {
                "rasters": [],
            },
        })
        .to_string();

        let deserialized: Statistics = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.params, statistics.params);
    }

    #[tokio::test]
    async fn single_raster() {
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
                },
            },
        }
        .boxed();

        let statistics = Statistics {
            params: StatisticsParams {},
            sources: vec![raster_source].into(),
        };

        let execution_context = MockExecutionContext::new_with_tiling_spec(tiling_specification);

        let statistics = statistics
            .boxed()
            .initialize(&execution_context)
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
            json!([{
                "pixelCount": 6,
                "nanCount": 64_794, // (360*180)-6
                "min": 1.0,
                "max": 6.0,
                "mean": 3.5,
                "stddev": 1.707_825_127_659_933
            }])
            .to_string()
        );
    }
}
