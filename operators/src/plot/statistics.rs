use crate::engine::{
    ExecutionContext, InitializedPlotOperator, InitializedRasterOperator, MultipleRasterSources,
    Operator, PlotOperator, PlotQueryProcessor, PlotResultDescriptor, QueryContext, QueryProcessor,
    TypedPlotQueryProcessor, TypedRasterQueryProcessor, VectorQueryRectangle,
};
use crate::util::number_statistics::NumberStatistics;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::join_all;
use futures::stream::select_all;
use futures::{FutureExt, StreamExt};
use geoengine_datatypes::raster::{Grid2D, GridOrEmpty, GridSize, NoDataValue};
use serde::{Deserialize, Serialize};

pub const STATISTICS_OPERATOR_NAME: &str = "Statistics";

/// A plot that outputs basic statistics about its inputs
///
/// Does currently not use a weighted computations, so it assumes equally weighted
/// time steps in the sources.
// TODO: implement operator also for vector data
pub type Statistics = Operator<StatisticsParams, MultipleRasterSources>;

/// The parameter spec for `Statistics`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

        let initialized_operator = InitializedStatistics {
            result_descriptor: PlotResultDescriptor {},
            rasters: rasters.into_iter().collect::<Result<Vec<_>>>()?,
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
                             .map(move |r| r.map(|tile| (i, tile.convert::<f64>())))
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
                        GridOrEmpty::Grid(g) => process_raster(&mut number_statistics[i], &g),
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
fn process_raster(number_statistics: &mut NumberStatistics, tile_grid: &Grid2D<f64>) {
    let no_data_value = tile_grid.no_data_value();

    if let Some(no_data_value) = no_data_value {
        for &value in &tile_grid.data {
            if value == no_data_value {
                number_statistics.add_no_data();
            } else {
                number_statistics.add(value);
            }
        }
    } else {
        for &value in &tile_grid.data {
            number_statistics.add(value);
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
        MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor,
        VectorQueryRectangle,
    };
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::{
        BoundingBox2D, Measurement, SpatialResolution, TimeInterval,
    };
    use geoengine_datatypes::raster::{Grid2D, RasterDataType, RasterTile2D, TileInformation};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use num_traits::AsPrimitive;

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
        let no_data_value = None;
        let raster_source = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::default(),
                    TileInformation {
                        global_geo_transform: TestDefault::test_default(),
                        global_tile_position: [0, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                    },
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value)
                        .unwrap()
                        .into(),
                )],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let statistics = Statistics {
            params: StatisticsParams {},
            sources: vec![raster_source].into(),
        };

        let execution_context = MockExecutionContext::default();

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
                &MockQueryContext::new(0),
            )
            .await
            .unwrap();

        assert_eq!(
            result.to_string(),
            json!([{
                "pixelCount": 6,
                "nanCount": 0,
                "min": 1.0,
                "max": 6.0,
                "mean": 3.5,
                "stddev": 1.707_825_127_659_933
            }])
            .to_string()
        );
    }
}
