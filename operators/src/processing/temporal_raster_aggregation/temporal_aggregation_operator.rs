use crate::engine::{
    ExecutionContext, Operator, QueryProcessor, RasterOperator, SingleRasterSource,
};
use crate::{
    adapters::SubQueryTileAggregator,
    engine::{
        InitializedRasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        TypedRasterQueryProcessor,
    },
    error,
    util::Result,
};
use async_trait::async_trait;
use futures::StreamExt;
use geoengine_datatypes::primitives::SpatialPartition2D;
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_datatypes::{primitives::TimeStep, raster::TilingSpecification};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use typetag;

use super::mean_aggregation_subquery::{
    mean_tile_fold_future, TemporalRasterMeanAggregationSubQuery,
};
use super::min_max_first_last_subquery::{
    first_tile_fold_future, fold_future, last_tile_fold_future, no_data_ignoring_fold_future,
    FirstValidAccFunction, LastValidAccFunction, MaxAccFunction, MaxIgnoreNoDataAccFunction,
    MinAccFunction, MinIgnoreNoDataAccFunction, TemporalRasterAggregationSubQuery,
};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporalRasterAggregationParameters {
    aggregation: Aggregation,
    window: TimeStep,
    // TODO: allow specifying window start instead of using query.start?
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "type")]
pub enum Aggregation {
    #[serde(rename_all = "camelCase")]
    Min { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    Max { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    First { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    Last { ignore_no_data: bool },
    #[serde(rename_all = "camelCase")]
    Mean { ignore_no_data: bool },
}

pub type TemporalRasterAggregation =
    Operator<TemporalRasterAggregationParameters, SingleRasterSource>;

#[typetag::serde]
#[async_trait]
impl RasterOperator for TemporalRasterAggregation {
    async fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        ensure!(self.params.window.step > 0, error::WindowSizeMustNotBeZero);

        let source = self.sources.raster.initialize(context).await?;

        debug!(
            "Initializing TemporalRasterAggregation with {:?}.",
            &self.params
        );

        let initialized_operator = InitializedTemporalRasterAggregation {
            aggregation_type: self.params.aggregation,
            window: self.params.window,
            result_descriptor: source.result_descriptor().clone(),
            source,
            tiling_specification: context.tiling_specification(),
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedTemporalRasterAggregation {
    aggregation_type: Aggregation,
    window: TimeStep,
    source: Box<dyn InitializedRasterOperator>,
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
}

impl InitializedRasterOperator for InitializedTemporalRasterAggregation {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source_processor = self.source.query_processor()?;

        let res = call_on_generic_raster_processor!(
            source_processor, p =>
           TemporalRasterAggregationProcessor::new(
                self.aggregation_type,
                self.window,
                p,
                self.tiling_specification,
                self.source.result_descriptor().no_data_value
            ).boxed()
            .into()
        );

        Ok(res)
    }
}

pub struct TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    aggregation_type: Aggregation,
    window: TimeStep,
    source: Q,
    tiling_specification: TilingSpecification,
    no_data_value: Option<P>,
}

impl<Q, P> TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    fn new(
        aggregation_type: Aggregation,
        window: TimeStep,
        source: Q,
        tiling_specification: TilingSpecification,
        no_data_value: Option<f64>,
    ) -> Self {
        Self {
            aggregation_type,
            window,
            source,
            tiling_specification,
            no_data_value: no_data_value.map(P::from_),
        }
    }

    fn create_subquery<F>(
        &self,
        fold_fn: F,
        initial_value: P,
    ) -> TemporalRasterAggregationSubQuery<F, P> {
        TemporalRasterAggregationSubQuery {
            fold_fn,
            no_data_value: self.no_data_value,
            initial_value,
            step: self.window,
        }
    }

    fn create_subquery_mean<F>(
        &self,
        fold_fn: F,
        ignore_no_data: bool,
    ) -> TemporalRasterMeanAggregationSubQuery<F, P> {
        TemporalRasterMeanAggregationSubQuery {
            fold_fn,
            no_data_value: self.no_data_value.expect("mus have nodata"),
            step: self.window,
            ignore_no_data,
        }
    }
}

#[async_trait]
impl<Q, P> QueryProcessor for TemporalRasterAggregationProcessor<Q, P>
where
    Q: QueryProcessor<Output = RasterTile2D<P>, SpatialBounds = SpatialPartition2D>,
    P: Pixel,
{
    type Output = RasterTile2D<P>;
    type SpatialBounds = SpatialPartition2D;

    #[allow(clippy::too_many_lines)]
    async fn query<'a>(
        &'a self,
        query: crate::engine::RasterQueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<Self::Output>>> {
        match self.aggregation_type {
            Aggregation::Min {
                ignore_no_data: true,
            } => Ok(self
                .create_subquery(
                    no_data_ignoring_fold_future::<P, MinIgnoreNoDataAccFunction>,
                    P::max_value(),
                )
                .into_raster_overlap_adapter(&self.source, query, ctx, self.tiling_specification)
                .boxed()),
            Aggregation::Min {
                ignore_no_data: false,
            } => Ok(self
                .create_subquery(fold_future::<P, MinAccFunction>, P::max_value())
                .into_raster_overlap_adapter(&self.source, query, ctx, self.tiling_specification)
                .boxed()),
            Aggregation::Max {
                ignore_no_data: true,
            } => Ok(self
                .create_subquery(
                    no_data_ignoring_fold_future::<P, MaxIgnoreNoDataAccFunction>,
                    P::min_value(),
                )
                .into_raster_overlap_adapter(&self.source, query, ctx, self.tiling_specification)
                .boxed()),
            Aggregation::Max {
                ignore_no_data: false,
            } => Ok(self
                .create_subquery(fold_future::<P, MaxAccFunction>, P::min_value())
                .into_raster_overlap_adapter(&self.source, query, ctx, self.tiling_specification)
                .boxed()),
            Aggregation::First {
                ignore_no_data: true,
            } => {
                let no_data_value = self
                    .no_data_value
                    .ok_or(error::Error::TemporalRasterAggregationFirstValidRequiresNoData)?;
                Ok(self
                    .create_subquery(
                        no_data_ignoring_fold_future::<P, FirstValidAccFunction>,
                        no_data_value,
                    )
                    .into_raster_overlap_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .boxed())
            }
            Aggregation::First {
                ignore_no_data: false,
            } => {
                let no_data_value = self
                    .no_data_value
                    .ok_or(error::Error::TemporalRasterAggregationFirstValidRequiresNoData)?;
                Ok(self
                    .create_subquery(first_tile_fold_future::<P>, no_data_value)
                    .into_raster_overlap_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .boxed())
            }
            Aggregation::Last {
                ignore_no_data: true,
            } => {
                let no_data_value = self
                    .no_data_value
                    .ok_or(error::Error::TemporalRasterAggregationLastValidRequiresNoData)?;
                Ok(self
                    .create_subquery(
                        no_data_ignoring_fold_future::<P, LastValidAccFunction>,
                        no_data_value,
                    )
                    .into_raster_overlap_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .boxed())
            }

            Aggregation::Last {
                ignore_no_data: false,
            } => {
                let no_data_value = self
                    .no_data_value
                    .ok_or(error::Error::TemporalRasterAggregationLastValidRequiresNoData)?;
                Ok(self
                    .create_subquery(last_tile_fold_future::<P>, no_data_value)
                    .into_raster_overlap_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .boxed())
            }

            Aggregation::Mean { ignore_no_data } => {
                let _ = self
                    .no_data_value
                    .ok_or(error::Error::TemporalRasterAggregationLastValidRequiresNoData)?;
                Ok(self
                    .create_subquery_mean(mean_tile_fold_future::<P>, ignore_no_data)
                    .into_raster_overlap_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .boxed())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{Measurement, SpatialResolution, TimeInterval},
        raster::{EmptyGrid, EmptyGrid2D, Grid2D, GridOrEmpty, RasterDataType, TileInformation},
        spatial_reference::SpatialReference,
    };
    use num_traits::AsPrimitive;

    use crate::{
        engine::{MockExecutionContext, MockQueryContext, RasterQueryRectangle},
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_min() {
        let (no_data_value, raster_tiles) = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Min {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[2].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[3].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1], no_data_value).unwrap()
                ),
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max() {
        let (no_data_value, raster_tiles) = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[2].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[3].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], no_data_value).unwrap()
                ),
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max_with_no_data() {
        let (no_data_value, mut raster_tiles) = make_raster();
        if let GridOrEmpty::Grid(ref mut g) = raster_tiles.get_mut(0).unwrap().grid_array {
            g.data[0] = no_data_value.unwrap();
        } else {
            panic!("test tile should not be empty");
        }

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new(
                        [3, 2].into(),
                        vec![no_data_value.unwrap(), 11, 10, 9, 8, 7],
                        no_data_value
                    )
                    .unwrap()
                ),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[2].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[3].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], no_data_value).unwrap()
                ),
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max_with_no_data_but_ignoring_it() {
        let (no_data_value, mut raster_tiles) = make_raster();
        if let GridOrEmpty::Grid(ref mut g) = raster_tiles.get_mut(0).unwrap().grid_array {
            g.data[0] = no_data_value.unwrap();
        } else {
            panic!("test tile should not be empty");
        }

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 4);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[2].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[3].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], no_data_value).unwrap()
                ),
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_only_no_data() {
        let no_data_value: Option<u8> = Some(42);

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![RasterTile2D::new_with_tile_info(
                    TimeInterval::new_unchecked(0, 20),
                    TileInformation {
                        global_tile_position: [-1, 0].into(),
                        tile_size_in_pixels: [3, 2].into(),
                        global_geo_transform: Default::default(),
                    },
                    GridOrEmpty::Empty(EmptyGrid::new([3, 2].into(), no_data_value.unwrap())),
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (2., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 20),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Empty(EmptyGrid::new([3, 2].into(), no_data_value.unwrap())),
            )
        );
    }

    #[tokio::test]
    async fn test_first_with_no_data() {
        let (no_data_value, raster_tiles) = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::First {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 16, 11, 12], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6], no_data_value).unwrap()
                ),
            )
        );
    }

    #[tokio::test]
    async fn test_last_with_no_data() {
        let (no_data_value, raster_tiles) = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Last {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![13, 8, 15, 16, 17, 18], no_data_value).unwrap()
                ),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6], no_data_value).unwrap()
                ),
            )
        );
    }

    #[tokio::test]
    async fn test_last() {
        let (no_data_value, raster_tiles) = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Last {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![13, 42, 15, 16, 17, 18], no_data_value)
                        .unwrap()
                )
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value.unwrap()))
            )
        );
    }

    #[tokio::test]
    async fn test_first() {
        let (no_data_value, raster_tiles) = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::First {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value.unwrap()))
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6], no_data_value).unwrap()
                )
            )
        );
    }

    #[tokio::test]
    async fn test_mean_nodata() {
        let (no_data_value, raster_tiles) = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Mean {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value.unwrap()))
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6], no_data_value).unwrap()
                )
            )
        );
    }

    #[tokio::test]
    async fn test_mean_ignore_nodata() {
        let (no_data_value, raster_tiles) = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Mean {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::default();

        let qp = agg
            .initialize(&exe_ctx)
            .await
            .unwrap()
            .query_processor()
            .unwrap()
            .get_u8()
            .unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![10, 8, 12, 16, 14, 15], no_data_value).unwrap()
                )
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6], no_data_value).unwrap()
                )
            )
        );
    }

    fn make_raster() -> (
        Option<u8>,
        Vec<geoengine_datatypes::raster::RasterTile2D<u8>>,
    ) {
        let no_data_value = Some(42);
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(30, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7], no_data_value).unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(30, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1], no_data_value).unwrap(),
                ),
            ),
        ];
        (no_data_value, raster_tiles)
    }

    fn make_raster_with_no_data() -> (
        Option<u8>,
        Vec<geoengine_datatypes::raster::RasterTile2D<u8>>,
    ) {
        let no_data_value = 42;
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value)),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6], Some(no_data_value))
                        .unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new(
                        [3, 2].into(),
                        vec![7, 8, 9, 42, 11, 12],
                        Some(no_data_value),
                    )
                    .unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value)),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Grid(
                    Grid2D::new(
                        [3, 2].into(),
                        vec![13, 42, 15, 16, 17, 18],
                        Some(no_data_value),
                    )
                    .unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: Default::default(),
                },
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into(), no_data_value)),
            ),
        ];
        (Some(no_data_value), raster_tiles)
    }
}
