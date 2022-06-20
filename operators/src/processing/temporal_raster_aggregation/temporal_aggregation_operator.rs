use std::marker::PhantomData;

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
use geoengine_datatypes::primitives::{RasterQueryRectangle, SpatialPartition2D, TimeInstance};
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
    TemporalRasterAggregationSubQueryNoDataOnly,
};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporalRasterAggregationParameters {
    aggregation: Aggregation,
    window: TimeStep,
    /// Define an anchor point for `window`
    /// If `None`, the anchor point is `1970-01-01T00:00:00Z` by default
    window_reference: Option<TimeInstance>,
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
            window_reference: self
                .params
                .window_reference
                .unwrap_or(TimeInstance::EPOCH_START),
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
    window_reference: TimeInstance,
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
                self.window_reference,
                p,
                self.tiling_specification,
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
    window_reference: TimeInstance,
    source: Q,
    tiling_specification: TilingSpecification,
}

impl<Q, P> TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    fn new(
        aggregation_type: Aggregation,
        window: TimeStep,
        window_reference: TimeInstance,
        source: Q,
        tiling_specification: TilingSpecification,
    ) -> Self {
        Self {
            aggregation_type,
            window,
            window_reference,
            source,
            tiling_specification,
        }
    }

    fn create_subquery<F>(
        &self,
        fold_fn: F,
        initial_value: P,
    ) -> TemporalRasterAggregationSubQuery<F, P> {
        TemporalRasterAggregationSubQuery {
            fold_fn,
            step: self.window,
            step_reference: self.window_reference,
            _phantom_pixel_type: PhantomData,
        }
    }

    fn create_subquery_first<F>(
        &self,
        fold_fn: F,
    ) -> Result<TemporalRasterAggregationSubQueryNoDataOnly<F, P>> {
        Ok(TemporalRasterAggregationSubQueryNoDataOnly {
            fold_fn,
            step: self.window,
            step_reference: self.window_reference,
            _phantom_pixel_type: PhantomData,
        })
    }

    fn create_subquery_last<F>(
        &self,
        fold_fn: F,
    ) -> Result<TemporalRasterAggregationSubQueryNoDataOnly<F, P>> {
        Ok(TemporalRasterAggregationSubQueryNoDataOnly {
            fold_fn,
            step: self.window,
            step_reference: self.window_reference,
            _phantom_pixel_type: PhantomData,
        })
    }

    fn create_subquery_mean<F>(
        &self,
        fold_fn: F,
        ignore_no_data: bool,
    ) -> Result<TemporalRasterMeanAggregationSubQuery<F, P>> {
        Ok(TemporalRasterMeanAggregationSubQuery {
            fold_fn,
            step: self.window,
            step_reference: self.window_reference,
            ignore_no_data,
            _phantom_pixel_type: PhantomData,
        })
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
        query: RasterQueryRectangle,
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
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Min")),
            Aggregation::Min {
                ignore_no_data: false,
            } => Ok(self
                .create_subquery(fold_future::<P, MinAccFunction>, P::max_value())
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Min")),
            Aggregation::Max {
                ignore_no_data: true,
            } => Ok(self
                .create_subquery(
                    no_data_ignoring_fold_future::<P, MaxIgnoreNoDataAccFunction>,
                    P::min_value(),
                )
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Max")),
            Aggregation::Max {
                ignore_no_data: false,
            } => Ok(self
                .create_subquery(fold_future::<P, MaxAccFunction>, P::min_value())
                .into_raster_subquery_adapter(&self.source, query, ctx, self.tiling_specification)
                .expect("no tiles must be skipped in Aggregation::Max")),

            Aggregation::First {
                ignore_no_data: true,
            } => self
                .create_subquery_first(no_data_ignoring_fold_future::<P, FirstValidAccFunction>)
                .map(|o| {
                    o.into_raster_subquery_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .expect("no tiles must be skipped in Aggregation::First")
                }),
            Aggregation::First {
                ignore_no_data: false,
            } => self
                .create_subquery_first(first_tile_fold_future::<P>)
                .map(|o| {
                    o.into_raster_subquery_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .expect("no tiles must be skipped in Aggregation::First")
                }),
            Aggregation::Last {
                ignore_no_data: true,
            } => self
                .create_subquery_last(no_data_ignoring_fold_future::<P, LastValidAccFunction>)
                .map(|o| {
                    o.into_raster_subquery_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .expect("no tiles must be skipped in Aggregation::Last")
                }),

            Aggregation::Last {
                ignore_no_data: false,
            } => self
                .create_subquery_last(last_tile_fold_future::<P>)
                .map(|o| {
                    o.into_raster_subquery_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .expect("no tiles must be skipped in Aggregation::Last")
                }),

            Aggregation::Mean { ignore_no_data } => self
                .create_subquery_mean(mean_tile_fold_future::<P>, ignore_no_data)
                .map(|o| {
                    o.into_raster_subquery_adapter(
                        &self.source,
                        query,
                        ctx,
                        self.tiling_specification,
                    )
                    .expect("no tiles must be skipped in Aggregation::Mean")
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use geoengine_datatypes::{
        primitives::{Measurement, SpatialResolution, TimeInterval},
        raster::{
            EmptyGrid, EmptyGrid2D, Grid2D, GridOrEmpty, MaskedGrid2D, RasterDataType,
            TileInformation,
        },
        spatial_reference::SpatialReference,
        util::test::TestDefault,
    };

    use crate::{
        engine::{MockExecutionContext, MockQueryContext},
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_min() {
        let raster_tiles = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Min {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1]).unwrap()),
            )
        );

        assert_eq!(
            result[2].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
            )
        );

        assert_eq!(
            result[3].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1]).unwrap()),
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max() {
        let raster_tiles = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
            )
        );

        assert_eq!(
            result[2].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
            )
        );

        assert_eq!(
            result[3].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max_with_no_data() {
        let mut raster_tiles = make_raster(); // TODO: switch to make_raster_with_no_data?

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![0, 11, 10, 9, 8, 7],).unwrap()),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
            )
        );

        assert_eq!(
            result[2].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
            )
        );

        assert_eq!(
            result[3].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
            )
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_max_with_no_data_but_ignoring_it() {
        let mut raster_tiles = make_raster(); // TODO: switch to make_raster_with_no_data?

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
            )
        );

        assert_eq!(
            result[2].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
            )
        );

        assert_eq!(
            result[3].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
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
                        global_geo_transform: TestDefault::test_default(),
                    },
                    GridOrEmpty::from(EmptyGrid2D::<u8>::new([3, 2].into())),
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Max {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (2., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 20),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::Empty(EmptyGrid::new([3, 2].into())),
            )
        );
    }

    #[tokio::test]
    async fn test_first_with_no_data() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::First {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 16, 11, 12]).unwrap()),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6]).unwrap()),
            )
        );
    }

    #[tokio::test]
    async fn test_last_with_no_data() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Last {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![13, 8, 15, 16, 17, 18]).unwrap()),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6]).unwrap()),
            )
        );
    }

    #[tokio::test]
    async fn test_last() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Last {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    Grid2D::new([3, 2].into(), vec![13, 42, 15, 16, 17, 18]).unwrap()
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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::Empty(EmptyGrid2D::new([3, 2].into()))
            )
        );
    }

    #[tokio::test]
    async fn test_first() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::First {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into()))
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6]).unwrap())
            )
        );
    }

    #[tokio::test]
    async fn test_mean_nodata() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Mean {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into()))
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6]).unwrap())
            )
        );
    }

    #[tokio::test]
    async fn test_mean_ignore_nodata() {
        let raster_tiles = make_raster_with_no_data();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Mean {
                    ignore_no_data: true,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: None,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![10, 8, 12, 16, 14, 15]).unwrap())
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6]).unwrap())
            )
        );
    }

    #[tokio::test]
    async fn test_query_not_aligned_with_window_reference() {
        let raster_tiles = make_raster();

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: raster_tiles,
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

        let agg = TemporalRasterAggregation {
            params: TemporalRasterAggregationParameters {
                aggregation: Aggregation::Last {
                    ignore_no_data: false,
                },
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 30,
                },
                window_reference: Some(TimeInstance::EPOCH_START),
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(TilingSpecification::new(
            (0., 0.).into(),
            [3, 2].into(),
        ));
        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 3.).into(), (4., 0.).into()),
            time_interval: TimeInterval::new_unchecked(5, 5),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext::test_default();

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
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
            )
        );

        assert_eq!(
            result[1].as_ref().unwrap(),
            &RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
            )
        );
    }

    fn make_raster() -> Vec<geoengine_datatypes::raster::RasterTile2D<u8>> {
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![7, 8, 9, 10, 11, 12]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(30, 40),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![12, 11, 10, 9, 8, 7]).unwrap()),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(30, 40),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(Grid2D::new([3, 2].into(), vec![6, 5, 4, 3, 2, 1]).unwrap()),
            ),
        ];
        raster_tiles
    }

    fn make_raster_with_no_data() -> Vec<geoengine_datatypes::raster::RasterTile2D<u8>> {
        let raster_tiles = vec![
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(0, 10),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![1, 2, 3, 42, 5, 6]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap(),
                    )
                    .unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![7, 8, 9, 42, 11, 12]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, true, true, false, true, true])
                            .unwrap(),
                    )
                    .unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(10, 20),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 0].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(
                    MaskedGrid2D::new(
                        Grid2D::new([3, 2].into(), vec![13, 42, 15, 16, 17, 18]).unwrap(),
                        Grid2D::new([3, 2].into(), vec![true, false, true, true, true, true])
                            .unwrap(),
                    )
                    .unwrap(),
                ),
            ),
            RasterTile2D::new_with_tile_info(
                TimeInterval::new_unchecked(20, 30),
                TileInformation {
                    global_tile_position: [-1, 1].into(),
                    tile_size_in_pixels: [3, 2].into(),
                    global_geo_transform: TestDefault::test_default(),
                },
                GridOrEmpty::from(EmptyGrid2D::new([3, 2].into())),
            ),
        ];
        raster_tiles
    }
}
