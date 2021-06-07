use crate::{
    adapters::{RasterOverlapAdapter, SubQueryTileAggregator},
    engine::{
        InitializedOperator, InitializedRasterOperator, QueryRectangle, RasterQueryProcessor,
        RasterResultDescriptor, TypedRasterQueryProcessor,
    },
    error,
    util::Result,
};
use async_trait::async_trait;
use futures::{Future, FutureExt, StreamExt, TryFuture};
use geoengine_datatypes::raster::GridOrEmpty;
use geoengine_datatypes::{
    primitives::{SpatialBounded, TimeInstance, TimeInterval, TimeStep},
    raster::{Grid2D, Pixel, RasterTile2D, TileInformation, TilingSpecification},
};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use typetag;

use crate::engine::{ExecutionContext, Operator, RasterOperator, SingleRasterSource};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TemporalRasterAggregationParameters {
    aggregation_type: AggregationType,
    window: TimeStep,
    ignore_no_data: bool,
    // TODO: allow specifying window start instead of using query.start?
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AggregationType {
    Min,
    Max,
}

pub type TemporalRasterAggregation =
    Operator<TemporalRasterAggregationParameters, SingleRasterSource>;

#[typetag::serde]
impl RasterOperator for TemporalRasterAggregation {
    fn initialize(
        self: Box<Self>,
        context: &dyn ExecutionContext,
    ) -> Result<Box<InitializedRasterOperator>> {
        ensure!(self.params.window.step > 0, error::WindowSizeMustNotBeZero);

        let source = self.sources.raster.initialize(context)?;

        let initialized_operator = InitializedTemporalRasterAggregation {
            aggregation_type: self.params.aggregation_type,
            window: self.params.window,
            ignore_no_data: self.params.ignore_no_data,
            result_descriptor: source.result_descriptor().clone(),
            source,
            tiling_specification: context.tiling_specification(),
        };

        Ok(initialized_operator.boxed())
    }
}

pub struct InitializedTemporalRasterAggregation {
    aggregation_type: AggregationType,
    window: TimeStep,
    ignore_no_data: bool,
    source: Box<InitializedRasterOperator>,
    result_descriptor: RasterResultDescriptor,
    tiling_specification: TilingSpecification,
}

impl InitializedOperator<RasterResultDescriptor, TypedRasterQueryProcessor>
    for InitializedTemporalRasterAggregation
{
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
                self.ignore_no_data,
                p,
                self.tiling_specification,
                self.source.result_descriptor().no_data_value
            )
            .boxed().into()
        );

        Ok(res)
    }
}

pub struct TemporalRasterAggregationProcessor<Q, P>
where
    Q: RasterQueryProcessor<RasterType = P>,
    P: Pixel,
{
    aggregation_type: AggregationType,
    window: TimeStep,
    ignore_no_data: bool,
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
        aggregation_type: AggregationType,
        window: TimeStep,
        ignore_no_data: bool,
        source: Q,
        tiling_specification: TilingSpecification,
        no_data_value: Option<f64>,
    ) -> Self {
        Self {
            aggregation_type,
            window,
            ignore_no_data,
            source,
            tiling_specification,
            no_data_value: no_data_value.map(P::from_),
        }
    }
}

#[async_trait]
impl<Q, P> RasterQueryProcessor for TemporalRasterAggregationProcessor<Q, P>
where
    P: Pixel,
    Q: RasterQueryProcessor<RasterType = P>,
{
    type RasterType = P;

    async fn raster_query<'a>(
        &'a self,
        query: crate::engine::QueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>> {
        match self.aggregation_type {
            AggregationType::Min => {
                if self.ignore_no_data {
                    let spec = TemporalRasterAggregationSubQuery {
                        fold_fn: no_data_ignoring_fold_future::<P, MinIgnoreNoDataAccFunction>,
                        no_data_value: self.no_data_value,
                        initial_value: P::max_value(),
                        step: self.window,
                    };

                    let s = RasterOverlapAdapter::<'a, P, _, _>::new(
                        &self.source,
                        query,
                        self.tiling_specification,
                        ctx,
                        spec,
                    );

                    Ok(s.boxed())
                } else {
                    let spec = TemporalRasterAggregationSubQuery {
                        fold_fn: fold_future::<P, MinAccFunction>,
                        no_data_value: self.no_data_value,
                        initial_value: P::max_value(),
                        step: self.window,
                    };

                    let s = RasterOverlapAdapter::<'a, P, _, _>::new(
                        &self.source,
                        query,
                        self.tiling_specification,
                        ctx,
                        spec,
                    );

                    Ok(s.boxed())
                }
            }
            AggregationType::Max => {
                if self.ignore_no_data {
                    let spec = TemporalRasterAggregationSubQuery {
                        fold_fn: no_data_ignoring_fold_future::<P, MaxIgnoreNoDataAccFunction>,
                        no_data_value: self.no_data_value,
                        initial_value: P::min_value(),
                        step: self.window,
                    };

                    let s = RasterOverlapAdapter::<'a, P, _, _>::new(
                        &self.source,
                        query,
                        self.tiling_specification,
                        ctx,
                        spec,
                    );

                    Ok(s.boxed())
                } else {
                    let spec = TemporalRasterAggregationSubQuery {
                        fold_fn: fold_future::<P, MaxAccFunction>,
                        no_data_value: self.no_data_value,
                        initial_value: P::min_value(),
                        step: self.window,
                    };

                    let s = RasterOverlapAdapter::<'a, P, _, _>::new(
                        &self.source,
                        query,
                        self.tiling_specification,
                        ctx,
                        spec,
                    );

                    Ok(s.boxed())
                }
            }
        }
    }
}

pub trait AccFunction {
    /// produce new accumulator value from current state and new value
    fn acc<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T;
}
pub trait NoDataIgnoringAccFunction {
    /// produce new accumulator value from current state and new value, ignoring no data values
    fn acc_ignore_no_data<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T;
}

struct MinAccFunction {}

impl AccFunction for MinAccFunction {
    fn acc<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T {
        if let Some(no_data) = no_data {
            if acc == no_data || value == no_data {
                return no_data;
            }
        }

        if acc < value {
            acc
        } else {
            value
        }
    }
}

struct MinIgnoreNoDataAccFunction {}

impl NoDataIgnoringAccFunction for MinIgnoreNoDataAccFunction {
    fn acc_ignore_no_data<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T {
        if let Some(no_data) = no_data {
            if value == no_data {
                return acc;
            } else if acc == no_data {
                return value;
            }
        }

        if acc < value {
            acc
        } else {
            value
        }
    }
}

struct MaxAccFunction {}

impl AccFunction for MaxAccFunction {
    fn acc<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T {
        if let Some(no_data) = no_data {
            if acc == no_data || value == no_data {
                return no_data;
            }
        }

        if acc > value {
            acc
        } else {
            value
        }
    }
}

struct MaxIgnoreNoDataAccFunction {}

impl NoDataIgnoringAccFunction for MaxIgnoreNoDataAccFunction {
    fn acc_ignore_no_data<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T {
        if let Some(no_data) = no_data {
            if value == no_data {
                return acc;
            } else if acc == no_data {
                return value;
            }
        }

        if acc > value {
            acc
        } else {
            value
        }
    }
}

fn fold_fn<T, C>(acc: (RasterTile2D<T>, ()), tile: RasterTile2D<T>) -> (RasterTile2D<T>, ())
where
    T: Pixel,
    C: AccFunction,
{
    let mut acc = acc.0;
    let grid = match (acc.grid_array, tile.grid_array) {
        (GridOrEmpty::Grid(mut a), GridOrEmpty::Grid(g)) => {
            a.data = a
                .inner_ref()
                .iter()
                .zip(g.inner_ref())
                .map(|(x, y)| C::acc(a.no_data_value, *x, *y))
                .collect();
            GridOrEmpty::Grid(a)
        }
        (GridOrEmpty::Empty(e), _) | (_, GridOrEmpty::Empty(e)) => GridOrEmpty::Empty(e),
    };

    acc.grid_array = grid;
    (acc, ())
}

fn no_data_ignoring_fold_fn<T, C>(
    acc: (RasterTile2D<T>, ()),
    tile: RasterTile2D<T>,
) -> (RasterTile2D<T>, ())
where
    T: Pixel,
    C: NoDataIgnoringAccFunction,
{
    let mut acc = acc.0;
    let grid = match (acc.grid_array, tile.grid_array) {
        (GridOrEmpty::Grid(mut a), GridOrEmpty::Grid(g)) => {
            a.data = a
                .inner_ref()
                .iter()
                .zip(g.inner_ref())
                .map(|(x, y)| C::acc_ignore_no_data(a.no_data_value, *x, *y))
                .collect();
            GridOrEmpty::Grid(a)
        }
        (GridOrEmpty::Grid(a), GridOrEmpty::Empty(_)) => GridOrEmpty::Grid(a),
        (GridOrEmpty::Empty(_), GridOrEmpty::Grid(g)) => GridOrEmpty::Grid(g),
        (GridOrEmpty::Empty(a), GridOrEmpty::Empty(_)) => GridOrEmpty::Empty(a),
    };

    acc.grid_array = grid;
    (acc, ())
}

pub fn fold_future<T, C>(
    accu: (RasterTile2D<T>, ()),
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<(RasterTile2D<T>, ())>>
where
    T: Pixel,
    C: AccFunction,
{
    tokio::task::spawn_blocking(|| fold_fn::<T, C>(accu, tile)).then(async move |x| match x {
        Ok(r) => Ok(r),
        Err(e) => Err(e.into()),
    })
}

pub fn no_data_ignoring_fold_future<T, C>(
    accu: (RasterTile2D<T>, ()),
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<(RasterTile2D<T>, ())>>
where
    T: Pixel,
    C: NoDataIgnoringAccFunction,
{
    tokio::task::spawn_blocking(|| no_data_ignoring_fold_fn::<T, C>(accu, tile)).then(
        async move |x| match x {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        },
    )
}

#[derive(Debug, Clone)]
pub struct TemporalRasterAggregationSubQuery<F, T: Pixel> {
    fold_fn: F,
    no_data_value: Option<T>,
    initial_value: T,
    step: TimeStep,
}

impl<T, FoldM, FoldF> SubQueryTileAggregator<T> for TemporalRasterAggregationSubQuery<FoldM, T>
where
    T: Pixel,
    FoldM: Send + Clone + Fn((RasterTile2D<T>, ()), RasterTile2D<T>) -> FoldF,
    FoldF: TryFuture<Ok = (RasterTile2D<T>, ()), Error = crate::error::Error>,
{
    fn result_no_data_value(&self) -> Option<T> {
        self.no_data_value
    }

    fn initial_fill_value(&self) -> T {
        self.initial_value
    }

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: QueryRectangle,
    ) -> Result<(RasterTile2D<T>, ())> {
        let output_raster = Grid2D::new_filled(
            tile_info.tile_size_in_pixels,
            self.initial_fill_value(),
            self.result_no_data_value(),
        );
        Ok((
            RasterTile2D::new_with_tile_info(
                query_rect.time_interval,
                tile_info,
                GridOrEmpty::Grid(output_raster),
            ),
            (),
        ))
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: QueryRectangle,
        start_time: TimeInstance,
    ) -> Result<QueryRectangle> {
        Ok(QueryRectangle {
            bbox: tile_info.spatial_bounds(),
            spatial_resolution: query_rect.spatial_resolution,
            time_interval: TimeInterval::new(start_time, (start_time + self.step)?)?,
        })
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }

    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type FoldCompanion = ();
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Measurement, SpatialResolution},
        raster::{EmptyGrid, RasterDataType},
        spatial_reference::SpatialReference,
    };
    use num_traits::AsPrimitive;

    use crate::{
        engine::{MockExecutionContext, MockQueryContext},
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
                aggregation_type: AggregationType::Min,
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                ignore_no_data: false,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (4., 3.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };

        let qp = agg
            .initialize(&exe_ctx)
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
                aggregation_type: AggregationType::Max,
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                ignore_no_data: false,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (4., 3.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };

        let qp = agg
            .initialize(&exe_ctx)
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
            panic!("test tile should not be empty")
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
                aggregation_type: AggregationType::Max,
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                ignore_no_data: false,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (4., 3.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };

        let qp = agg
            .initialize(&exe_ctx)
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
            panic!("test tile should not be empty")
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
                aggregation_type: AggregationType::Max,
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                ignore_no_data: true,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (4., 3.).into()),
            time_interval: TimeInterval::new_unchecked(0, 40),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };

        let qp = agg
            .initialize(&exe_ctx)
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
                aggregation_type: AggregationType::Max,
                window: TimeStep {
                    granularity: geoengine_datatypes::primitives::TimeGranularity::Millis,
                    step: 20,
                },
                ignore_no_data: false,
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let exe_ctx = MockExecutionContext {
            tiling_specification: TilingSpecification::new((0., 0.).into(), [3, 2].into()),
            ..Default::default()
        };
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (2., 3.).into()),
            time_interval: TimeInterval::new_unchecked(0, 20),
            spatial_resolution: SpatialResolution::one(),
        };
        let query_ctx = MockQueryContext {
            chunk_byte_size: 1024 * 1024,
        };

        let qp = agg
            .initialize(&exe_ctx)
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
}
