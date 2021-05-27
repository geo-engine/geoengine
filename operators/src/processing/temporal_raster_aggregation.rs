use crate::adapters::{FoldTileAccu, FoldTileAccuMut};
use crate::engine::{ExecutionContext, Operator, RasterOperator, SingleRasterSource};
use crate::{
    adapters::SubQueryTileAggregator,
    engine::{
        InitializedOperator, InitializedRasterOperator, QueryRectangle, RasterQueryProcessor,
        RasterResultDescriptor, TypedRasterQueryProcessor,
    },
    error,
    util::Result,
};
use futures::{Future, FutureExt, StreamExt, TryFuture};
use geoengine_datatypes::raster::{EmptyGrid2D, GridOrEmpty};
use geoengine_datatypes::{
    primitives::{SpatialBounded, TimeInstance, TimeInterval, TimeStep},
    raster::{Grid2D, Pixel, RasterTile2D, TileInformation, TilingSpecification},
};
use log::debug;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use typetag;

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
}

impl<Q, P> RasterQueryProcessor for TemporalRasterAggregationProcessor<Q, P>
where
    P: Pixel,
    Q: RasterQueryProcessor<RasterType = P>,
{
    type RasterType = P;

    #[allow(clippy::too_many_lines)]
    fn raster_query<'a>(
        &'a self,
        query: crate::engine::QueryRectangle,
        ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<'a, Result<RasterTile2D<Self::RasterType>>>> {
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
                    .ok_or(error::Error::TemporalRasterAggregationFirstValidRequiresNoData)?;
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

struct LastValidAccFunction {}

impl NoDataIgnoringAccFunction for LastValidAccFunction {
    fn acc_ignore_no_data<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T {
        if let Some(no_data) = no_data {
            if value == no_data {
                return acc;
            }
        }
        value
    }
}
struct FirstValidAccFunction {}

impl NoDataIgnoringAccFunction for FirstValidAccFunction {
    fn acc_ignore_no_data<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T {
        if let Some(no_data) = no_data {
            if acc == no_data {
                return value;
            }
        }
        acc
    }
}

fn fold_fn<T, C>(
    acc: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> TemporalRasterAggregationTileAccu<T>
where
    T: Pixel,
    C: AccFunction,
{
    let mut accu_tile = acc.accu_tile;

    let grid = if acc.initial_state {
        tile.grid_array
    } else {
        match (accu_tile.grid_array, tile.grid_array) {
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
        }
    };

    accu_tile.grid_array = grid;
    TemporalRasterAggregationTileAccu {
        accu_tile: accu_tile,
        initial_state: false,
    }
}

fn no_data_ignoring_fold_fn<T, C>(
    acc: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> TemporalRasterAggregationTileAccu<T>
where
    T: Pixel,
    C: NoDataIgnoringAccFunction,
{
    let mut acc_tile = acc.into_tile();
    let grid = match (acc_tile.grid_array, tile.grid_array) {
        (GridOrEmpty::Grid(mut a), GridOrEmpty::Grid(g)) => {
            a.data = a
                .inner_ref()
                .iter()
                .zip(g.inner_ref())
                .map(|(x, y)| C::acc_ignore_no_data(a.no_data_value, *x, *y))
                .collect();
            GridOrEmpty::Grid(a)
        }
        // TODO: need to increase temporal validity?
        (GridOrEmpty::Grid(a), GridOrEmpty::Empty(_)) => GridOrEmpty::Grid(a),
        (GridOrEmpty::Empty(_), GridOrEmpty::Grid(g)) => GridOrEmpty::Grid(g),
        (GridOrEmpty::Empty(a), GridOrEmpty::Empty(_)) => GridOrEmpty::Empty(a),
    };

    acc_tile.grid_array = grid;
    TemporalRasterAggregationTileAccu {
        accu_tile: acc_tile,
        initial_state: false,
    }
}

pub fn fold_future<T, C>(
    accu: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>>
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
    accu: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>>
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

fn first_tile_fold_fn<T>(
    acc: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> Result<TemporalRasterAggregationTileAccu<T>>
where
    T: Pixel,
{
    if acc.initial_state {
        let mut next_accu = tile;
        next_accu.time = acc.accu_tile.time;

        Ok(TemporalRasterAggregationTileAccu {
            accu_tile: next_accu,
            initial_state: false,
        })
    } else {
        Ok(acc)
    }
}

pub fn first_tile_fold_future<T>(
    accu: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>>
where
    T: Pixel,
{
    tokio::task::spawn_blocking(|| first_tile_fold_fn(accu, tile)).then(async move |x| match x {
        Ok(r) => r,
        Err(e) => Err(e.into()),
    })
}

fn last_tile_fold_fn<T>(
    acc: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> Result<TemporalRasterAggregationTileAccu<T>>
where
    T: Pixel,
{
    let mut next_accu = tile;
    next_accu.time = acc.accu_tile.time;

    Ok(TemporalRasterAggregationTileAccu {
        accu_tile: next_accu,
        initial_state: false,
    })
}

pub fn last_tile_fold_future<T>(
    accu: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>>
where
    T: Pixel,
{
    tokio::task::spawn_blocking(|| last_tile_fold_fn(accu, tile)).then(async move |x| match x {
        Ok(r) => r,
        Err(e) => Err(e.into()),
    })
}

#[derive(Debug, Clone)]
pub struct TemporalRasterAggregationTileAccu<T> {
    accu_tile: RasterTile2D<T>,
    initial_state: bool,
}

impl<T: Pixel> FoldTileAccu for TemporalRasterAggregationTileAccu<T> {
    type RasterType = T;

    fn tile_ref(&self) -> &RasterTile2D<Self::RasterType> {
        &self.accu_tile
    }

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        self.accu_tile
    }
}

impl<T: Pixel> FoldTileAccuMut for TemporalRasterAggregationTileAccu<T> {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType> {
        &mut self.accu_tile
    }
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
    FoldM: Send + Clone + Fn(TemporalRasterAggregationTileAccu<T>, RasterTile2D<T>) -> FoldF,
    FoldF: TryFuture<Ok = TemporalRasterAggregationTileAccu<T>, Error = crate::error::Error>,
{
    type TileAccu = TemporalRasterAggregationTileAccu<T>;

    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

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
    ) -> Result<Self::TileAccu> {
        let output_raster = if let Some(no_data_value) = self.result_no_data_value() {
            EmptyGrid2D::new(tile_info.tile_size_in_pixels, no_data_value).into()
        } else {
            Grid2D::new_filled(
                tile_info.tile_size_in_pixels,
                self.initial_fill_value(),
                self.result_no_data_value(),
            )
            .into()
        };
        Ok(TemporalRasterAggregationTileAccu {
            accu_tile: RasterTile2D::new_with_tile_info(
                query_rect.time_interval,
                tile_info,
                output_raster,
            ),
            initial_state: true,
        })
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
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::{BoundingBox2D, Measurement, SpatialResolution},
        raster::{EmptyGrid, EmptyGrid2D, RasterDataType},
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
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (4., 3.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
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
        let query_rect = QueryRectangle {
            bbox: BoundingBox2D::new_unchecked((0., 0.).into(), (4., 3.).into()),
            time_interval: TimeInterval::new_unchecked(0, 30),
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
