use crate::{
    adapters::{FoldTileAccu, FoldTileAccuMut, SubQueryTileAggregator},
    util::Result,
};
use async_trait::async_trait;
use futures::{future::BoxFuture, Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::{
    primitives::{
        RasterQueryRectangle, SpatialPartitioned, SpatialQuery, TimeInstance, TimeInterval,
        TimeStep,
    },
    raster::{EmptyGrid2D, Pixel, RasterTile2D, TileInformation},
};
use rayon::ThreadPool;
use std::{marker::PhantomData, sync::Arc};

/// Only outputs the first tile as accumulator.
pub fn first_tile_fold_fn<T>(
    acc: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> TemporalRasterAggregationTileAccu<T>
where
    T: Pixel,
{
    if acc.initial_state {
        let mut next_accu = tile;
        next_accu.time = acc.accu_tile.time;

        TemporalRasterAggregationTileAccu {
            accu_tile: next_accu,
            initial_state: false,
            pool: acc.pool,
        }
    } else {
        acc
    }
}

pub fn first_tile_fold_future<T>(
    accu: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>>
where
    T: Pixel,
{
    crate::util::spawn_blocking(|| first_tile_fold_fn(accu, tile)).then(move |x| async move {
        match x {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
    })
}

/// Only outputs the last tile as accumulator.
#[allow(clippy::needless_pass_by_value)]
pub fn last_tile_fold_fn<T>(
    acc: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> TemporalRasterAggregationTileAccu<T>
where
    T: Pixel,
{
    let mut next_accu = tile;
    next_accu.time = acc.accu_tile.time;

    TemporalRasterAggregationTileAccu {
        accu_tile: next_accu,
        initial_state: false,
        pool: acc.pool,
    }
}

pub fn last_tile_fold_future<T>(
    accu: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>>
where
    T: Pixel,
{
    crate::util::spawn_blocking(|| last_tile_fold_fn(accu, tile)).then(move |x| async move {
        match x {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
    })
}

#[derive(Debug, Clone)]
pub struct TemporalRasterAggregationTileAccu<T> {
    accu_tile: RasterTile2D<T>,
    initial_state: bool,
    pool: Arc<ThreadPool>,
}

#[async_trait]
impl<T: Pixel> FoldTileAccu for TemporalRasterAggregationTileAccu<T> {
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        Ok(self.accu_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel> FoldTileAccuMut for TemporalRasterAggregationTileAccu<T> {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType> {
        &mut self.accu_tile
    }
}

#[derive(Debug, Clone)]
pub struct TemporalRasterAggregationSubQuery<F, T: Pixel> {
    pub fold_fn: F,
    pub step: TimeStep,
    pub step_reference: TimeInstance,
    pub _phantom_pixel_type: PhantomData<T>,
}

impl<'a, T, FoldM, FoldF> SubQueryTileAggregator<'a, T>
    for TemporalRasterAggregationSubQuery<FoldM, T>
where
    T: Pixel,
    FoldM: Send
        + Sync
        + 'static
        + Clone
        + Fn(TemporalRasterAggregationTileAccu<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TemporalRasterAggregationTileAccu<T>, Error = crate::error::Error>,
{
    type TileAccu = TemporalRasterAggregationTileAccu<T>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        build_temporal_accu(query_rect, tile_info, pool.clone()).boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        let snapped_start = self.step.snap_relative(self.step_reference, start_time)?;
        Ok(Some(
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                // TODO: we shond use the pixelspace here
                tile_info.spatial_partition(),
                query_rect.spatial_query().spatial_resolution(),
                query_rect.spatial_query().origin_coordinate(),
                TimeInterval::new(snapped_start, (snapped_start + self.step)?)?,
            ),
        ))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

fn build_temporal_accu<T: Pixel>(
    query_rect: RasterQueryRectangle,
    tile_info: TileInformation,
    pool: Arc<ThreadPool>,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>> {
    crate::util::spawn_blocking(move || TemporalRasterAggregationTileAccu {
        accu_tile: RasterTile2D::new_with_tile_info(
            query_rect.time_interval,
            tile_info,
            EmptyGrid2D::new(tile_info.tile_size_in_pixels).into(),
        ),
        initial_state: true,
        pool,
    })
    .map_err(From::from)
}

#[derive(Debug, Clone)]
pub struct TemporalRasterAggregationSubQueryNoDataOnly<F, T: Pixel> {
    pub fold_fn: F,
    pub step: TimeStep,
    pub step_reference: TimeInstance,
    pub _phantom_pixel_type: PhantomData<T>,
}

impl<'a, T, FoldM, FoldF> SubQueryTileAggregator<'a, T>
    for TemporalRasterAggregationSubQueryNoDataOnly<FoldM, T>
where
    T: Pixel,
    FoldM: Send
        + Sync
        + 'static
        + Clone
        + Fn(TemporalRasterAggregationTileAccu<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TemporalRasterAggregationTileAccu<T>, Error = crate::error::Error>,
{
    type TileAccu = TemporalRasterAggregationTileAccu<T>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        build_temporal_no_data_accu(query_rect, tile_info, pool.clone()).boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        let snapped_start_time = self.step.snap_relative(self.step_reference, start_time)?;
        Ok(Some(
            RasterQueryRectangle::with_partition_and_resolution_and_origin(
                tile_info.spatial_partition(),
                query_rect.spatial_query().spatial_resolution(),
                query_rect.spatial_query().origin_coordinate(),
                TimeInterval::new(snapped_start_time, (snapped_start_time + self.step)?)?,
            ),
        ))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

fn build_temporal_no_data_accu<T: Pixel>(
    query_rect: RasterQueryRectangle,
    tile_info: TileInformation,
    pool: Arc<ThreadPool>,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>> {
    crate::util::spawn_blocking(move || {
        let output_raster = EmptyGrid2D::new(tile_info.tile_size_in_pixels).into();

        TemporalRasterAggregationTileAccu {
            accu_tile: RasterTile2D::new_with_tile_info(
                query_rect.time_interval,
                tile_info,
                output_raster,
            ),
            initial_state: true,
            pool,
        }
    })
    .map_err(From::from)
}
