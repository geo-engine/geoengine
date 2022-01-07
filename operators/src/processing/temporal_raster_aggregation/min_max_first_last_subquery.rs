use std::sync::Arc;

use futures::{future::BoxFuture, Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::{
    primitives::{SpatialPartitioned, TimeInstance, TimeInterval, TimeStep},
    raster::{EmptyGrid2D, Grid2D, GridOrEmpty, Pixel, RasterTile2D, TileInformation},
};
use rayon::ThreadPool;

use crate::{
    adapters::{FoldTileAccu, FoldTileAccuMut, SubQueryTileAggregator},
    engine::{QueryRectangle, RasterQueryRectangle},
    util::Result,
};

pub trait AccFunction {
    /// produce new accumulator value from current state and new value
    fn acc<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T;
}
pub trait NoDataIgnoringAccFunction {
    /// produce new accumulator value from current state and new value, ignoring no data values
    fn acc_ignore_no_data<T: Pixel>(no_data: Option<T>, acc: T, value: T) -> T;
}

pub struct MinAccFunction {}

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

pub struct MinIgnoreNoDataAccFunction {}

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

pub struct MaxAccFunction {}

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

pub struct MaxIgnoreNoDataAccFunction {}

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

pub struct LastValidAccFunction {}

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
pub struct FirstValidAccFunction {}

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

pub fn fold_fn<T, C>(
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
        accu_tile,
        initial_state: false,
        pool: acc.pool,
    }
}

pub fn no_data_ignoring_fold_fn<T, C>(
    acc: TemporalRasterAggregationTileAccu<T>,
    tile: RasterTile2D<T>,
) -> TemporalRasterAggregationTileAccu<T>
where
    T: Pixel,
    C: NoDataIgnoringAccFunction,
{
    let TemporalRasterAggregationTileAccu {
        mut accu_tile,
        initial_state: _initial_state,
        pool,
    } = acc;

    let grid = match (accu_tile.grid_array, tile.grid_array) {
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

    accu_tile.grid_array = grid;
    TemporalRasterAggregationTileAccu {
        accu_tile,
        initial_state: false,
        pool,
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
    tokio::task::spawn_blocking(|| fold_fn::<T, C>(accu, tile)).then(|x| async move {
        match x {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
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
        |x| async move {
            match x {
                Ok(r) => Ok(r),
                Err(e) => Err(e.into()),
            }
        },
    )
}

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
    tokio::task::spawn_blocking(|| first_tile_fold_fn(accu, tile)).then(move |x| async move {
        match x {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
    })
}

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
    tokio::task::spawn_blocking(|| last_tile_fold_fn(accu, tile)).then(move |x| async move {
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

impl<T: Pixel> FoldTileAccu for TemporalRasterAggregationTileAccu<T> {
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        self.accu_tile
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
    pub no_data_value: Option<T>,
    pub initial_value: T,
    pub step: TimeStep,
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
        build_temporal_accu(
            query_rect,
            tile_info,
            pool.clone(),
            self.no_data_value,
            self.initial_value,
        )
        .boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(QueryRectangle {
            spatial_bounds: tile_info.spatial_partition(),
            spatial_resolution: query_rect.spatial_resolution,
            time_interval: TimeInterval::new(start_time, (start_time + self.step)?)?,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

fn build_temporal_accu<T: Pixel>(
    query_rect: RasterQueryRectangle,
    tile_info: TileInformation,
    pool: Arc<ThreadPool>,
    no_data_value: Option<T>,
    initial_value: T,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>> {
    tokio::task::spawn_blocking(move || {
        let output_raster = if let Some(no_data_value) = no_data_value {
            EmptyGrid2D::new(tile_info.tile_size_in_pixels, no_data_value).into()
        } else {
            Grid2D::new_filled(tile_info.tile_size_in_pixels, initial_value, no_data_value).into()
        };
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

#[derive(Debug, Clone)]
pub struct TemporalRasterAggregationSubQueryNoDataOnly<F, T: Pixel> {
    pub fold_fn: F,
    pub no_data_value: T,
    pub initial_value: T,
    pub step: TimeStep,
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
        build_temporal_no_data_accu(query_rect, tile_info, pool.clone(), self.no_data_value).boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(QueryRectangle {
            spatial_bounds: tile_info.spatial_partition(),
            spatial_resolution: query_rect.spatial_resolution,
            time_interval: TimeInterval::new(start_time, (start_time + self.step)?)?,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

fn build_temporal_no_data_accu<T: Pixel>(
    query_rect: RasterQueryRectangle,
    tile_info: TileInformation,
    pool: Arc<ThreadPool>,
    no_data_value: T,
) -> impl Future<Output = Result<TemporalRasterAggregationTileAccu<T>>> {
    tokio::task::spawn_blocking(move || {
        let output_raster = EmptyGrid2D::new(tile_info.tile_size_in_pixels, no_data_value).into();

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
