use std::sync::Arc;

use futures::{future::BoxFuture, Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::{
    primitives::{SpatialPartitioned, TimeInstance, TimeInterval, TimeStep},
    raster::{
        EmptyGrid2D, GeoTransform, Grid2D, GridIdx2D, GridOrEmpty, GridOrEmpty2D, GridShapeAccess,
        NoDataValue, Pixel, RasterTile2D, TileInformation,
    },
};
use num_traits::AsPrimitive;
use rayon::ThreadPool;

use crate::{
    adapters::{FoldTileAccu, SubQueryTileAggregator},
    engine::RasterQueryRectangle,
    util::Result,
};

pub fn mean_tile_fold_future<T>(
    accu: TemporalMeanTileAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<TemporalMeanTileAccu<T>>>
where
    T: Pixel,
{
    tokio::task::spawn_blocking(|| {
        let mut accu = accu;
        accu.add_tile(tile)?;
        Ok(accu)
    })
    .then(|x| async move {
        match x {
            Ok(r) => r,
            Err(e) => Err(e.into()),
        }
    })
}

#[derive(Debug, Clone)]
pub struct TemporalMeanTileAccu<T> {
    time: TimeInterval,
    tile_position: GridIdx2D,
    global_geo_transform: GeoTransform,

    value_grid: GridOrEmpty2D<f64>,
    count_grid: Grid2D<u64>,
    ignore_no_data: bool,
    out_no_data_value: T,

    initial_state: bool,

    pool: Arc<ThreadPool>,
}

impl<T> TemporalMeanTileAccu<T> {
    pub fn add_tile(&mut self, in_tile: RasterTile2D<T>) -> Result<()>
    where
        T: Copy + AsPrimitive<f64> + Pixel,
    {
        self.time = self.time.union(&in_tile.time)?;

        let in_tile_grid = match in_tile.grid_array {
            GridOrEmpty::Grid(g) => g,
            GridOrEmpty::Empty(_) => {
                self.initial_state = false;
                return Ok(());
            }
        };

        match &mut self.value_grid {
            GridOrEmpty::Empty(_) if !self.initial_state && !self.ignore_no_data => {
                // every pixel is nodata we will keep it like this forever
            }

            GridOrEmpty::Empty(_) => {
                let mut accu_grid = self.value_grid.clone().into_materialized_grid();

                for ((acc_value, acc_count), new_value) in accu_grid
                    .data
                    .iter_mut()
                    .zip(self.count_grid.data.iter_mut())
                    .zip(in_tile_grid.data.iter())
                {
                    if in_tile_grid.is_no_data(*new_value) {
                        *acc_count = 0;
                    } else {
                        let ivf: f64 = new_value.as_();
                        *acc_value = ivf;
                        *acc_count = 1;
                    }
                }

                self.value_grid = accu_grid.into();
            }

            GridOrEmpty::Grid(accu_grid) => {
                for ((acc_value, acc_count), new_value) in accu_grid
                    .data
                    .iter_mut()
                    .zip(self.count_grid.data.iter_mut())
                    .zip(in_tile_grid.data.iter())
                {
                    if in_tile_grid.is_no_data(*new_value) {
                        // The input pixel value is nodata
                        if !self.ignore_no_data {
                            // once nodata always nodata
                            *acc_count = 0;
                        }
                    } else {
                        let ivf: f64 = new_value.as_();
                        if self.ignore_no_data || *acc_count > 0 {
                            // we either ignore nodata, then we add all non-nodata pixels or the count is > 0, so not nodata
                            // *av += ivf;
                            *acc_count += 1;
                            let delta = ivf - *acc_value;
                            *acc_value += delta / (*acc_count as f64);
                        }
                    }
                }
            }
        }

        self.initial_state = false;
        Ok(())
    }
}

impl<T> FoldTileAccu for TemporalMeanTileAccu<T>
where
    T: Pixel,
{
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        let TemporalMeanTileAccu {
            time,
            tile_position,
            global_geo_transform,
            value_grid,
            count_grid,
            ignore_no_data: _,
            out_no_data_value,
            initial_state: _,
            pool: _pool,
        } = self;

        let value_grid = match value_grid {
            GridOrEmpty::Grid(g) => g,
            GridOrEmpty::Empty(_) => {
                return RasterTile2D::new(
                    time,
                    tile_position,
                    global_geo_transform,
                    EmptyGrid2D::new(value_grid.grid_shape(), out_no_data_value).into(),
                )
            }
        };

        let res: Vec<T> = value_grid
            .data
            .into_iter()
            .zip(count_grid.data.into_iter())
            .map(|(v, c)| {
                if c == 0 {
                    out_no_data_value
                } else {
                    T::from_(v) // lets hope that this works..
                }
            })
            .collect();

        let res_grid = Grid2D {
            shape: value_grid.shape,
            data: res,
            no_data_value: Some(out_no_data_value),
        };

        RasterTile2D::new(time, tile_position, global_geo_transform, res_grid.into())
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

#[derive(Debug, Clone)]
pub struct TemporalRasterMeanAggregationSubQuery<F, T: Pixel> {
    pub fold_fn: F,
    pub no_data_value: T,
    pub ignore_no_data: bool,
    pub step: TimeStep,
}

impl<'a, T, FoldM, FoldF> SubQueryTileAggregator<'a, T>
    for TemporalRasterMeanAggregationSubQuery<FoldM, T>
where
    T: Pixel,
    FoldM: Send + Sync + 'static + Clone + Fn(TemporalMeanTileAccu<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TemporalMeanTileAccu<T>, Error = crate::error::Error>,
{
    type TileAccu = TemporalMeanTileAccu<T>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        build_accu(
            query_rect,
            tile_info,
            pool.clone(),
            self.ignore_no_data,
            self.no_data_value,
        )
        .boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        Ok(Some(RasterQueryRectangle {
            spatial_bounds: tile_info.spatial_partition(),
            spatial_resolution: query_rect.spatial_resolution,
            time_interval: TimeInterval::new(start_time, (start_time + self.step)?)?,
        }))
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

fn build_accu<T: Pixel>(
    query_rect: RasterQueryRectangle,
    tile_info: TileInformation,
    pool: Arc<ThreadPool>,
    ignore_no_data: bool,
    no_data_value: T,
) -> impl Future<Output = Result<TemporalMeanTileAccu<T>>> {
    tokio::task::spawn_blocking(move || TemporalMeanTileAccu {
        time: query_rect.time_interval,
        tile_position: tile_info.global_tile_position,
        global_geo_transform: tile_info.global_geo_transform,
        value_grid: EmptyGrid2D::new(tile_info.tile_size_in_pixels, 0.).into(),
        count_grid: Grid2D::new_filled(tile_info.tile_size_in_pixels, 0, None),
        ignore_no_data,
        out_no_data_value: no_data_value,
        initial_state: true,
        pool,
    })
    .map_err(From::from)
}
