use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use futures::{future::BoxFuture, Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::{
    primitives::{RasterQueryRectangle, SpatialPartitioned, TimeInstance, TimeInterval, TimeStep},
    raster::{
        EmptyGrid2D, GeoTransform, GridIdx2D, GridOrEmpty, GridOrEmpty2D, GridShapeAccess,
        MapIndexedElements, MapMaskedElements, Pixel, RasterTile2D,
        TileInformation,
    },
};
use num_traits::AsPrimitive;
use rayon::ThreadPool;

use crate::{
    adapters::{FoldTileAccu, SubQueryTileAggregator},
    util::Result,
};

pub fn mean_tile_fold_future<T>(
    accu: TemporalMeanTileAccu<T>,
    tile: RasterTile2D<T>,
) -> impl Future<Output = Result<TemporalMeanTileAccu<T>>>
where
    T: Pixel,
{
    crate::util::spawn_blocking(|| {
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
    value_grid: GridOrEmpty2D<(f64, u64)>,
    ignore_no_data: bool,
    initial_state: bool,
    pool: Arc<ThreadPool>,
    _phantom_pixel_type: PhantomData<T>,
}

impl<T> TemporalMeanTileAccu<T> {
    pub fn add_tile(&mut self, in_tile: RasterTile2D<T>) -> Result<()>
    where
        T: Copy + AsPrimitive<f64> + Pixel,
    {
        self.time = self.time.union(&in_tile.time)?;

        debug_assert!(self.value_grid.grid_shape() == in_tile.grid_shape());

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
                // this could stay empty?
                let accu_grid = self.value_grid.clone().into_materialized_grid();

                let map_fn = |lin_idx: usize, _acc_values_option| {
                    let new_value_option = in_tile_grid.at_linear_index_unchecked_deref(lin_idx);
                    if let Some(new_value) = new_value_option {
                        let ivf: f64 = new_value.as_();
                        Some((ivf, 1))
                    } else {
                        None
                    }
                };

                self.value_grid = accu_grid.map_index_elements(map_fn).into(); // TODO: could also do this parallel
            }

            GridOrEmpty::Grid(_) => {
                let accu_grid = self.value_grid.clone().into_materialized_grid(); // TODO do not clone!

                let map_fn = |lin_idx: usize, acc_values_option: Option<(f64, u64)>| {
                    let new_value_option = in_tile_grid.at_linear_index_unchecked_deref(lin_idx);
                    match (acc_values_option, new_value_option) {
                        (None, Some(new_value)) if self.ignore_no_data || self.initial_state => {
                            let ivf: f64 = new_value.as_();
                            Some((ivf, 1))
                        }
                        (Some(acc_value), None) if self.ignore_no_data => Some(acc_value),
                        (Some((acc_value, acc_count)), Some(new_value)) => {
                            let ivf: f64 = new_value.as_();
                            let new_acc_count = acc_count + 1;
                            let delta = ivf - acc_value;
                            let new_acc_value = acc_value + delta / (new_acc_count as f64);
                            Some((new_acc_value, new_acc_count))
                        }
                        (None, Some(_) | None) | (Some(_), None) => None,
                    }
                };

                self.value_grid = accu_grid.map_index_elements(map_fn).into(); // TODO: could also do this parallel
            }
        }

        self.initial_state = false;
        Ok(())
    }
}

#[async_trait]
impl<T> FoldTileAccu for TemporalMeanTileAccu<T>
where
    T: Pixel,
{
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        let TemporalMeanTileAccu {
            time,
            tile_position,
            global_geo_transform,
            value_grid,
            ignore_no_data: _,
            initial_state: _,
            pool: _pool,
            _phantom_pixel_type: _,
        } = self;

        let value_grid = match value_grid {
            GridOrEmpty::Grid(g) => g,
            GridOrEmpty::Empty(_) => {
                return Ok(RasterTile2D::new(
                    time,
                    tile_position,
                    global_geo_transform,
                    EmptyGrid2D::new(value_grid.grid_shape()).into(),
                ))
            }
        };

        let map_fn = |value_option| {
            if let Some((acc_value, acc_count)) = value_option {
                if acc_count == 0 {
                    None
                } else {
                    Some(T::from_(acc_value))
                }
            } else {
                None
            }
        };

        let res_grid = value_grid.map_or_mask_elements(map_fn);

        Ok(RasterTile2D::new(
            time,
            tile_position,
            global_geo_transform,
            res_grid.into(),
        ))
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

#[derive(Debug, Clone)]
pub struct TemporalRasterMeanAggregationSubQuery<F, T: Pixel> {
    pub fold_fn: F,
    pub ignore_no_data: bool,
    pub step: TimeStep,
    pub step_reference: TimeInstance,
    pub _phantom_pixel_type: PhantomData<T>,
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
        build_accu(query_rect, tile_info, pool.clone(), self.ignore_no_data).boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        let snapped_start = self.step.snap_relative(self.step_reference, start_time)?;
        Ok(Some(RasterQueryRectangle {
            spatial_bounds: tile_info.spatial_partition(),
            spatial_resolution: query_rect.spatial_resolution,
            time_interval: TimeInterval::new(snapped_start, (snapped_start + self.step)?)?,
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
) -> impl Future<Output = Result<TemporalMeanTileAccu<T>>> {
    crate::util::spawn_blocking(move || TemporalMeanTileAccu {
        time: query_rect.time_interval,
        tile_position: tile_info.global_tile_position,
        global_geo_transform: tile_info.global_geo_transform,
        value_grid: EmptyGrid2D::new(tile_info.tile_size_in_pixels).into(),
        ignore_no_data,
        initial_state: true,
        pool,
        _phantom_pixel_type: PhantomData,
    })
    .map_err(From::from)
}
