use std::sync::Arc;

use crate::engine::RasterQueryRectangle;
use crate::error;
use crate::util::Result;
use futures::{FutureExt, TryFuture};
use geoengine_datatypes::operations::reproject::Reproject;
use geoengine_datatypes::primitives::{SpatialPartition2D, SpatialPartitioned};
use geoengine_datatypes::raster::{Grid2D, GridIndexAccess, GridSize};
use geoengine_datatypes::{
    operations::reproject::{CoordinateProjection, CoordinateProjector},
    primitives::{SpatialResolution, TimeInterval},
    raster::EmptyGrid,
    spatial_reference::SpatialReference,
};
use geoengine_datatypes::{
    primitives::{Coordinate2D, TimeInstance},
    raster::{CoordinatePixelAccess, GridIdx2D, Pixel, RasterTile2D, TileInformation},
};
use rayon::ThreadPool;

use log::debug;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;

use super::{FoldTileAccu, FoldTileAccuMut, SubQueryTileAggregator};

#[derive(Debug)]
pub struct TileReprojectionSubQuery<T, F> {
    pub in_srs: SpatialReference,
    pub out_srs: SpatialReference,
    pub no_data_and_fill_value: T,
    pub fold_fn: F,
    pub in_spatial_res: SpatialResolution,
    pub valid_bounds_in: Option<SpatialPartition2D>,
    pub valid_bounds_out: Option<SpatialPartition2D>,
}

impl<T, FoldM, FoldF> SubQueryTileAggregator<T> for TileReprojectionSubQuery<T, FoldM>
where
    T: Pixel,
    FoldM: Send + Clone + Fn(TileWithProjectionCoordinates<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = TileWithProjectionCoordinates<T>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Result<Self::TileAccu> {
        // println!("new_fold_accu {:?}", &tile_info.global_tile_position);

        let output_raster =
            EmptyGrid::new(tile_info.tile_size_in_pixels, self.no_data_and_fill_value);

        let pool = pool.clone();

        // if there is a valid output spatial partition, we need to reproject the coordinates.
        let projected_coords = if let Some(valid_out_area) = self.valid_bounds_out {
            projected_coordinate_grid_parallel(
                &pool,
                tile_info,
                self.out_srs,
                self.in_srs,
                &valid_out_area,
            )?
        } else {
            debug!("reproject tile outside valid bounds"); // TODO: error?
            Grid2D::new_filled(tile_info.tile_size_in_pixels, None, None)
        };

        Ok(TileWithProjectionCoordinates {
            accu_tile: RasterTile2D::new_with_tile_info(
                query_rect.time_interval,
                tile_info,
                output_raster.into(),
            ),
            coords: projected_coords,
            pool,
        })
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        start_time: TimeInstance,
    ) -> Result<Option<RasterQueryRectangle>> {
        // this is the spatial partition we are interested in
        let valid_spatial_bounds = self
            .valid_bounds_out
            .and_then(|vo| vo.intersection(&tile_info.spatial_partition()))
            .and_then(|vo| vo.intersection(&query_rect.spatial_partition()));
        if let Some(bounds) = valid_spatial_bounds {
            let proj = CoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;
            let projected_bounds = bounds.reproject(&proj)?;
            Ok(Some(RasterQueryRectangle {
                spatial_bounds: projected_bounds,
                time_interval: TimeInterval::new_instant(start_time)?,
                spatial_resolution: self.in_spatial_res,
            }))
        } else {
            // output query rectangle is not valid in source projection => produce empty tile
            Ok(None)
        }
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

fn projected_coordinate_grid_parallel(
    pool: &ThreadPool,
    tile_info: TileInformation,
    out_srs: SpatialReference,
    in_srs: SpatialReference,
    valid_out_area: &SpatialPartition2D,
) -> Result<Grid2D<Option<Coordinate2D>>> {
    const MIN_ROWS_IN_PAR_CHUNK: usize = 64; // this must never be smaller than 1

    let start = std::time::Instant::now();

    let res = pool.install(|| {
        // get all pixel idxs and coordinates.
        debug!(
            "projected_coordinate_grid_parallel {:?}",
            &tile_info.global_tile_position
        );

        let mut coord_grid: Grid2D<Option<Coordinate2D>> =
            Grid2D::new_filled(tile_info.tile_size_in_pixels, None, None);

        let tile_geo_transform = tile_info.tile_geo_transform();

        let parallelism = pool.current_num_threads();
        let par_chunk_split =
            (tile_info.tile_size_in_pixels.axis_size_y() / parallelism).max(MIN_ROWS_IN_PAR_CHUNK); // don't go below MIN_ROWS_IN_PAR_CHUNK lines per chunk.
        let par_chunk_size = tile_info.tile_size_in_pixels.axis_size_x() * par_chunk_split;
        debug!(
            "parallelism: threads={} par_chunk_split={} par_chunk_size={}",
            pool.current_num_threads(),
            par_chunk_split,
            par_chunk_size
        );

        let axis_size_x = tile_info.tile_size_in_pixels.axis_size_x();

        let res = coord_grid
            .data
            .par_chunks_mut(par_chunk_size)
            .enumerate()
            .try_for_each(|(chunk_idx, opt_coord_slice)| {
                let chunk_start_y = chunk_idx * par_chunk_split;
                let chunk_len = opt_coord_slice.len();
                let out_coords = (0..chunk_len)
                    .map(|lin_idx| {
                        let x_idx = lin_idx % axis_size_x;
                        let y_idx = lin_idx / axis_size_x + chunk_start_y;
                        let grid_idx = GridIdx2D::from([y_idx as isize, x_idx as isize]);
                        tile_geo_transform.grid_idx_to_upper_left_coordinate_2d(grid_idx)
                    })
                    .collect::<Vec<Coordinate2D>>();

                let chunk_bounds =
                    SpatialPartition2D::new_unchecked(out_coords[0], out_coords[chunk_len - 1]);

                let proj = CoordinateProjector::from_known_srs(out_srs, in_srs)?;

                if valid_out_area.contains(&chunk_bounds) {
                    debug!("reproject whole tile chunk");
                    let in_coords = proj.project_coordinates(&out_coords)?;
                    opt_coord_slice
                        .iter_mut()
                        .zip(in_coords.into_iter())
                        .for_each(|(opt_coord, in_coord)| *opt_coord = Some(in_coord));
                } else if valid_out_area.intersects(&chunk_bounds) {
                    debug!("reproject part of tile chunk");
                    opt_coord_slice
                        .iter_mut()
                        .zip(out_coords.into_iter())
                        .for_each(|(opt_coord, idx_coord)| {
                            let in_coord = if valid_out_area.contains_coordinate(&idx_coord) {
                                proj.project_coordinate(idx_coord).ok()
                            } else {
                                None
                            };
                            *opt_coord = in_coord;
                        });
                } else {
                    debug!("reproject empty tile chunk");
                }
                Result::<(), crate::error::Error>::Ok(())
            });
        res.map(|_| coord_grid)
    });
    debug!(
        "projected_coordinate_grid_parallel took {} (ns)",
        start.elapsed().as_nanos()
    );
    res
}

#[allow(dead_code)]
pub fn fold_by_coordinate_lookup_future<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> impl TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>
where
    T: Pixel,
{
    //  println!("fold_by_coordinate_lookup_future {:?}", &tile.tile_position);
    tokio::task::spawn_blocking(|| fold_by_coordinate_lookup_impl(accu, tile)).then(
        |x| async move {
            match x {
                Ok(r) => r,
                Err(e) => Err(e.into()),
            }
        },
    )
}

#[allow(dead_code)]
#[allow(clippy::type_complexity)]
#[allow(clippy::needless_pass_by_value)]
pub fn fold_by_coordinate_lookup_impl<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> Result<TileWithProjectionCoordinates<T>>
where
    T: Pixel,
{
    const MIN_ROWS_IN_PAR_CHUNK: usize = 32; // this must never be smaller than 1

    // println!("fold_by_coordinate_lookup_impl {:?}", &tile.tile_position);
    let mut accu = accu;
    let t_union = accu.accu_tile.time.union(&tile.time)?;

    accu.tile_mut().time = t_union;

    if tile.grid_array.is_empty() {
        return Ok(accu);
    }

    let TileWithProjectionCoordinates {
        accu_tile,
        coords,
        pool,
    } = accu;

    let mut materialized_accu_tile = accu_tile.into_materialized_tile(); //in a fold chain the real materialization should only happen once. All other calls will be simple conversions.

    pool.install(|| {
        let parallelism = pool.current_num_threads();
        let par_chunk_split = (materialized_accu_tile.grid_array.shape.axis_size_y() / parallelism)
            .max(MIN_ROWS_IN_PAR_CHUNK); // don't go below MIN_ROWS_IN_PAR_CHUNK lines per chunk.
        let par_chunk_size =
            materialized_accu_tile.grid_array.shape.axis_size_x() * par_chunk_split;

        materialized_accu_tile
            .grid_array
            .data
            .par_chunks_mut(par_chunk_size)
            .enumerate()
            .for_each(|(y_f, row_slice)| {
                let y = y_f * par_chunk_split;
                row_slice.iter_mut().enumerate().for_each(|(x, pixel)| {
                    let lookup_coord = coords
                        .get_at_grid_index_unchecked(GridIdx2D::from([y as isize, x as isize]));
                    if let Some(coord) = lookup_coord {
                        if tile.spatial_partition().contains_coordinate(&coord) {
                            let lookup_value = tile.pixel_value_at_coord_unchecked(coord);
                            *pixel = lookup_value;
                        }
                    }
                });
            });
    });

    Ok(TileWithProjectionCoordinates {
        accu_tile: materialized_accu_tile.into(),
        coords,
        pool,
    })
}

#[derive(Debug, Clone)]
pub struct TileWithProjectionCoordinates<T> {
    accu_tile: RasterTile2D<T>,
    coords: Grid2D<Option<Coordinate2D>>,
    pool: Arc<ThreadPool>,
}

impl<T: Pixel> FoldTileAccu for TileWithProjectionCoordinates<T> {
    type RasterType = T;

    fn into_tile(self) -> RasterTile2D<Self::RasterType> {
        self.accu_tile
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel> FoldTileAccuMut for TileWithProjectionCoordinates<T> {
    fn tile_mut(&mut self) -> &mut RasterTile2D<Self::RasterType> {
        &mut self.accu_tile
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::Measurement,
        raster::{Grid, GridShape, RasterDataType},
        util::test::TestDefault,
    };
    use num_traits::AsPrimitive;

    use crate::{
        adapters::RasterSubQueryAdapter,
        engine::{MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor},
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    async fn identity_projection() {
        let projection = SpatialReference::epsg_4326();
        let no_data_value = Some(0);

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22], no_data_value)
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext::default();
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).await.unwrap();

        let raster_res_desc: &RasterResultDescriptor = op.result_descriptor();

        let no_data_v = raster_res_desc
            .no_data_value
            .map(AsPrimitive::as_)
            .expect("must be 0 since we specified it above");

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let valid_bounds = projection.area_of_use_projected().unwrap();

        let state_gen = TileReprojectionSubQuery {
            in_srs: projection,
            out_srs: projection,
            no_data_and_fill_value: no_data_v,
            fold_fn: fold_by_coordinate_lookup_future,
            in_spatial_res: query_rect.spatial_resolution,
            valid_bounds_in: Some(valid_bounds),
            valid_bounds_out: Some(valid_bounds),
        };
        let a = RasterSubQueryAdapter::new(&qp, query_rect, tiling_strat, &query_ctx, state_gen);
        let res = a
            .map(Result::unwrap)
            .map(Option::unwrap)
            .collect::<Vec<RasterTile2D<u8>>>()
            .await;
        assert_eq!(data, res);
    }
}
