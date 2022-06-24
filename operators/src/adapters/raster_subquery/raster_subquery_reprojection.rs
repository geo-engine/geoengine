use std::marker::PhantomData;
use std::sync::Arc;

use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::operations::reproject::Reproject;
use geoengine_datatypes::primitives::{
    RasterQueryRectangle, SpatialPartition2D, SpatialPartitioned,
};
use geoengine_datatypes::raster::{Grid2D, GridIndexAccess, GridSize, MapIndexedElementsParallel};
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
use log::debug;
use num;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use rayon::ThreadPool;

use super::{FoldTileAccu, FoldTileAccuMut, SubQueryTileAggregator};

#[derive(Debug)]
pub struct TileReprojectionSubQuery<T, F> {
    pub in_srs: SpatialReference,
    pub out_srs: SpatialReference,
    pub fold_fn: F,
    pub in_spatial_res: SpatialResolution,
    pub valid_bounds_in: Option<SpatialPartition2D>,
    pub valid_bounds_out: Option<SpatialPartition2D>,
    pub _phantom_data: PhantomData<T>,
}

impl<'a, T, FoldM, FoldF> SubQueryTileAggregator<'a, T> for TileReprojectionSubQuery<T, FoldM>
where
    T: Pixel,
    FoldM: Send
        + Sync
        + 'static
        + Clone
        + Fn(TileWithProjectionCoordinates<T>, RasterTile2D<T>) -> FoldF,
    FoldF: Send + TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>,
{
    type FoldFuture = FoldF;

    type FoldMethod = FoldM;

    type TileAccu = TileWithProjectionCoordinates<T>;
    type TileAccuFuture = BoxFuture<'a, Result<Self::TileAccu>>;

    fn new_fold_accu(
        &self,
        tile_info: TileInformation,
        query_rect: RasterQueryRectangle,
        pool: &Arc<ThreadPool>,
    ) -> Self::TileAccuFuture {
        // println!("new_fold_accu {:?}", &tile_info.global_tile_position);

        build_accu(
            query_rect,
            pool.clone(),
            tile_info,
            self.valid_bounds_out,
            self.out_srs,
            self.in_srs,
        )
        .boxed()
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
            let projected_bounds = bounds.reproject(&proj);

            match projected_bounds {
                Ok(pb) => Ok(Some(RasterQueryRectangle {
                    spatial_bounds: pb,
                    time_interval: TimeInterval::new_instant(start_time)?,
                    spatial_resolution: self.in_spatial_res,
                })),
                // In some strange cases the reprojection can return an empty box.
                // We ignore it since it contains no pixels.
                Err(geoengine_datatypes::error::Error::OutputBboxEmpty { bbox: _ }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        } else {
            // output query rectangle is not valid in source projection => produce empty tile
            Ok(None)
        }
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

fn build_accu<T: Pixel>(
    query_rect: RasterQueryRectangle,
    pool: Arc<ThreadPool>,
    tile_info: TileInformation,
    valid_bounds_out: Option<SpatialPartition2D>,
    out_srs: SpatialReference,
    in_srs: SpatialReference,
) -> impl Future<Output = Result<TileWithProjectionCoordinates<T>>> {
    crate::util::spawn_blocking(move || {
        let output_raster = EmptyGrid::new(tile_info.tile_size_in_pixels);

        let pool = pool.clone();

        // if there is a valid output spatial partition, we need to reproject the coordinates.
        let projected_coords = if let Some(valid_out_area) = valid_bounds_out {
            projected_coordinate_grid_parallel(&pool, tile_info, out_srs, in_srs, &valid_out_area)?
        } else {
            debug!("reproject tile outside valid bounds"); // TODO: error?
            Grid2D::new_filled(tile_info.tile_size_in_pixels, None)
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
    })
    .map_err(From::from)
    .and_then(|x| async { x }) // flatten Ok(Ok())
}

fn projected_coordinate_grid_parallel(
    // TODO: use a map/create_* _parallel method from Grid / Tile
    pool: &ThreadPool,
    tile_info: TileInformation,
    out_srs: SpatialReference,
    in_srs: SpatialReference,
    valid_out_area: &SpatialPartition2D,
) -> Result<Grid2D<Option<Coordinate2D>>> {
    const MIN_ELEMENTS_IN_PAR_CHUNK: usize = 64 * 512; // this must never be smaller than 1
    let min_rows_in_par_chunk = num::integer::div_ceil(
        MIN_ELEMENTS_IN_PAR_CHUNK,
        tile_info.tile_size_in_pixels.axis_size_x(),
    )
    .max(1);

    let start = std::time::Instant::now();

    let res = pool.install(|| {
        // get all pixel idxs and coordinates.
        debug!(
            "projected_coordinate_grid_parallel {:?}",
            &tile_info.global_tile_position
        );

        let mut coord_grid: Grid2D<Option<Coordinate2D>> =
            Grid2D::new_filled(tile_info.tile_size_in_pixels, None);

        let tile_geo_transform = tile_info.tile_geo_transform();

        let parallelism = pool.current_num_threads();
        let par_chunk_split =
            num::integer::div_ceil(tile_info.tile_size_in_pixels.axis_size_y(), parallelism)
                .max(min_rows_in_par_chunk); // don't go below MIN_ROWS_IN_PAR_CHUNK lines per chunk.
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
                let chunk_end_y = chunk_start_y + (chunk_len / axis_size_x) - 1;
                let out_coords = (0..chunk_len)
                    .map(|lin_idx| {
                        let x_idx = lin_idx % axis_size_x;
                        let y_idx = lin_idx / axis_size_x + chunk_start_y;
                        let grid_idx = GridIdx2D::from([y_idx as isize, x_idx as isize]);
                        tile_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(grid_idx)
                    })
                    .collect::<Vec<Coordinate2D>>();

                // the output bounds start at the top left corner of the chunk.
                let ul_grid_idx = GridIdx2D::from([chunk_start_y as isize, 0_isize]);
                let ul_coord =
                    tile_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(ul_grid_idx);

                // the output bounds must cover the whole chunk pixels.
                let lr_grid_idx =
                    GridIdx2D::from([chunk_end_y as isize, (axis_size_x - 1) as isize]);
                let lr_coord =
                    tile_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(lr_grid_idx + 1);

                let chunk_bounds = SpatialPartition2D::new_unchecked(ul_coord, lr_coord);

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
    crate::util::spawn_blocking(|| fold_by_coordinate_lookup_impl(accu, tile)).then(
        |x| async move {
            match x {
                Ok(r) => r,
                Err(e) => Err(e.into()),
            }
        },
    )
}

#[allow(clippy::type_complexity)]
#[allow(clippy::needless_pass_by_value)]
pub fn fold_by_coordinate_lookup_impl<T>(
    accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> Result<TileWithProjectionCoordinates<T>>
where
    T: Pixel,
{
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

    let new_accu = pool.install(|| {
        let tile_bounding_box = tile.spatial_partition();

        let map_fn = |grid_idx: GridIdx2D, accu_value: Option<T>| {
            let lookup_coord = coords.get_at_grid_index_unchecked(grid_idx);
            let lookup_value = lookup_coord
                .filter(|coord| tile_bounding_box.contains_coordinate(coord))
                .and_then(|coord| tile.pixel_value_at_coord_unchecked(coord));
            lookup_value.or(accu_value)
        };

        accu_tile.map_index_elements_parallel(map_fn)
    });

    Ok(TileWithProjectionCoordinates {
        accu_tile: new_accu,
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

#[async_trait]
impl<T: Pixel> FoldTileAccu for TileWithProjectionCoordinates<T> {
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        Ok(self.accu_tile)
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

    use crate::{
        adapters::RasterSubQueryAdapter,
        engine::{MockExecutionContext, MockQueryContext, RasterOperator, RasterResultDescriptor},
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[tokio::test]
    async fn identity_projection() {
        let projection = SpatialReference::epsg_4326();

        let data = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1, 2, 3, 4]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
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
                    time: None,
                    bbox: None,
                },
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 10),
            spatial_resolution: SpatialResolution::one(),
        };

        let query_ctx = MockQueryContext::test_default();
        let tiling_strat = exe_ctx.tiling_specification;

        let op = mrs1.initialize(&exe_ctx).await.unwrap();

        let qp = op.query_processor().unwrap().get_u8().unwrap();

        let valid_bounds = projection.area_of_use_projected().unwrap();

        let state_gen = TileReprojectionSubQuery {
            in_srs: projection,
            out_srs: projection,
            fold_fn: fold_by_coordinate_lookup_future,
            in_spatial_res: query_rect.spatial_resolution,
            valid_bounds_in: Some(valid_bounds),
            valid_bounds_out: Some(valid_bounds),
            _phantom_data: PhantomData,
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
