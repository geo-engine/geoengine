use std::marker::PhantomData;
use std::sync::Arc;

use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{Future, FutureExt, TryFuture, TryFutureExt};
use geoengine_datatypes::operations::reproject::Reproject;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, CacheHint, RasterQueryRectangle, SpatialPartition2D,
    SpatialPartitioned,
};
use geoengine_datatypes::raster::{
    Grid2D, GridIndexAccess, GridIntersection, GridSize, SpatialGridDefinition,
    UpdateIndexedElementsParallel,
};
use geoengine_datatypes::{
    operations::reproject::{CoordinateProjection, CoordinateProjector},
    primitives::TimeInterval,
    raster::EmptyGrid,
    spatial_reference::SpatialReference,
};
use geoengine_datatypes::{
    primitives::{Coordinate2D, TimeInstance},
    raster::{CoordinatePixelAccess, GridIdx2D, Pixel, RasterTile2D, TileInformation},
};
use num;
use rayon::ThreadPool;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::slice::{ParallelSlice, ParallelSliceMut};
use tracing::debug;

use super::{FoldTileAccu, FoldTileAccuMut, SubQueryTileAggregator};

#[derive(Debug, Clone, Copy)]
pub struct TileReprojectionSubqueryGridInfo {
    pub in_spatial_grid: SpatialGridDefinition,
    pub out_spatial_grid: SpatialGridDefinition,
}

#[derive(Debug)]
pub struct TileReprojectionSubQuery<T, F> {
    pub in_srs: SpatialReference,
    pub out_srs: SpatialReference,
    pub fold_fn: F,
    pub state: TileReprojectionSubqueryGridInfo,
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
        build_accu(
            &query_rect,
            pool.clone(),
            tile_info,
            self.state,
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
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        // this are the pixels we are interested in
        debug_assert_eq!(
            tile_info.global_geo_transform,
            self.state.out_spatial_grid.geo_transform()
        );

        // why is this none?
        let valid_pixel_bounds = dbg!(
            dbg!(
                self.state
                    .out_spatial_grid
                    .grid_bounds()
                    .intersection(&tile_info.global_pixel_bounds())
            )
            .and_then(|b| b.intersection(&query_rect.spatial_query.grid_bounds()))
        );

        log::debug!(
            "ÖÖÖÖÖ valid_pixel_bounds {:?} -> {:?}",
            tile_info.global_pixel_bounds(),
            valid_pixel_bounds
        );

        let valid_spatial_bounds = valid_pixel_bounds.map(|pb| {
            self.state
                .out_spatial_grid
                .geo_transform()
                .grid_to_spatial_bounds(&pb)
        });

        log::debug!(
            "ÖÖÖÖÖ valid_spatial_bounds {:?} -> {:?}",
            query_rect.spatial_query.grid_bounds(),
            valid_spatial_bounds
        );

        if let Some(bounds) = valid_spatial_bounds {
            let proj = CoordinateProjector::from_known_srs(self.out_srs, self.in_srs)?;
            let projected_bounds = bounds.reproject(&proj);

            log::debug!(
                "ÖÖÖÖÖ projected_bounds {:?} -> {:?}",
                bounds,
                projected_bounds
            );

            match projected_bounds {
                Ok(pb) => {
                    dbg!("produce something");
                    Ok(Some(RasterQueryRectangle::new_with_grid_bounds(
                        self.state
                            .in_spatial_grid
                            .geo_transform()
                            .spatial_to_grid_bounds(&pb),
                        TimeInterval::new_instant(start_time)?,
                        band_idx.into(),
                    )))
                }
                // In some strange cases the reprojection can return an empty box.
                // We ignore it since it contains no pixels.
                Err(geoengine_datatypes::error::Error::OutputBboxEmpty { bbox: _ }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        } else {
            dbg!("output query rectangle is not valid in source projection => produce empty tile");
            log::debug!(
                "ÖÖÖÖÖ output query rectangle is not valid in source projection => produce empty tile {:?}",
                self.state
                    .out_spatial_grid
                    .geo_transform()
                    .grid_to_spatial_bounds(&query_rect.spatial_query.grid_bounds())
            );

            // output query rectangle is not valid in source projection => produce empty tile
            Ok(None)
        }
    }

    fn fold_method(&self) -> Self::FoldMethod {
        self.fold_fn.clone()
    }
}

fn build_accu<T: Pixel>(
    query_rect: &RasterQueryRectangle,
    pool: Arc<ThreadPool>,
    tile_info: TileInformation,
    state: TileReprojectionSubqueryGridInfo,
    out_srs: SpatialReference,
    in_srs: SpatialReference,
) -> impl Future<Output = Result<TileWithProjectionCoordinates<T>>> + use<T> {
    let time_interval = query_rect.time_interval;
    crate::util::spawn_blocking(move || {
        let output_raster = EmptyGrid::new(tile_info.tile_size_in_pixels);

        let pool = pool.clone();

        // if there is a valid output spatial partition, we need to reproject the coordinates.
        let projected_coords = projected_coordinate_grid_parallel(
            &pool,
            tile_info,
            out_srs,
            in_srs,
            &state.out_spatial_grid.spatial_partition(),
        )?;

        Ok(TileWithProjectionCoordinates {
            accu_tile: RasterTile2D::new_with_tile_info(
                time_interval,
                tile_info,
                0,
                output_raster.into(),
                CacheHint::max_duration(),
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

        let mut in_coord_grid: Grid2D<Option<Coordinate2D>> =
            Grid2D::new_filled(tile_info.tile_size_in_pixels, None);

        let out_coords = tile_info
            .spatial_grid_definition()
            .generate_coord_grid_pixel_center();

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

        in_coord_grid
            .data
            .par_chunks_mut(par_chunk_size)
            .zip(out_coords.data.par_chunks(par_chunk_size))
            .try_for_each(|(in_coord_slice, out_coord_slice)| {
                debug_assert_eq!(
                    in_coord_slice.len(),
                    out_coord_slice.len(),
                    "slices must be equal"
                );
                let chunk_bounds = BoundingBox2D::from_coord_ref_iter(out_coord_slice.iter());

                if chunk_bounds.is_none() {
                    debug!("reprojection early exit");
                    return Ok(());
                }

                let chunk_bounds = chunk_bounds.expect("checked above");
                let valid_out_area = valid_out_area.as_bbox();

                let proj = CoordinateProjector::from_known_srs(out_srs, in_srs)?;

                if valid_out_area.contains_bbox(&chunk_bounds) {
                    debug!("reproject whole tile chunk");
                    let in_coords = proj.project_coordinates(out_coord_slice)?;
                    in_coord_slice
                        .iter_mut()
                        .zip(in_coords.into_iter())
                        .for_each(|(a, b)| *a = Some(b));
                } else if valid_out_area.intersects_bbox(&chunk_bounds) {
                    debug!("reproject part of tile chunk");
                    in_coord_slice
                        .iter_mut()
                        .zip(out_coord_slice.iter())
                        .for_each(|(in_coord, out_coord)| {
                            *in_coord = if valid_out_area.contains_coordinate(out_coord) {
                                proj.project_coordinate(*out_coord).ok()
                            } else {
                                None
                            };
                        });
                } else {
                    // do nothing. Should be unreachable
                }
                Result::<(), crate::error::Error>::Ok(())
            })?;
        Ok(in_coord_grid)
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

    accu.set_time(t_union);
    accu.set_cache_hint(accu.accu_tile.cache_hint.merged(&tile.cache_hint));

    if tile.grid_array.is_empty() {
        return Ok(accu);
    }

    let TileWithProjectionCoordinates {
        mut accu_tile,
        coords,
        pool,
    } = accu;

    pool.install(|| {
        let tile_bounding_box = tile.spatial_partition();

        let map_fn = |grid_idx: GridIdx2D, accu_value: Option<T>| {
            let lookup_coord = coords.get_at_grid_index_unchecked(grid_idx);
            let lookup_value = lookup_coord
                .filter(|coord| tile_bounding_box.contains_coordinate(coord))
                .and_then(|coord| tile.pixel_value_at_coord_unchecked(coord));
            lookup_value.or(accu_value)
        };

        accu_tile.update_indexed_elements_parallel(map_fn);
    });

    Ok(TileWithProjectionCoordinates {
        accu_tile,
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
        debug_assert!(self.accu_tile.time.end() > self.accu_tile.time.start());
        debug_assert!(self.accu_tile.time.end() != self.accu_tile.time.start() + 1);
        Ok(self.accu_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel> FoldTileAccuMut for TileWithProjectionCoordinates<T> {
    fn set_time(&mut self, time: TimeInterval) {
        debug_assert!(time.end() > time.start());
        self.accu_tile.time = time;
    }

    fn set_cache_hint(&mut self, cache_hint: CacheHint) {
        self.accu_tile.cache_hint = cache_hint;
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use geoengine_datatypes::{
        primitives::BandSelection,
        raster::{
            BoundedGrid, GeoTransform, Grid, GridBoundingBox2D, GridShape, GridShape2D,
            RasterDataType, SpatialGridDefinition, TilesEqualIgnoringCacheHint,
            TilingSpecification,
        },
        util::test::TestDefault,
    };

    use crate::{
        adapters::RasterSubQueryAdapter,
        engine::{
            MockExecutionContext, RasterBandDescriptors, RasterOperator, RasterResultDescriptor,
            SpatialGridDescriptor, WorkflowOperatorPath,
        },
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
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![1_u8, 2, 3, 4])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![13, 14, 15, 16])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(5, 10),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![19, 20, 21, 22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: None,
            spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 2.), 1., -1.),
                GridShape::new_2d(2, 4).bounding_box(),
            )),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let tiling_spec = TilingSpecification::new(GridShape2D::new([2, 2]));

        let tiling_grid = result_descriptor.tiling_grid_definition(tiling_spec);
        let tiling_strat = tiling_grid.generate_data_tiling_strategy();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(tiling_spec);

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let query_rect = RasterQueryRectangle::new_with_grid_bounds(
            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 10),
            BandSelection::first(),
        );

        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let op = mrs1
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_u8();
        let qp = qp.unwrap();

        let state_gen = TileReprojectionSubQuery {
            in_srs: projection,
            out_srs: projection,
            fold_fn: fold_by_coordinate_lookup_future,
            state: TileReprojectionSubqueryGridInfo {
                in_spatial_grid: tiling_grid.tiling_spatial_grid_definition(),
                out_spatial_grid: tiling_grid.tiling_spatial_grid_definition(),
            },
            _phantom_data: PhantomData,
        };
        let a = RasterSubQueryAdapter::new(&qp, query_rect, tiling_strat, &query_ctx, state_gen);
        let res = a
            .map(Result::unwrap)
            .map(Option::unwrap)
            .collect::<Vec<_>>()
            .await;
        assert!(data.tiles_equal_ignoring_cache_hint(&res));
    }
}
