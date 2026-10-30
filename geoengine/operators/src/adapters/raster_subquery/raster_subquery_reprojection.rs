use std::cell::RefCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError};

use crate::error;
use crate::util::Result;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFuture};
use geoengine_datatypes::operations::reproject::Reproject;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, CacheHint, RasterQueryRectangle, SpatialPartition2D,
};
use geoengine_datatypes::raster::{
    GeoTransform, GridBoundingBox2D, GridBoundingBoxExt, GridBounds, GridContains,
    GridIntersection, GridSize, SpatialGridDefinition,
};
use geoengine_datatypes::{
    primitives::Coordinate2D,
    raster::{GridIdx2D, Pixel, RasterTile2D, TileInformation},
};
use geoengine_datatypes::{
    primitives::TimeInterval, raster::EmptyGrid, spatial_reference::CoordinateProjection,
    spatial_reference::DefaultCoordinateProjector, spatial_reference::SpatialReference,
};
use num;
use rayon::ThreadPool;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use rayon::slice::{ParallelSlice, ParallelSliceMut};
use tokio::sync::OnceCell;

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
    pub index_cache: InputPixelIndicesCache,
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
        // the indices are computed by the first fold with data, so sub-queries without data project nothing
        let accu = TileWithProjectionCoordinates {
            accu_tile: RasterTile2D::new_with_tile_info(
                query_rect.time_interval(),
                tile_info,
                0,
                EmptyGrid::new(tile_info.tile_size_in_pixels).into(),
                CacheHint::max_duration(),
            ),
            indices: self.index_cache.get(tile_info.global_tile_position),
            spec: InputPixelIndicesSpec {
                tile_info,
                out_srs: self.out_srs,
                in_srs: self.in_srs,
                valid_out_area: self.state.out_spatial_grid.spatial_partition(),
                in_geo_transform: self.state.in_spatial_grid.geo_transform(),
            },
            delivered_bounds: None,
            pool: pool.clone(),
        };
        futures::future::ok(accu).boxed()
    }

    fn tile_query_rectangle(
        &self,
        tile_info: TileInformation,
        _query_rect: RasterQueryRectangle,
        time: TimeInterval,
        band_idx: u32,
    ) -> Result<Option<RasterQueryRectangle>> {
        // all pixels of the tile, not only the queried ones, since tiles are always computed whole
        debug_assert_eq!(
            tile_info.global_geo_transform,
            self.state.out_spatial_grid.geo_transform()
        );

        let valid_pixel_bounds = self
            .state
            .out_spatial_grid
            .grid_bounds()
            .intersection(&tile_info.global_pixel_bounds());

        let valid_spatial_bounds = valid_pixel_bounds.map(|pb| {
            self.state
                .out_spatial_grid
                .geo_transform()
                .grid_to_spatial_bounds(&pb)
        });

        if let Some(bounds) = valid_spatial_bounds {
            let projected_bounds =
                with_projector(self.out_srs, self.in_srs, |proj| bounds.reproject(proj))?;

            match projected_bounds {
                Ok(pb) => Ok(Some(RasterQueryRectangle::new(
                    self.state
                        .in_spatial_grid
                        .geo_transform()
                        .spatial_to_grid_bounds(&pb),
                    time,
                    band_idx.into(),
                ))),
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

/// Runs `f` with this thread's projector from `from` to `to`, which is only created when the
/// thread has not used the pair recently.
fn with_projector<R>(
    from: SpatialReference,
    to: SpatialReference,
    f: impl FnOnce(&DefaultCoordinateProjector) -> R,
) -> Result<R> {
    /// Enough for chained reprojections and concurrent queries with a few CRS pairs.
    const PROJECTORS_PER_THREAD: usize = 4;

    thread_local! {
        static PROJECTORS: RefCell<VecDeque<DefaultCoordinateProjector>> = const { RefCell::new(VecDeque::new()) };
    }

    PROJECTORS.with_borrow_mut(|projectors| {
        let position = projectors
            .iter()
            .position(|p| p.source_srs() == from && p.target_srs() == to);
        let projector = match position {
            Some(position) => projectors.remove(position).expect("the position exists"),
            None => DefaultCoordinateProjector::from_known_srs(from, to)?,
        };
        if projectors.len() == PROJECTORS_PER_THREAD {
            projectors.pop_back();
        }
        // the most recently used projector comes first
        projectors.push_front(projector);
        Ok(f(projectors
            .front()
            .expect("the projector was pushed above")))
    })
}

/// Marks an output pixel without a source pixel.
const NO_PIXEL: [i32; 2] = [i32::MIN, i32::MIN];

/// The global pixel index `[y, x]` of `coordinate` in `geo_transform`, or [`NO_PIXEL`].
fn input_pixel_index(geo_transform: &GeoTransform, coordinate: Coordinate2D) -> [i32; 2] {
    // a failed projection may return non-finite coordinates instead of an error
    if !(coordinate.x.is_finite() && coordinate.y.is_finite()) {
        return NO_PIXEL;
    }
    let [y, x] = geo_transform.coordinate_to_grid_idx_2d(coordinate).0;
    match (i32::try_from(y), i32::try_from(x)) {
        (Ok(y), Ok(x)) => [y, x],
        _ => NO_PIXEL,
    }
}

/// The bounds of the indices of one output row, `None` if it has none.
fn index_bounds_of_row(row: &[[i32; 2]]) -> Option<GridBoundingBox2D> {
    let [min_y, min_x, max_y, max_x] =
        row.iter()
            .filter(|index| **index != NO_PIXEL)
            .fold(None, |bounds, &[y, x]| {
                let [min_y, min_x, max_y, max_x] = bounds.unwrap_or([y, x, y, x]);
                Some([min_y.min(y), min_x.min(x), max_y.max(y), max_x.max(x)])
            })?;
    Some(GridBoundingBox2D::new_unchecked(
        [min_y as isize, min_x as isize],
        [max_y as isize, max_x as isize],
    ))
}

/// The global input pixel index `[y, x]` of each output pixel of one tile.
#[derive(Debug)]
pub struct InputPixelIndices {
    width: usize,
    /// row-major, [`NO_PIXEL`] where the output pixel has no source pixel
    indices: Vec<[i32; 2]>,
    /// the bounds of the indices of each output row, `None` if the row has none
    row_bounds: Vec<Option<GridBoundingBox2D>>,
}

impl InputPixelIndices {
    fn bounds(&self) -> Option<GridBoundingBox2D> {
        self.row_bounds
            .iter()
            .flatten()
            .copied()
            .reduce(|a, b| a.extended(&b))
    }
}

/// Everything the [`InputPixelIndices`] of one tile depend on.
#[derive(Debug, Clone, Copy)]
pub struct InputPixelIndicesSpec {
    tile_info: TileInformation,
    out_srs: SpatialReference,
    in_srs: SpatialReference,
    valid_out_area: SpatialPartition2D,
    in_geo_transform: GeoTransform,
}

/// The [`InputPixelIndices`] of the last few tiles of one query, so that further bands and time
/// steps of a tile reuse them. Concurrent sub-queries of a tile wait for the first one to compute them.
#[derive(Debug, Default)]
pub struct InputPixelIndicesCache(Mutex<VecDeque<(GridIdx2D, Arc<OnceCell<InputPixelIndices>>)>>);

impl InputPixelIndicesCache {
    /// Concurrent sub-queries are consecutive in (time, tile, band) order, so a few entries suffice.
    // shortcut: a fixed capacity misses across time steps of multi-tile queries, size it by the tiles per time slice if that shows in benchmarks
    const CAPACITY: usize = 4;

    fn get(&self, tile_position: GridIdx2D) -> Arc<OnceCell<InputPixelIndices>> {
        // the entries stay valid if another thread panicked while holding the lock
        let mut entries = self.0.lock().unwrap_or_else(PoisonError::into_inner);

        if let Some((_, cell)) = entries.iter().find(|(pos, _)| *pos == tile_position) {
            return cell.clone();
        }

        if entries.len() == Self::CAPACITY {
            entries.pop_front();
        }
        let cell = Arc::new(OnceCell::new());
        entries.push_back((tile_position, cell.clone()));
        cell
    }
}

fn input_pixel_indices_parallel(
    pool: &ThreadPool,
    spec: InputPixelIndicesSpec,
) -> Result<InputPixelIndices> {
    const MIN_ELEMENTS_IN_PAR_CHUNK: usize = 64 * 512; // this must never be smaller than 1

    let tile_info = spec.tile_info;
    let width = tile_info.tile_size_in_pixels.axis_size_x();
    let min_rows_in_par_chunk = num::integer::div_ceil(MIN_ELEMENTS_IN_PAR_CHUNK, width).max(1);

    let start = std::time::Instant::now();

    let parallelism = pool.current_num_threads();
    let par_chunk_split =
        num::integer::div_ceil(tile_info.tile_size_in_pixels.axis_size_y(), parallelism)
            .max(min_rows_in_par_chunk); // don't go below MIN_ROWS_IN_PAR_CHUNK lines per chunk.
    let par_chunk_size = width * par_chunk_split;

    let out_geo_transform = tile_info.global_geo_transform;
    let [ul_y, ul_x] = tile_info.global_upper_left_pixel_idx().0;
    let valid_out_area = spec.valid_out_area.as_bbox();

    let to_index = |c: Coordinate2D| input_pixel_index(&spec.in_geo_transform, c);

    let mut indices = vec![NO_PIXEL; tile_info.tile_size_in_pixels.number_of_elements()];

    pool.install(|| {
        indices
            .par_chunks_mut(par_chunk_size)
            .enumerate()
            .try_for_each(|(chunk_idx, index_slice)| {
                let first = chunk_idx * par_chunk_size;
                let out_coords: Vec<Coordinate2D> = (first..first + index_slice.len())
                    .map(|i| {
                        out_geo_transform.grid_idx_to_pixel_center_coordinate_2d(
                            [ul_y + (i / width) as isize, ul_x + (i % width) as isize].into(),
                        )
                    })
                    .collect();

                let Some(chunk_bounds) = BoundingBox2D::from_coord_ref_iter(out_coords.iter())
                else {
                    tracing::trace!("reprojection early exit");
                    return Ok(());
                };

                with_projector(spec.out_srs, spec.in_srs, |proj| {
                    if valid_out_area.contains_bbox(&chunk_bounds) {
                        tracing::trace!("reproject whole tile chunk");
                        match proj.project_coordinates(&out_coords) {
                            Ok(in_coords) => {
                                for (index, in_coord) in index_slice.iter_mut().zip(in_coords) {
                                    *index = to_index(in_coord);
                                }
                            }
                            // one failing coordinate must not fail the whole chunk
                            Err(_) => {
                                for (index, out_coord) in index_slice.iter_mut().zip(&out_coords) {
                                    if let Ok(in_coord) = proj.project_coordinate(*out_coord) {
                                        *index = to_index(in_coord);
                                    }
                                }
                            }
                        }
                    } else if valid_out_area.intersects_bbox(&chunk_bounds) {
                        tracing::trace!("reproject part of tile chunk");
                        for (index, out_coord) in index_slice.iter_mut().zip(&out_coords) {
                            if valid_out_area.contains_coordinate(out_coord)
                                && let Ok(in_coord) = proj.project_coordinate(*out_coord)
                            {
                                *index = to_index(in_coord);
                            }
                        }
                    } else {
                        // do nothing. Should be unreachable
                    }
                })
            })
    })?;

    let row_bounds = pool.install(|| indices.par_chunks(width).map(index_bounds_of_row).collect());

    tracing::trace!(
        "input_pixel_indices_parallel {:?}, parallelism: threads={} par_chunk_split={} par_chunk_size={} took {} (ns) ",
        &tile_info.global_tile_position,
        pool.current_num_threads(),
        par_chunk_split,
        par_chunk_size,
        start.elapsed().as_nanos()
    );

    Ok(InputPixelIndices {
        width,
        indices,
        row_bounds,
    })
}

pub fn fold_by_coordinate_lookup_future<T>(
    mut accu: TileWithProjectionCoordinates<T>,
    tile: RasterTile2D<T>,
) -> impl TryFuture<Ok = TileWithProjectionCoordinates<T>, Error = error::Error>
where
    T: Pixel,
{
    async move {
        let t_union = accu.accu_tile.time.union(&tile.time)?;
        accu.set_time(t_union);
        accu.set_cache_hint(accu.accu_tile.cache_hint.merged(&tile.cache_hint));

        let tile_bounds = tile.tile_information().global_pixel_bounds();
        accu.delivered_bounds = Some(
            accu.delivered_bounds
                .map_or(tile_bounds, |b| b.extended(&tile_bounds)),
        );

        if tile.grid_array.is_empty() {
            return Ok(accu);
        }

        let (pool, spec) = (accu.pool.clone(), accu.spec);
        accu.indices
            .get_or_try_init(|| async move {
                crate::util::spawn_blocking(move || input_pixel_indices_parallel(&pool, spec))
                    .await?
            })
            .await?;

        Ok(
            crate::util::spawn_blocking(move || fold_by_coordinate_lookup_impl(accu, &tile))
                .await?,
        )
    }
}

/// Copies the pixels of `tile` that the output pixels map to into the accumulator.
/// The indices must have been computed before.
fn fold_by_coordinate_lookup_impl<T>(
    mut accu: TileWithProjectionCoordinates<T>,
    tile: &RasterTile2D<T>,
) -> TileWithProjectionCoordinates<T>
where
    T: Pixel,
{
    let Some(input) = tile.grid_array.as_masked_grid() else {
        return accu;
    };

    debug_assert_eq!(tile.global_geo_transform, accu.spec.in_geo_transform);

    let indices = accu.indices.clone();
    let indices = indices
        .get()
        .expect("the indices are computed before the first fold with data");

    let tile_bounds = tile.tile_information().global_pixel_bounds();
    let [min_y, min_x] = tile_bounds.min_index().0;
    let [max_y, max_x] = tile_bounds.max_index().0;
    let tile_width = tile_bounds.axis_size_x();

    accu.accu_tile.grid_array.materialize();
    let out = accu
        .accu_tile
        .grid_array
        .as_masked_grid_mut()
        .expect("the grid was materialized above");

    accu.pool.install(|| {
        out.inner_grid
            .data
            .par_chunks_mut(indices.width)
            .zip(out.validity_mask.data.par_chunks_mut(indices.width))
            .zip(indices.indices.par_chunks(indices.width))
            .zip(indices.row_bounds.par_iter())
            .for_each(|(((values, validity), row_indices), row_bounds)| {
                if !row_bounds.is_some_and(|b| b.intersects(&tile_bounds)) {
                    return;
                }
                for ((value, valid), &[y, x]) in
                    values.iter_mut().zip(validity.iter_mut()).zip(row_indices)
                {
                    // `NO_PIXEL` is never inside a tile
                    let (y, x) = (y as isize, x as isize);
                    if y < min_y || y > max_y || x < min_x || x > max_x {
                        continue;
                    }
                    let i = (y - min_y) as usize * tile_width + (x - min_x) as usize;
                    // a no-data input pixel keeps what an earlier tile wrote
                    if input.validity_mask.data[i] {
                        *value = input.inner_grid.data[i];
                        *valid = true;
                    }
                }
            });
    });

    accu
}

#[derive(Debug, Clone)]
pub struct TileWithProjectionCoordinates<T> {
    accu_tile: RasterTile2D<T>,
    indices: Arc<OnceCell<InputPixelIndices>>,
    spec: InputPixelIndicesSpec,
    /// the bounds of all input tiles folded so far
    delivered_bounds: Option<GridBoundingBox2D>,
    pool: Arc<ThreadPool>,
}

#[async_trait]
impl<T: Pixel> FoldTileAccu for TileWithProjectionCoordinates<T> {
    type RasterType = T;

    async fn into_tile(self) -> Result<RasterTile2D<Self::RasterType>> {
        // the sub-query must deliver every input tile that an output pixel maps to
        debug_assert!(
            self.indices
                .get()
                .and_then(InputPixelIndices::bounds)
                .is_none_or(|b| self.delivered_bounds.is_some_and(|d| d.contains(&b))),
            "output pixels of tile {:?} map outside of the input tiles {:?}",
            self.accu_tile.tile_position,
            self.delivered_bounds
        );
        Ok(self.accu_tile)
    }

    fn thread_pool(&self) -> &Arc<ThreadPool> {
        &self.pool
    }
}

impl<T: Pixel> FoldTileAccuMut for TileWithProjectionCoordinates<T> {
    fn set_time(&mut self, time: TimeInterval) {
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
        primitives::{BandSelection, TimeStep},
        raster::{
            BoundedGrid, GeoTransform, Grid, GridBoundingBox2D, GridShape, GridShape2D,
            RasterDataType, SpatialGridDefinition, TilingSpecification, TilingStrategy,
        },
        util::test::{TestDefault, assert_eq_two_list_of_tiles_u8},
    };

    use crate::{
        adapters::RasterSubQueryAdapter,
        engine::{
            MockExecutionContext, RasterBandDescriptor, RasterBandDescriptors, RasterOperator,
            RasterResultDescriptor, SpatialGridDescriptor, SubQueryParallelism, TimeDescriptor,
            WorkflowOperatorPath,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
    };

    use super::*;

    #[allow(clippy::too_many_lines)]
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
                cache_hint: CacheHint::no_cache(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![7, 8, 9, 10]).unwrap().into(),
                properties: Default::default(),
                cache_hint: CacheHint::no_cache(),
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
                cache_hint: CacheHint::no_cache(),
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
                cache_hint: CacheHint::no_cache(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 2.), 1., -1.),
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
        .typed();

        let query_rect = RasterQueryRectangle::new(
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
            index_cache: Default::default(),
            state: TileReprojectionSubqueryGridInfo {
                in_spatial_grid: tiling_grid.tiling_spatial_grid_definition(),
                out_spatial_grid: tiling_grid.tiling_spatial_grid_definition(),
            },
            _phantom_data: PhantomData,
        };

        let time_stream = qp
            .time_query(query_rect.time_interval(), &query_ctx)
            .await
            .unwrap();

        let a = RasterSubQueryAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            state_gen,
            time_stream,
        );
        let res = a.map(Result::unwrap).collect::<Vec<_>>().await;

        assert_eq_two_list_of_tiles_u8(&data, &res, false);
    }

    #[tokio::test]
    async fn it_skips_sub_queries_outside_source_bounds() {
        let projection = SpatialReference::epsg_4326();

        let tile = |x: isize, values: Vec<u8>| RasterTile2D {
            time: TimeInterval::new_unchecked(0, 5),
            tile_position: [-1, x].into(),
            band: 0,
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([2, 2].into(), values).unwrap().into(),
            properties: Default::default(),
            cache_hint: CacheHint::no_cache(),
        };

        // the mock source returns the last tile although it is outside of its result descriptor bounds
        let data = vec![
            tile(0, vec![1, 2, 3, 4]),
            tile(1, vec![5, 6, 7, 8]),
            tile(2, vec![9, 10, 11, 12]),
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 2.), 1., -1.),
                GridShape::new_2d(2, 4).bounding_box(),
            )),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let tiling_spec = TilingSpecification::new(GridShape2D::new([2, 2]));
        let tiling_grid = result_descriptor.tiling_grid_definition(tiling_spec);
        let tiling_strat = tiling_grid.generate_data_tiling_strategy();
        let in_spatial_grid = tiling_grid.tiling_spatial_grid_definition();
        // the output grid covers all three tiles, so only the adapter's source bounds check can skip the last one
        let out_spatial_grid = SpatialGridDefinition::new(
            in_spatial_grid.geo_transform(),
            GridBoundingBox2D::new([-2, 0], [-1, 5]).unwrap(),
        );

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(tiling_spec);
        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let qp = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor,
            },
        }
        .typed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [-1, 5]).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            BandSelection::first(),
        );

        let state_gen = TileReprojectionSubQuery {
            in_srs: projection,
            out_srs: projection,
            fold_fn: fold_by_coordinate_lookup_future,
            index_cache: Default::default(),
            state: TileReprojectionSubqueryGridInfo {
                in_spatial_grid,
                out_spatial_grid,
            },
            _phantom_data: PhantomData,
        };

        let time_stream = qp
            .time_query(query_rect.time_interval(), &query_ctx)
            .await
            .unwrap();

        let res = RasterSubQueryAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            state_gen,
            time_stream,
        )
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;

        assert_eq!(res.len(), 3);
        assert_eq_two_list_of_tiles_u8(&data[..2], &res[..2], false);
        assert_eq!(res[2].tile_position, [-1, 2].into());
        assert!(res[2].is_empty());
    }

    #[test]
    fn it_shares_indices_of_recent_tiles() {
        let cache = InputPixelIndicesCache::default();
        let first = cache.get([0, 0].into());

        assert!(Arc::ptr_eq(&first, &cache.get([0, 0].into())));

        for x in 1..=InputPixelIndicesCache::CAPACITY as isize {
            cache.get([0, x].into());
        }
        assert!(!Arc::ptr_eq(&first, &cache.get([0, 0].into())));
    }

    #[tokio::test]
    async fn it_reprojects_bands_in_parallel() {
        let tile = |x: isize, band: u32, values: Vec<u8>| RasterTile2D {
            time: TimeInterval::new_unchecked(0, 5),
            tile_position: [-1, x].into(),
            band,
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([2, 2].into(), values).unwrap().into(),
            properties: Default::default(),
            cache_hint: CacheHint::no_cache(),
        };
        let data = vec![
            tile(0, 0, vec![1, 2, 3, 4]),
            tile(0, 1, vec![5, 6, 7, 8]),
            tile(1, 0, vec![9, 10, 11, 12]),
            tile(1, 1, vec![13, 14, 15, 16]),
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 2.), 1., -1.),
                GridShape::new_2d(2, 4).bounding_box(),
            )),
            bands: RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new_unitless("band_0".into()),
                RasterBandDescriptor::new_unitless("band_1".into()),
            ])
            .unwrap(),
        };

        let tiling_spec = TilingSpecification::new(GridShape2D::new([2, 2]));
        let tiling_grid = result_descriptor.tiling_grid_definition(tiling_spec);
        let tiling_strat = tiling_grid.generate_data_tiling_strategy();

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(tiling_spec);
        let mut query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());
        query_ctx.subquery_parallelism = SubQueryParallelism::new(4);

        let qp = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor,
            },
        }
        .typed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [-1, 3]).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            BandSelection::new(vec![0, 1]).unwrap(),
        );

        let state_gen = TileReprojectionSubQuery {
            in_srs: SpatialReference::epsg_4326(),
            out_srs: SpatialReference::epsg_4326(),
            fold_fn: fold_by_coordinate_lookup_future,
            index_cache: Default::default(),
            state: TileReprojectionSubqueryGridInfo {
                in_spatial_grid: tiling_grid.tiling_spatial_grid_definition(),
                out_spatial_grid: tiling_grid.tiling_spatial_grid_definition(),
            },
            _phantom_data: PhantomData,
        };

        let time_stream = qp
            .time_query(query_rect.time_interval(), &query_ctx)
            .await
            .unwrap();

        let res = RasterSubQueryAdapter::new(
            &qp,
            query_rect,
            tiling_strat,
            &query_ctx,
            state_gen,
            time_stream,
        )
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;

        assert_eq_two_list_of_tiles_u8(&data, &res, false);
    }

    #[test]
    fn it_maps_unusable_coordinates_to_no_pixel() {
        let geo_transform = GeoTransform::test_default();
        let index = |x, y| input_pixel_index(&geo_transform, Coordinate2D::new(x, y));

        assert_eq!(index(0.5, -0.5), [0, 0]);
        assert_eq!(index(f64::NAN, 0.5), NO_PIXEL);
        assert_eq!(index(0.5, f64::INFINITY), NO_PIXEL);
        assert_eq!(index(1e300, 0.5), NO_PIXEL);
    }

    #[test]
    fn it_bounds_the_indices_of_a_row() {
        assert_eq!(index_bounds_of_row(&[NO_PIXEL, NO_PIXEL]), None);
        assert_eq!(
            index_bounds_of_row(&[[3, 7], NO_PIXEL, [1, 9], [2, 4]]),
            Some(GridBoundingBox2D::new_unchecked([1, 4], [3, 9]))
        );
    }

    /// An output tile whose pixels map into an input tile that the queried pixels alone do not touch.
    #[tokio::test]
    async fn it_computes_whole_tiles_for_partial_queries() {
        let tile = |x: isize, values: Vec<u8>| RasterTile2D {
            time: TimeInterval::new_unchecked(0, 5),
            tile_position: [-1, x].into(),
            band: 0,
            global_geo_transform: TestDefault::test_default(),
            grid_array: Grid::new([2, 2].into(), values).unwrap().into(),
            properties: Default::default(),
            cache_hint: CacheHint::no_cache(),
        };
        let data = vec![tile(0, vec![1, 2, 3, 4]), tile(1, vec![5, 6, 7, 8])];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            spatial_grid: SpatialGridDescriptor::new_source(SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 2.), 1., -1.),
                GridShape::new_2d(2, 4).bounding_box(),
            )),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let tiling_spec = TilingSpecification::new(GridShape2D::new([2, 2]));
        let tiling_grid = result_descriptor.tiling_grid_definition(tiling_spec);

        // the output grid is shifted by one pixel, so output column c reads input column c + 1
        let out_geo_transform = GeoTransform::new_unchecked(Coordinate2D::new(1., 0.), 1., -1.);
        let out_spatial_grid = SpatialGridDefinition::new(
            out_geo_transform,
            GridBoundingBox2D::new([-2, 0], [-1, 2]).unwrap(),
        );
        let out_tiling_strat =
            TilingStrategy::new(tiling_spec.tile_size_in_pixels, out_geo_transform);

        let exe_ctx = MockExecutionContext::new_with_tiling_spec(tiling_spec);
        let query_ctx = exe_ctx.mock_query_context(TestDefault::test_default());

        let qp = MockRasterSource {
            params: MockRasterSourceParams {
                data,
                result_descriptor,
            },
        }
        .typed()
        .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
        .await
        .unwrap()
        .query_processor()
        .unwrap()
        .get_u8()
        .unwrap();

        // only the first column of the output tile, which reads only from the first input tile
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new([-2, 0], [-1, 0]).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            BandSelection::first(),
        );

        let state_gen = TileReprojectionSubQuery {
            in_srs: SpatialReference::epsg_4326(),
            out_srs: SpatialReference::epsg_4326(),
            fold_fn: fold_by_coordinate_lookup_future,
            index_cache: Default::default(),
            state: TileReprojectionSubqueryGridInfo {
                in_spatial_grid: tiling_grid.tiling_spatial_grid_definition(),
                out_spatial_grid,
            },
            _phantom_data: PhantomData,
        };

        let time_stream = qp
            .time_query(query_rect.time_interval(), &query_ctx)
            .await
            .unwrap();

        let res = RasterSubQueryAdapter::new(
            &qp,
            query_rect,
            out_tiling_strat,
            &query_ctx,
            state_gen,
            time_stream,
        )
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;

        let expected = RasterTile2D {
            time: TimeInterval::new_unchecked(0, 5),
            tile_position: [-1, 0].into(),
            band: 0,
            global_geo_transform: out_geo_transform,
            grid_array: Grid::new([2, 2].into(), vec![2, 5, 4, 7]).unwrap().into(),
            properties: Default::default(),
            cache_hint: CacheHint::no_cache(),
        };
        assert_eq_two_list_of_tiles_u8(&[expected], &res, false);
    }
}
