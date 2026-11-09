//! Reads of whole file blocks through the raster cache.
//!
//! Every read of a level the dataset declares is served from the decoded blocks of that level:
//! the full resolution, or the overview `factor` times coarser. Pixel `i` of that overview covers
//! the full-resolution pixels `[i * factor, (i + 1) * factor)`, so every output pixel is one
//! pixel of the level and nothing is resampled.
//!
//! The unit of reading and caching is a fixed, level-aligned group of whole blocks, at least
//! `MIN_UNIT_SIZE` on each side: small blocks are grouped, so a tile costs one request and not
//! one per block. Levels whose blocks are too small or too large to group, e.g. the strips of
//! untiled files, use square virtual units instead.
//!
//! Larger units are cached in pieces of `PIECE_SIZE`, because a hit decodes a whole entry: a tile
//! then decodes less than a whole unit per unit it touches.
//!
//! Tiles that miss the same unit share one read of it: the cache runs it once, stores its pieces
//! once and hands all of them the same decoded unit, which they copy only their pixels from. The
//! pool joins concurrent requests for the file headers that give the levels and block sizes, and
//! reuses them.
//!
//! Blocks and headers live as long as the dataset's cache hint says, and at least for the cache's
//! minimum block TTL. A dataset with a TTL of 0 still has its tiles computed anew, but they share
//! the blocks and headers of its files.

use super::level_read_advise;
use crate::cache::new_raster_cache::{BlockKey, NewRasterCacheEnum, SupportedRasterDataType};
use crate::engine::QueryContext;
use crate::source::gdal_worker_process::process_common::{
    GdalReadAdvise, GdalReadWindow, IpcProcessError,
};
use crate::source::gdal_worker_process::reader::{GdalPoolReader, flip_grid_y_if_needed};
use crate::source::gdal_worker_process::{
    GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError, GdalProcessReadResult,
    GridAndProperties, RasterInfo,
};
use futures::future::{join_all, try_join_all};
use gdal::raster::GdalType;
use geoengine_datatypes::primitives::{CacheHint, CacheTtlSeconds, TimeInterval};
use geoengine_datatypes::raster::{
    ChangeGridBounds, EmptyGrid, GeoTransform, GridBlit, GridBoundingBox2D, GridBounds, GridIdx2D,
    GridOrEmpty, GridShapeAccess, Pixel, RasterTile2D,
};
use num::FromPrimitive;
use std::sync::Arc;

/// Blocks with fewer pixels, e.g. the one-row strips of untiled files, are not grouped into units.
const MIN_BLOCK_PIXELS: usize = 64 * 64;
/// Blocks with more pixels, e.g. an untiled file in one strip, are too large to read whole for a
/// small window.
const MAX_BLOCK_PIXELS: usize = 2048 * 2048;
/// The least side of a unit, the size of a default tile: a tile then touches one to four units.
const MIN_UNIT_SIZE: usize = 512;
/// The side of the cached pieces of a unit that divides into them.
const PIECE_SIZE: usize = 512;
/// The side of the virtual units of levels with blocks outside the range above.
const VIRTUAL_UNIT_SIZE: usize = 512;

/// Reads blocks through the cache of a query context.
#[derive(Clone)]
pub struct BlockIo {
    cache: Arc<NewRasterCacheEnum>,
    reader: GdalPoolReader,
    min_ttl: CacheTtlSeconds,
}

/// The part of one level of a file that a read covers at native size.
#[derive(Clone, Copy)]
struct LevelWindow {
    /// the level is this many times coarser than the full resolution
    factor: usize,
    /// in the level's pixel space
    window: GridBoundingBox2D,
    /// the level's size, y then x
    level_size: [usize; 2],
    /// the size of a unit, y then x
    block_size: [usize; 2],
    /// the size of a cached piece of a unit, y then x
    piece_size: [usize; 2],
}

/// A decoded block, bounded in the pixel space of its level.
type Block<T> = GridAndProperties<T, GridBoundingBox2D>;

/// The read of a unit: `None` if the file is missing and counts as no data.
pub type UnitResult<T> = Result<Option<Arc<Block<T>>>, GdalProcessPoolError>;

/// The read of a unit of any pixel type, so that the reads of all of them share one
/// [`crate::cache::single_read::SingleRead`].
#[derive(Clone)]
pub enum UnitRead {
    I8(UnitResult<i8>),
    I16(UnitResult<i16>),
    I32(UnitResult<i32>),
    I64(UnitResult<i64>),
    U8(UnitResult<u8>),
    U16(UnitResult<u16>),
    U32(UnitResult<u32>),
    U64(UnitResult<u64>),
    F32(UnitResult<f32>),
    F64(UnitResult<f64>),
}

impl BlockIo {
    /// `min_ttl` is the least time blocks and headers live.
    pub fn new(
        cache: Arc<NewRasterCacheEnum>,
        gdal_worker: GdalPoolDispatcher,
        min_ttl: CacheTtlSeconds,
    ) -> Self {
        Self {
            cache,
            reader: GdalPoolReader::from(gdal_worker),
            min_ttl,
        }
    }

    /// Block reads need the raster cache; without it, sources read windows.
    pub fn from_query_context(ctx: &dyn QueryContext) -> Option<Self> {
        ctx.new_raster_cache().map(|cache| {
            let min_ttl = cache.min_block_ttl();
            Self::new(cache, ctx.get_gdal_worker(), min_ttl)
        })
    }

    /// Whether blocks read for `cache_hint` stay in the cache. If they do not, each tile reads
    /// the header and its whole unit again, which costs more than reading its window.
    pub fn caches(&self, cache_hint: CacheHint) -> bool {
        block_cache_hint(cache_hint, self.min_ttl).may_cache()
    }

    /// Reads `read_advise` of the file from whole blocks of the level `factor` times coarser
    /// than the full resolution.
    ///
    /// # Errors
    /// Returns an error if a header or block read fails, if the file lacks the level or if the
    /// read does not start at a pixel of the level.
    ///
    /// # Panics
    /// Never: the level window has the size of the read by construction.
    pub async fn read<T>(
        &self,
        params: &GdalDatasetParameters,
        read_advise: GdalReadAdvise,
        factor: usize,
        cache_hint: CacheHint,
    ) -> Result<GdalProcessReadResult<T>, GdalProcessPoolError>
    where
        T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType,
    {
        let file = file_id(params);
        let info = match self.reader.remembered_raster_info(&file) {
            Some(info) => Some(info),
            None => {
                let info = self
                    .reader
                    .read_raster_info_or_no_data(params.clone())
                    .await?;
                if let Some(info) = &info {
                    let is_new = self.reader.remember_raster_info(
                        file.clone(),
                        info.clone(),
                        block_cache_hint(cache_hint, self.min_ttl),
                    );
                    let ttl = cache_hint.total_ttl_seconds();
                    if is_new && ttl < self.min_ttl {
                        tracing::debug!(
                            "{} has a cache TTL of {} s, its blocks live for the minimum of {} s",
                            params.file_path.display(),
                            ttl.seconds(),
                            self.min_ttl.seconds(),
                        );
                    }
                }
                info
            }
        };
        let Some(info) = info else {
            return Ok(GdalProcessReadResult::FileNotFoundAsNoData);
        };
        let Some(level) = info.level_of_factor(factor) else {
            return Err(IpcProcessError::MissingOverviewLevel {
                file: params.file_path.display().to_string(),
                factor,
            }
            .into());
        };
        let Some(level_window) = level_window(&info, level, factor, read_advise) else {
            return Err(IpcProcessError::IpcOther {
                msg: format!(
                    "read {:?} of {} is not inside its level {factor}",
                    read_advise.gdal_read_widow,
                    params.file_path.display()
                ),
            }
            .into());
        };

        let blocks = try_join_all(
            level_window
                .pieces_in(level_window.window)
                .map(|piece| self.piece::<T>(params, &file, &level_window, piece, cache_hint)),
        )
        .await?;
        let Some(pieces) = blocks.into_iter().collect::<Option<Vec<_>>>() else {
            return Ok(GdalProcessReadResult::FileNotFoundAsNoData);
        };
        // pieces of a unit that was just read are that unit, which is composed only once
        let mut blocks: Vec<Arc<Block<T>>> = Vec::with_capacity(pieces.len());
        for piece in pieces {
            if !blocks.iter().any(|block| Arc::ptr_eq(block, &piece)) {
                blocks.push(piece);
            }
        }

        // the blocks are in the file's row order, south-up files are flipped afterwards
        // off the task that polls all tiles of the query, so that tiles compose in parallel
        let window = level_window.window;
        let (grid, properties) = crate::util::spawn_blocking(move || {
            let grid = compose(window, &blocks);
            let properties = blocks.first().map(|block| block.properties.clone());
            let grid = flip_grid_y_if_needed(grid, read_advise.flip_y)
                .set_grid_bounds(read_advise.read_window_bounds)
                .expect("the level window has the size of the read");
            (grid, properties)
        })
        .await
        .map_err(|source| IpcProcessError::IpcOther {
            msg: format!("could not compose blocks: {source}"),
        })?;

        Ok(GdalProcessReadResult::Grid(Box::new(GridAndProperties {
            grid,
            properties: properties.unwrap_or_default(),
        })))
    }

    /// One piece from the cache, or else the whole unit it is in. `None` if the file is missing
    /// and counts as no data.
    async fn piece<T>(
        &self,
        params: &GdalDatasetParameters,
        file: &Arc<str>,
        level_window: &LevelWindow,
        piece: [usize; 2],
        cache_hint: CacheHint,
    ) -> UnitResult<T>
    where
        T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType,
    {
        match self
            .cache
            .get_block::<T>(block_key::<T>(file, level_window, piece))
            .await
        {
            Ok(Some(tile)) => {
                if let Ok(grid) = tile
                    .grid_array
                    .set_grid_bounds(level_window.piece_bounds(piece))
                {
                    return Ok(Some(Arc::new(Block {
                        grid,
                        properties: tile.properties,
                    })));
                }
            }
            Ok(None) => {}
            Err(err) => tracing::warn!("Failed to load a cached file block: {err}"),
        }

        let this = self.clone();
        let (params, file_id, level_window) = (params.clone(), file.clone(), *level_window);
        let unit_key = block_key::<T>(file, &level_window, level_window.unit_of(piece));
        let unit = self
            .cache
            .block_reads()
            .read(unit_key, async move {
                T::map_unit_to_enum(
                    this.read_unit::<T>(params, file_id, level_window, piece, cache_hint)
                        .await,
                )
            })
            .await
            .map_err(|source| IpcProcessError::IpcOther {
                msg: source.to_string(),
            })?;
        T::map_enum_to_unit(unit).map_err(|source| IpcProcessError::IpcOther {
            msg: source.to_string(),
        })?
    }

    /// Reads the unit of `piece` and caches all its pieces.
    async fn read_unit<T>(
        self,
        params: GdalDatasetParameters,
        file: Arc<str>,
        level_window: LevelWindow,
        piece: [usize; 2],
        cache_hint: CacheHint,
    ) -> UnitResult<T>
    where
        T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType,
    {
        let unit_bounds = level_window.unit_bounds(piece);
        let read_advise = GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new(unit_bounds.min_index(), unit_bounds.grid_shape()),
            read_window_bounds: unit_bounds,
            bounds_of_target: unit_bounds,
            flip_y: false,
        };
        let data = match self
            .reader
            .read_level_data::<T>(params, level_window.factor, read_advise)
            .await?
        {
            GdalProcessReadResult::Grid(data) => *data,
            GdalProcessReadResult::FileNotFoundAsNoData => return Ok(None),
        };
        let unit = Arc::new(data);

        let units = std::slice::from_ref(&unit);
        let inserts = level_window.pieces_in(unit_bounds).map(|other| {
            let tile = RasterTile2D::new_with_properties(
                TimeInterval::default(),
                GridIdx2D::new([0, 0]),
                0,
                GeoTransform::new_unchecked((0., 0.).into(), 1., -1.),
                compose(level_window.piece_bounds(other), units).unbounded(),
                unit.properties.clone(),
                block_cache_hint(cache_hint, self.min_ttl),
            );
            self.cache
                .insert_block(block_key::<T>(&file, &level_window, other), tile)
        });
        for inserted in join_all(inserts).await {
            if let Err(err) = inserted {
                tracing::warn!("Failed to cache a file block: {err}");
            }
        }

        Ok(Some(unit))
    }
}

impl LevelWindow {
    /// The pieces that intersect `bounds`.
    fn pieces_in(&self, bounds: GridBoundingBox2D) -> impl Iterator<Item = [usize; 2]> + use<> {
        let [piece_y, piece_x] = self.piece_size;
        let ys = (bounds.y_min() as usize / piece_y)..=(bounds.y_max() as usize / piece_y);
        let xs = (bounds.x_min() as usize / piece_x)..=(bounds.x_max() as usize / piece_x);
        ys.flat_map(move |y| xs.clone().map(move |x| [y, x]))
    }

    /// The pixels of a piece, clipped to the level.
    fn piece_bounds(&self, [y, x]: [usize; 2]) -> GridBoundingBox2D {
        self.clipped_cell(self.piece_size, [y, x])
    }

    /// The index of the unit of a piece.
    fn unit_of(&self, [y, x]: [usize; 2]) -> [usize; 2] {
        let [piece_y, piece_x] = self.piece_size;
        let [unit_y, unit_x] = self.block_size;
        [y * piece_y / unit_y, x * piece_x / unit_x]
    }

    /// The pixels of the unit of a piece, clipped to the level.
    fn unit_bounds(&self, piece: [usize; 2]) -> GridBoundingBox2D {
        self.clipped_cell(self.block_size, self.unit_of(piece))
    }

    fn clipped_cell(&self, [cell_y, cell_x]: [usize; 2], [y, x]: [usize; 2]) -> GridBoundingBox2D {
        let [size_y, size_x] = self.level_size;
        GridBoundingBox2D::new_min_max(
            (y * cell_y) as isize,
            (((y + 1) * cell_y).min(size_y) - 1) as isize,
            (x * cell_x) as isize,
            (((x + 1) * cell_x).min(size_x) - 1) as isize,
        )
        .expect("a cell index of the window lies in the level")
    }
}

/// The pixels of `blocks` in `window`. As for window reads, a window without valid pixels is
/// empty, also if its blocks have valid pixels elsewhere.
fn compose<T: Pixel>(
    window: GridBoundingBox2D,
    blocks: &[Arc<Block<T>>],
) -> GridOrEmpty<GridBoundingBox2D, T> {
    let mut grid = GridOrEmpty::from(EmptyGrid::new(window));
    for block in blocks {
        grid.grid_blit_from(&block.grid);
    }
    if grid
        .as_masked_grid()
        .is_some_and(|grid| !grid.validity_mask.data.contains(&true))
    {
        return EmptyGrid::new(window).into();
    }
    grid
}

/// Identifies a file and everything that changes how its pixels and properties are read.
fn file_id(params: &GdalDatasetParameters) -> Arc<str> {
    serde_json::to_string(params)
        .expect("dataset parameters serialize to JSON")
        .into()
}

fn block_key<T: Pixel>(file: &Arc<str>, level_window: &LevelWindow, block: [usize; 2]) -> BlockKey {
    BlockKey {
        file: file.clone(),
        factor: level_window.factor,
        block,
        data_type: T::TYPE,
    }
}

fn block_cache_hint(cache_hint: CacheHint, min_ttl: CacheTtlSeconds) -> CacheHint {
    if cache_hint.expires().seconds_to_expiration() < min_ttl.seconds() {
        CacheHint::seconds(min_ttl.seconds())
    } else {
        cache_hint
    }
}

/// The window of the read in `level` of the file, with the blocks it is cached in. `None` if the
/// read does not start at a pixel of the level or reaches beyond it.
fn level_window(
    info: &RasterInfo,
    level: usize,
    factor: usize,
    read_advise: GdalReadAdvise,
) -> Option<LevelWindow> {
    let level_layout = info.levels.get(level)?;
    let window = level_read_advise(read_advise, factor)?.gdal_read_widow;
    let (x_min, y_min) = window.gdal_window_start();
    let (size_x, size_y) = window.gdal_window_size();
    let (x_min, y_min) = (usize::try_from(x_min).ok()?, usize::try_from(y_min).ok()?);
    if size_x == 0 || size_y == 0 {
        return None;
    }
    let (x_max, y_max) = (x_min + size_x - 1, y_min + size_y - 1);
    if x_max >= level_layout.size_x || y_max >= level_layout.size_y {
        return None;
    }

    let block_pixels = level_layout.block_size_x * level_layout.block_size_y;
    let block_size = if (MIN_BLOCK_PIXELS..=MAX_BLOCK_PIXELS).contains(&block_pixels) {
        // whole blocks, so that the worker decodes each block once per unit
        [level_layout.block_size_y, level_layout.block_size_x]
            .map(|size| size * MIN_UNIT_SIZE.div_ceil(size))
    } else {
        [VIRTUAL_UNIT_SIZE, VIRTUAL_UNIT_SIZE]
    };

    Some(LevelWindow {
        factor,
        window: GridBoundingBox2D::new_min_max(
            y_min as isize,
            y_max as isize,
            x_min as isize,
            x_max as isize,
        )
        .ok()?,
        level_size: [level_layout.size_y, level_layout.size_x],
        block_size,
        piece_size: block_size.map(|size| {
            if size.is_multiple_of(PIECE_SIZE) {
                PIECE_SIZE
            } else {
                size
            }
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::gdal_worker_process::RasterLevelLayout;
    use geoengine_datatypes::raster::RasterProperties;
    use geoengine_datatypes::raster::{Grid, GridIndexAccess, MaskedGrid};

    fn advise(start: [isize; 2], size: [usize; 2], out: [usize; 2]) -> GdalReadAdvise {
        let bounds =
            GridBoundingBox2D::new_min_max(0, out[0] as isize - 1, 0, out[1] as isize - 1).unwrap();
        GdalReadAdvise {
            gdal_read_widow: GdalReadWindow::new(start.into(), size.into()),
            read_window_bounds: bounds,
            bounds_of_target: bounds,
            flip_y: false,
        }
    }

    fn raster_info(levels: Vec<RasterLevelLayout>) -> RasterInfo {
        RasterInfo {
            data_type: geoengine_datatypes::raster::RasterDataType::U16,
            spatial_reference:
                geoengine_datatypes::spatial_reference::SpatialReferenceOption::Unreferenced,
            levels,
        }
    }

    fn level(size: usize, block: usize) -> RasterLevelLayout {
        RasterLevelLayout {
            size_x: size,
            size_y: size,
            block_size_x: block,
            block_size_y: block,
        }
    }

    #[test]
    fn it_keeps_blocks_as_long_as_the_dataset_says_and_at_least_the_minimum() {
        let min_ttl = CacheTtlSeconds::new(60);
        let seconds = |hint: CacheHint| hint.expires().seconds_to_expiration();

        assert!(seconds(block_cache_hint(CacheHint::no_cache(), min_ttl)) >= 59);
        assert!(seconds(block_cache_hint(CacheHint::seconds(10), min_ttl)) >= 59);
        assert!(seconds(block_cache_hint(CacheHint::seconds(1000), min_ttl)) >= 999);
    }

    #[test]
    fn it_composes_a_window_without_valid_pixels_as_empty() {
        let bounds = GridBoundingBox2D::new([0, 0], [3, 3]).unwrap();
        let mut validity = vec![false; 16];
        validity[0] = true;
        let block = Block {
            grid: GridOrEmpty::from(
                MaskedGrid::new(
                    Grid::new(bounds, vec![7_u8; 16]).unwrap(),
                    Grid::new(bounds, validity).unwrap(),
                )
                .unwrap(),
            ),
            properties: RasterProperties::default(),
        };
        let blocks = [Arc::new(block)];

        let with_valid_pixel = compose(GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(), &blocks);
        assert_eq!(with_valid_pixel.get_at_grid_index([0, 0]).unwrap(), Some(7));
        assert_eq!(with_valid_pixel.get_at_grid_index([1, 1]).unwrap(), None);
        assert!(compose(GridBoundingBox2D::new([2, 2], [3, 3]).unwrap(), &blocks).is_empty());
    }

    #[test]
    fn it_finds_levels_by_factor_with_rounded_up_sizes() {
        let layout = raster_info(vec![
            level(10_980, 1024),
            level(5490, 512),
            level(2745, 512),
            level(1373, 512),
        ]);
        assert_eq!(layout.level_of_factor(1), Some(0));
        assert_eq!(layout.level_of_factor(2), Some(1));
        assert_eq!(layout.level_of_factor(4), Some(2));
        assert_eq!(
            layout.level_of_factor(8),
            Some(3),
            "ceil(10 980 / 8) = 1 373"
        );
        assert_eq!(layout.level_of_factor(16), None);
    }

    #[test]
    fn it_maps_a_read_onto_the_pixels_and_units_of_its_level() {
        let layout = raster_info(vec![level(4096, 512), level(2048, 256), level(1024, 128)]);

        let window = level_window(&layout, 1, 2, advise([1024, 512], [1024, 1024], [512, 512]))
            .expect("inside overview 2");
        // the read starts at y 1024, x 512 of the full resolution
        assert_eq!(
            window.window,
            GridBoundingBox2D::new_min_max(512, 1023, 256, 767).unwrap()
        );
        // the 256 px blocks of overview 2 are read in units of 2 x 2
        assert_eq!(window.block_size, [512, 512]);
        assert_eq!(
            window.pieces_in(window.window).collect::<Vec<_>>(),
            vec![[1, 0], [1, 1]]
        );
    }

    #[test]
    fn it_groups_small_blocks_into_units_of_whole_blocks() {
        let unit = |block_x, block_y| {
            let layout = raster_info(vec![RasterLevelLayout {
                size_x: 4096,
                size_y: 4096,
                block_size_x: block_x,
                block_size_y: block_y,
            }]);
            level_window(&layout, 0, 1, advise([0, 0], [512, 512], [512, 512]))
                .expect("full resolution")
                .block_size
        };
        assert_eq!(unit(256, 256), [512, 512]);
        assert_eq!(unit(128, 128), [512, 512]);
        assert_eq!(unit(300, 300), [600, 600], "whole blocks, not a cut one");
        assert_eq!(unit(512, 512), [512, 512]);
        assert_eq!(
            unit(1024, 1024),
            [1024, 1024],
            "large blocks are units of their own"
        );
        assert_eq!(unit(2048, 64), [512, 2048], "rows of a wide block");
    }

    #[test]
    fn it_caches_large_units_in_pieces_and_reads_them_whole() {
        let layout = raster_info(vec![level(2600, 1024)]);
        let window = level_window(&layout, 0, 1, advise([500, 1000], [512, 512], [512, 512]))
            .expect("full resolution");
        assert_eq!(window.block_size, [1024, 1024]);
        assert_eq!(window.piece_size, [PIECE_SIZE, PIECE_SIZE]);
        assert_eq!(
            window.pieces_in(window.window).collect::<Vec<_>>(),
            vec![[0, 1], [0, 2], [1, 1], [1, 2]]
        );
        assert_eq!(
            window.piece_bounds([1, 3]),
            GridBoundingBox2D::new_min_max(512, 1023, 1536, 2047).unwrap()
        );
        assert_eq!(
            window.unit_bounds([1, 3]),
            GridBoundingBox2D::new_min_max(0, 1023, 1024, 2047).unwrap()
        );
        // the last unit and piece are clipped to the level
        assert_eq!(
            window.unit_bounds([5, 5]),
            GridBoundingBox2D::new_min_max(2048, 2599, 2048, 2599).unwrap()
        );
        assert_eq!(
            window.piece_bounds([5, 5]),
            GridBoundingBox2D::new_min_max(2560, 2599, 2560, 2599).unwrap()
        );
    }

    #[test]
    fn it_reads_the_partial_last_pixel_of_an_overview() {
        // 1001 px: overview 2 has 501 px, the last one covers only full-resolution pixel 1000
        let layout = raster_info(vec![level(1001, 256), level(501, 256)]);
        let window = level_window(&layout, 1, 2, advise([768, 0], [233, 233], [117, 117]))
            .expect("the last overview pixel is inside the level");
        assert_eq!(
            window.window,
            GridBoundingBox2D::new_min_max(384, 500, 0, 116).unwrap()
        );
    }

    #[test]
    fn it_rejects_misaligned_reads_and_reads_beyond_the_level() {
        let layout = raster_info(vec![level(1024, 256), level(512, 256)]);
        assert!(
            level_window(&layout, 1, 2, advise([1, 0], [256, 256], [128, 128])).is_none(),
            "starts inside an overview pixel"
        );
        assert!(
            level_window(&layout, 1, 2, advise([0, 0], [1024, 1024], [513, 513])).is_none(),
            "ends beyond the overview"
        );
    }

    #[test]
    fn it_clips_edge_blocks_to_the_level() {
        let layout = raster_info(vec![level(1000, 256)]);
        let window = level_window(&layout, 0, 1, advise([768, 768], [232, 232], [232, 232]))
            .expect("full resolution");
        assert_eq!(
            window.pieces_in(window.window).collect::<Vec<_>>(),
            vec![[1, 1]]
        );
        assert_eq!(
            window.piece_bounds([1, 1]),
            GridBoundingBox2D::new_min_max(512, 999, 512, 999).unwrap()
        );
    }

    #[test]
    fn it_caches_strips_and_huge_blocks_in_virtual_blocks() {
        let strips = raster_info(vec![RasterLevelLayout {
            size_x: 3600,
            size_y: 1800,
            block_size_x: 3600,
            block_size_y: 1,
        }]);
        let window = level_window(&strips, 0, 1, advise([600, 300], [512, 512], [512, 512]))
            .expect("full resolution");
        assert_eq!(window.block_size, [VIRTUAL_UNIT_SIZE, VIRTUAL_UNIT_SIZE]);
        assert_eq!(
            window.pieces_in(window.window).collect::<Vec<_>>(),
            vec![[1, 0], [1, 1], [2, 0], [2, 1]]
        );

        let one_block = raster_info(vec![level(4096, 4096)]);
        let window = level_window(&one_block, 0, 1, advise([0, 0], [512, 512], [512, 512]))
            .expect("full resolution");
        assert_eq!(window.block_size, [VIRTUAL_UNIT_SIZE, VIRTUAL_UNIT_SIZE]);
    }
}
