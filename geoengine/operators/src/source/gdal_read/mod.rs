//! The read path shared by the GDAL source operators: a tile plan names the files of one
//! output tile, `assemble_tile` reads them through the process pool and composes the tile,
//! and `tile_stream` drives the plans of a query in order with bounded read-ahead.

mod blocks;

pub use blocks::{BlockIo, MdSlice};
pub use blocks::{UnitRead, UnitResult};

use crate::cache::new_raster_cache::SupportedRasterDataType;
use crate::engine::{RasterOperator, SingleRasterSource, TypedRasterOperator};
use crate::error::Error;
use crate::processing::{
    Downsampling, DownsamplingMethod, DownsamplingParams, DownsamplingResolution,
};
use crate::source::gdal_worker_process::process_common::{
    GdalReadAdvise, GdalReadWindow, IpcProcessError,
};
use crate::source::gdal_worker_process::reader::GdalPoolReader;
use crate::source::gdal_worker_process::{
    GdalDatasetParameters, GdalPoolDispatcher, GdalProcessPoolError, GdalProcessReadResult,
    GdalReaderMode, GridAndProperties,
};
use crate::util::Result;
use futures::stream::{BoxStream, FuturesOrdered};
use futures::{Stream, StreamExt};
use gdal::raster::GdalType;
use geoengine_datatypes::dataset::NamedData;
use geoengine_datatypes::primitives::{
    CacheHint, SpatialResolution, TimeInterval, find_next_best_overview_level,
};
use geoengine_datatypes::raster::{
    BoundedGrid, ChangeGridBounds, EmptyGrid, GridBlit, GridBoundingBox2D, GridIdx2D,
    GridIntersection, GridOrEmpty, GridShapeAccess, GridSize, Pixel, RasterProperties,
    RasterTile2D, SpatialGridDefinition, TileInformation,
};
use num::FromPrimitive;
use std::num::{NonZeroU32, NonZeroUsize};
use tracing::trace;

/// Everything needed to produce one output tile.
#[derive(Debug, Clone)]
pub struct TilePlan {
    pub tile: TileInformation,
    pub time: TimeInterval,
    pub band: u32,
    /// the files covering the tile, highest z-index first
    pub files: Vec<GdalDatasetParameters>,
    pub cache_hint: CacheHint,
}

/// How the files of a tile are read. The path is chosen once per tile and holds for all its
/// files: a file never switches to the other path, so the pixels of a tile follow one rule.
///
/// Both paths read alike: at the full resolution the pixels as they are, at a coarser level each
/// output pixel from the nearest pixel of the coarsest level of the file that is fine enough,
/// placed by that level's real size. So a level the file has and whose size fits is read as is.
#[derive(Clone, Copy)]
enum ReadPath<'a> {
    /// Through whole cached blocks of the files, see [`BlockIo::read`]: the server picks the
    /// level and its pixels.
    Blocks(&'a BlockIo),
    /// Through GDAL's window read in the workers (`read_tile_data`): GDAL picks the level and
    /// its pixels.
    Gdal,
}

impl<'a> ReadPath<'a> {
    /// Blocks, if there is a block reader, the blocks read for the tile stay cached, and the
    /// reader reads every file of the tile: with `block_reads = "auto"` all are remote, and none
    /// is coarser than the dataset. Such a file has no level of the dataset, GDAL resamples it.
    fn of(blocks: Option<&'a BlockIo>, plan: &TilePlan, reader_mode: &GdalReaderMode) -> Self {
        match blocks {
            Some(blocks)
                if blocks.caches(plan.cache_hint)
                    && plan.files.iter().all(|file| {
                        blocks.reads(file)
                            && reader_mode
                                .coarser_file_factor(&file.spatial_grid_definition())
                                .is_none()
                    }) =>
            {
                ReadPath::Blocks(blocks)
            }
            _ => ReadPath::Gdal,
        }
    }
}

/// Reads the files of `plan` and composes its tile, all of them on one [`ReadPath`]: through
/// `blocks` if given and usable for the tile, else through GDAL.
///
/// The highest z-index wins at each valid pixel. A file is read only where the files above it
/// left pixels invalid. Files whose read area sticks out of the areas of the files above are read
/// at the same time, since those can never cover them; a file inside those areas waits for the
/// files above it and is only read if they left a pixel of its area invalid. So the same files
/// are read as one after another, and merging in the order of the files gives the same pixels.
/// The properties come from the first file that was read. A tile no file covers is empty.
///
/// A reduced resolution level of a JPEG 2000 file blends the edges of its data with the fill
/// value, so its pixels next to the fill value are uncertain (see [`reads_low_pass_level`]): the
/// valid pixels of the files below replace them, and only where none does, they keep their
/// value. Such a file is read with a margin of one pixel around the tile, so that this also
/// holds for the pixels at the edge of the tile.
///
/// The traces of the reads are in a span per tile, so that the reads of one tile, which run at
/// the same time, can be told apart from those of other tiles.
///
/// # Errors
/// Returns an error if a pool read fails. A missing file counts as no data if its parameters
/// say so.
#[tracing::instrument(
    name = "gdal_assemble_tile",
    level = "trace",
    skip_all,
    fields(tile = ?plan.tile.global_tile_position, band = plan.band, time = ?plan.time)
)]
pub async fn assemble_tile<T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType>(
    plan: TilePlan,
    reader_mode: GdalReaderMode,
    gdal_worker: &GdalPoolDispatcher,
    blocks: Option<&BlockIo>,
) -> Result<RasterTile2D<T>, GdalProcessPoolError> {
    let tile_bounds = plan.tile.global_pixel_bounds();
    let mut raster: GridOrEmpty<GridBoundingBox2D, T> =
        GridOrEmpty::from(EmptyGrid::new(tile_bounds));
    // the uncertain pixels of the files, of the highest file where several have one
    let mut uncertain: GridOrEmpty<GridBoundingBox2D, T> =
        GridOrEmpty::from(EmptyGrid::new(tile_bounds));
    let mut properties = None;
    let reader = GdalPoolReader::from(gdal_worker.clone());
    let factor = reader_mode.level_factor();
    let path = ReadPath::of(blocks, &plan, &reader_mode);

    let files = FileRead::of_tile(plan.files, &plan.tile, &reader_mode);
    let areas: Vec<GridBoundingBox2D> = files.iter().map(|file| file.area).collect();

    // reads in flight, merged in the order of the files (highest z-index first) as they finish
    let mut reads = FuturesOrdered::new();
    for (index, file) in files.into_iter().enumerate() {
        if is_covered(file.area, &areas[..index]) {
            // the files above may fill this one's area: merge them first, then decide
            while let Some(read) = reads.next().await {
                merge(
                    &mut raster,
                    &mut uncertain,
                    &mut properties,
                    tile_bounds,
                    read?,
                );
            }
            if raster
                .as_masked_grid()
                .is_some_and(|grid| grid.all_valid_in_bbox(&file.area))
            {
                trace!(
                    file = %file.params.file_path.display(),
                    "skipping file whose read area is covered by valid pixels of files above it"
                );
                continue;
            }
        }
        reads.push_back(read_file::<T>(file, path, &reader, factor, plan.cache_hint));
    }
    while let Some(read) = reads.next().await {
        merge(
            &mut raster,
            &mut uncertain,
            &mut properties,
            tile_bounds,
            read?,
        );
    }
    // uncertain pixels that no file below replaced keep their value
    raster.grid_blit_fill_no_data(&uncertain);

    Ok(RasterTile2D::new_with_properties(
        plan.time,
        plan.tile.global_tile_position,
        plan.band,
        plan.tile.global_geo_transform,
        raster.unbounded(),
        properties.unwrap_or_default(),
        plan.cache_hint,
    ))
}

/// Merges one file's read into the tile below the pixels of the files merged before it, and its
/// uncertain pixels below those of the files before it.
fn merge<T: Pixel>(
    raster: &mut GridOrEmpty<GridBoundingBox2D, T>,
    uncertain: &mut GridOrEmpty<GridBoundingBox2D, T>,
    properties: &mut Option<RasterProperties>,
    tile_bounds: GridBoundingBox2D,
    read: FileData<T>,
) {
    if let Some(file_uncertain) = &read.uncertain {
        uncertain.grid_blit_fill_no_data(file_uncertain);
    }
    let file_data = match read.read {
        GdalProcessReadResult::Grid(grid_and_properties) => *grid_and_properties,
        GdalProcessReadResult::FileNotFoundAsNoData => return,
    };

    if raster.is_empty() && file_data.grid.bounding_box() == tile_bounds {
        // nothing to merge with: take the grid instead of copying it
        *raster = file_data.grid;
    } else if raster.is_empty() {
        // nothing to merge with either: copy whole rows, the merge below checks each pixel
        raster.grid_blit_from(&file_data.grid);
    } else {
        raster.grid_blit_fill_no_data(&file_data.grid);
    }
    properties.get_or_insert(file_data.properties);
}

/// The read of a file of a tile.
struct FileData<T: Pixel> {
    /// the pixels the file gives for sure
    read: GdalProcessReadResult<T>,
    /// the valid pixels next to the fill value of a reduced JPEG 2000 level, which the files
    /// below replace, see [`split_next_to_no_data`]
    uncertain: Option<GridOrEmpty<GridBoundingBox2D, T>>,
}

/// A file of a tile and how it is read.
struct FileRead {
    params: GdalDatasetParameters,
    read_advise: GdalReadAdvise,
    /// the pixels of the tile the read gives, the read advise's bounds cut to the tile
    area: GridBoundingBox2D,
    /// whether the read has a margin of one pixel around the tile, and its pixels next to no data
    /// are uncertain, see [`reads_low_pass_level`]
    erodes: bool,
}

impl FileRead {
    /// The reads of the files of `tile` that intersect it, in their order.
    fn of_tile(
        files: Vec<GdalDatasetParameters>,
        tile: &TileInformation,
        reader_mode: &GdalReaderMode,
    ) -> Vec<Self> {
        let tile_bounds = tile.global_pixel_bounds();
        let tile_spatial_grid = tile.spatial_grid_definition();
        // the tile and a margin of one pixel, for the files whose pixels next to no data are invalid
        let margin_grid = SpatialGridDefinition::new(
            tile.global_geo_transform,
            GridBoundingBox2D::new_unchecked(
                [tile_bounds.y_min() - 1, tile_bounds.x_min() - 1],
                [tile_bounds.y_max() + 1, tile_bounds.x_max() + 1],
            ),
        );

        files
            .into_iter()
            .filter_map(|params| {
                let file_grid = params.spatial_grid_definition();
                let erodes = reads_low_pass_level(&params, reader_mode);
                let read_advise = if erodes {
                    reader_mode.tiling_to_dataset_read_advise(&file_grid, &margin_grid)
                } else {
                    reader_mode.tiling_to_dataset_read_advise(&file_grid, &tile_spatial_grid)
                };
                // the part of the read inside the tile, which the read is cut to
                let area = read_advise.and_then(|read_advise| {
                    read_advise.read_window_bounds.intersection(&tile_bounds)
                });
                let Some((read_advise, area)) = read_advise.zip(area) else {
                    trace!(
                        file = %params.file_path.display(),
                        "file does not intersect tile {:?}",
                        tile.global_tile_position,
                    );
                    return None;
                };
                Some(Self {
                    params,
                    read_advise,
                    area,
                    erodes,
                })
            })
            .collect()
    }
}

/// Reads `file` on `path`. If its pixels next to no data are uncertain, they are split off off the
/// task that polls all tiles of the query, as the composition of blocks does.
async fn read_file<T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType>(
    file: FileRead,
    path: ReadPath<'_>,
    reader: &GdalPoolReader,
    factor: NonZeroUsize,
    cache_hint: CacheHint,
) -> Result<FileData<T>, GdalProcessPoolError> {
    let FileRead {
        params,
        read_advise,
        area,
        erodes,
    } = file;
    let read = match path {
        ReadPath::Blocks(blocks) => {
            blocks
                .read::<T>(&params, read_advise, factor, cache_hint)
                .await?
        }
        ReadPath::Gdal => reader.read_tile_data::<T>(params, read_advise).await?,
    };
    let read = match read {
        GdalProcessReadResult::Grid(read) if erodes => read,
        read => {
            return Ok(FileData {
                read,
                uncertain: None,
            });
        }
    };
    let GridAndProperties { grid, properties } = *read;
    let (grid, uncertain) = crate::util::spawn_blocking(move || split_next_to_no_data(grid, area))
        .await
        .map_err(|source| IpcProcessError::IpcOther {
            msg: format!("could not split off the pixels next to no data: {source}"),
        })?;
    Ok(FileData {
        read: GdalProcessReadResult::Grid(Box::new(GridAndProperties { grid, properties })),
        uncertain: Some(uncertain),
    })
}

/// Whether the read of the file is of a reduced resolution level of a JPEG 2000 file.
///
/// The levels of JPEG 2000 are the low-pass bands of its wavelet transform, so they blend the
/// pixels at the edge of the data with the fill value next to it. Such mixed pixels are neither
/// no data nor correct, and they would cover the correct pixels of the files below. So pixels next
/// to no data in such a level are uncertain (see [`split_next_to_no_data`]): the files below
/// replace them where they have valid pixels. Elsewhere, e.g. at the outer edge of the data, they
/// keep their value, as no file has a better one.
///
/// A level is read if the reads are at least twice as coarse as the file: a file coarser than
/// the dataset is read at its full resolution up to a level twice its own factor, and resampled
/// by the nearest pixel.
fn reads_low_pass_level(params: &GdalDatasetParameters, reader_mode: &GdalReaderMode) -> bool {
    let is_jpeg_2000 = params
        .file_path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case("jp2") || extension.eq_ignore_ascii_case("j2k")
        });
    let file_factor = reader_mode
        .coarser_file_factor(&params.spatial_grid_definition())
        .map_or(1, NonZeroUsize::get);
    is_jpeg_2000 && reader_mode.level_factor().get() >= 2 * file_factor
}

/// Splits the valid pixels of `grid` next to an area of invalid pixels of it (including
/// diagonally) off as uncertain, and cuts both to `area`: the first grid has the other valid
/// pixels, the second the uncertain ones. Pixels outside of `grid`, e.g. beyond the edge of a
/// file, do not count.
///
/// An invalid pixel belongs to an area if at least two of its neighbors are invalid as well, as
/// along the edge of the fill value or a line of it. A single invalid pixel, or two, inside the
/// data is where the low-pass filter undershoots to the fill value, e.g. at dark water next to
/// snow. Its neighbors are not mixed with the fill value, so they stay certain.
fn split_next_to_no_data<T: Pixel>(
    grid: GridOrEmpty<GridBoundingBox2D, T>,
    area: GridBoundingBox2D,
) -> (
    GridOrEmpty<GridBoundingBox2D, T>,
    GridOrEmpty<GridBoundingBox2D, T>,
) {
    let GridOrEmpty::Grid(mut grid) = grid else {
        return (EmptyGrid::new(area).into(), EmptyGrid::new(area).into());
    };
    let [height, width] = grid.validity_mask.shape.axis_size();
    let valid = &grid.validity_mask.data;
    let uncertain = if valid.iter().all(|&valid| valid) {
        GridOrEmpty::from(EmptyGrid::new(area))
    } else {
        let invalid: Vec<bool> = valid.iter().map(|&valid| !valid).collect();
        let invalid_around = count_in_neighborhoods(&invalid, height, width);
        // the pixel itself and at least two of its neighbors
        let in_area: Vec<bool> = invalid
            .iter()
            .zip(&invalid_around)
            .map(|(&invalid, &count)| invalid && count >= 3)
            .collect();
        let in_area_around = count_in_neighborhoods(&in_area, height, width);
        let (certain, uncertain): (Vec<bool>, Vec<bool>) = valid
            .iter()
            .zip(&in_area_around)
            .map(|(&valid, &count)| (valid && count == 0, valid && count > 0))
            .unzip();
        let mut uncertain_grid = grid.clone();
        uncertain_grid.validity_mask.data = uncertain;
        grid.validity_mask.data = certain;
        cut_to(uncertain_grid.into(), area)
    };
    (cut_to(grid.into(), area), uncertain)
}

/// `grid` cut to `area`.
fn cut_to<T: Pixel>(
    grid: GridOrEmpty<GridBoundingBox2D, T>,
    area: GridBoundingBox2D,
) -> GridOrEmpty<GridBoundingBox2D, T> {
    if grid.bounding_box() == area {
        return grid;
    }
    let mut cut: GridOrEmpty<GridBoundingBox2D, T> = EmptyGrid::new(area).into();
    cut.grid_blit_from(&grid);
    cut
}

/// For each pixel of a `height` x `width` grid in row order, how many of the pixels of its 3 x 3
/// neighborhood inside the grid, itself included, are set in `flags`.
fn count_in_neighborhoods(flags: &[bool], height: usize, width: usize) -> Vec<u8> {
    // separable: first the neighbors in a row, then those in a column
    let mut rows = vec![0_u8; flags.len()];
    for y in 0..height {
        let row = &flags[y * width..(y + 1) * width];
        for x in 0..width {
            rows[y * width + x] = row[x.saturating_sub(1)..(x + 2).min(width)]
                .iter()
                .map(|&flag| u8::from(flag))
                .sum();
        }
    }
    let mut counts = vec![0_u8; flags.len()];
    for y in 0..height {
        for x in 0..width {
            counts[y * width + x] = (y.saturating_sub(1)..(y + 2).min(height))
                .map(|y| rows[y * width + x])
                .sum();
        }
    }
    counts
}

/// Whether every pixel of `area` lies in one of `above`. Only then can the files above fill it.
fn is_covered(area: GridBoundingBox2D, above: &[GridBoundingBox2D]) -> bool {
    // split `area` at every edge of the boxes above; each cell is inside a box or outside all
    let cuts = |min: isize, max: isize, edges: &mut dyn Iterator<Item = isize>| {
        let mut cuts: Vec<isize> = edges.filter(|&edge| edge > min && edge <= max).collect();
        cuts.push(min);
        cuts.sort_unstable();
        cuts.dedup();
        cuts
    };
    let ys = cuts(
        area.y_min(),
        area.y_max(),
        &mut above.iter().flat_map(|b| [b.y_min(), b.y_max() + 1]),
    );
    let xs = cuts(
        area.x_min(),
        area.x_max(),
        &mut above.iter().flat_map(|b| [b.x_min(), b.x_max() + 1]),
    );
    ys.iter().all(|&y| {
        xs.iter().all(|&x| {
            above.iter().any(|b| {
                (b.y_min()..=b.y_max()).contains(&y) && (b.x_min()..=b.x_max()).contains(&x)
            })
        })
    })
}

/// Checks the overview level a processing graph asks for against the levels its dataset
/// declares. Datasets without declared levels accept any level, GDAL then picks one.
///
/// # Errors
/// Returns an error if the dataset declares its levels and `overview_level` is not one of them.
pub(crate) fn ensure_overview_level_exists(
    data: &NamedData,
    overview_level: NonZeroU32,
    max_overview_level: Option<NonZeroU32>,
) -> Result<()> {
    match max_overview_level {
        Some(max_overview_level)
            if !overview_level.is_power_of_two() || overview_level > max_overview_level =>
        {
            Err(Error::OverviewLevelNotAvailable {
                data: data.to_string(),
                overview_level: overview_level.get(),
                max_overview_level: max_overview_level.get(),
            })
        }
        _ => Ok(()),
    }
}

/// The source for `target_resolution`: `source(level)` reads the coarsest power-of-two overview
/// level not coarser than the target. If the dataset declares fewer levels, the source reads its
/// coarsest one and a `Downsampling` produces the resolution of the missing level, so GDAL never
/// resamples.
pub(crate) fn overview_source(
    native_resolution: SpatialResolution,
    target_resolution: SpatialResolution,
    max_overview_level: Option<NonZeroU32>,
    source: impl FnOnce(NonZeroU32) -> TypedRasterOperator,
) -> TypedRasterOperator {
    let wanted = find_next_best_overview_level(native_resolution, target_resolution);
    let available = match max_overview_level {
        // the largest declared power of two; 1 reads the full resolution
        Some(max) => wanted.min(NonZeroU32::new(1 << max.ilog2()).unwrap_or(NonZeroU32::MIN)),
        None => wanted,
    };
    let source = source(available);
    if available >= wanted {
        return source;
    }

    let resolution = SpatialResolution::with_native_resolution_and_gdal_overview_level(
        native_resolution,
        wanted,
    )
    .unwrap_or(target_resolution);
    Downsampling {
        params: DownsamplingParams {
            sampling_method: DownsamplingMethod::NearestNeighbor,
            output_resolution: DownsamplingResolution::Resolution(resolution),
            output_origin_reference: None,
        },
        sources: SingleRasterSource { raster: source },
    }
    .typed()
}

/// `read_advise` in the pixel space of the level `factor` times coarser than the full
/// resolution, where its window has the size of its output. `None` if it does not start at a
/// pixel of the level.
pub(crate) fn level_read_advise(
    read_advise: GdalReadAdvise,
    factor: NonZeroUsize,
) -> Option<GdalReadAdvise> {
    let (start_x, start_y) = read_advise.gdal_read_widow.gdal_window_start();
    let (start_x, start_y) = (
        usize::try_from(start_x).ok()?,
        usize::try_from(start_y).ok()?,
    );
    if start_x % factor != 0 || start_y % factor != 0 {
        return None;
    }
    Some(GdalReadAdvise {
        gdal_read_widow: GdalReadWindow::new(
            GridIdx2D::new([(start_y / factor) as isize, (start_x / factor) as isize]),
            read_advise.read_window_bounds.grid_shape(),
        ),
        ..read_advise
    })
}

/// Assembles the tiles of `plans` in their order, with up to `read_ahead` tiles in flight.
/// `into_error` tags pool errors with the calling source.
pub fn tile_stream<'a, T>(
    plans: impl Stream<Item = Result<TilePlan>> + Send + 'a,
    reader_mode: GdalReaderMode,
    gdal_worker: GdalPoolDispatcher,
    blocks: Option<BlockIo>,
    read_ahead: usize,
    into_error: fn(GdalProcessPoolError) -> Error,
) -> BoxStream<'a, Result<RasterTile2D<T>>>
where
    T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType,
{
    plans
        .map(move |plan| {
            let gdal_worker = gdal_worker.clone();
            let blocks = blocks.clone();
            async move {
                assemble_tile(plan?, reader_mode, &gdal_worker, blocks.as_ref())
                    .await
                    .map_err(into_error)
            }
        })
        .buffered(read_ahead)
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::new_raster_cache::{BlockReadMode, NewRasterCacheEnum};
    use crate::source::gdal_worker_process::{
        DEFAULT_HEADER_CACHE_ENTRIES, FileNotFoundHandling, GdalDatasetGeoTransform,
        GdalMetadataMapping, GdalProcessPool, GdalProcessPoolAccess, OverviewReaderState,
        ReaderState, WorkerConfig,
    };
    use geoengine_datatypes::primitives::CacheTtlSeconds;
    use geoengine_datatypes::raster::{
        GeoTransform, Grid, GridShape2D, MaskedGrid, RasterPropertiesEntry,
        RasterPropertiesEntryType, RasterPropertiesKey,
    };
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::NonZeroFiniteF64;
    use geoengine_datatypes::util::test::TestDefault;
    use std::sync::Arc;

    #[test]
    fn it_knows_whether_the_areas_above_cover_an_area() {
        let area = |y_min, y_max, x_min, x_max| {
            GridBoundingBox2D::new_min_max(y_min, y_max, x_min, x_max).unwrap()
        };
        let file = area(0, 9, 0, 9);
        assert!(!is_covered(file, &[]));
        assert!(is_covered(file, &[area(0, 9, 0, 9)]));
        assert!(is_covered(file, &[area(-5, 20, -5, 20)]));
        // two halves that meet, in either order
        assert!(is_covered(file, &[area(0, 9, 0, 4), area(0, 9, 5, 9)]));
        assert!(is_covered(file, &[area(0, 9, 5, 9), area(0, 9, 0, 4)]));
        // four quarters, one of them overlapping the others
        assert!(is_covered(
            file,
            &[
                area(0, 4, 0, 4),
                area(0, 4, 5, 9),
                area(5, 9, 0, 4),
                area(3, 9, 3, 9)
            ]
        ));
        // a gap of one column, one missing corner pixel
        assert!(!is_covered(file, &[area(0, 9, 0, 3), area(0, 9, 5, 9)]));
        assert!(!is_covered(file, &[area(0, 9, 0, 8), area(0, 8, 9, 9)]));
        // sticking out of a box that covers most of it
        assert!(!is_covered(file, &[area(0, 9, 1, 20)]));
    }

    /// Three files of one tile: A, then B shifted 32 px to the right, then a copy of A. A and B
    /// stick out of each other and are read together; the copy of A lies in A's area, waits
    /// for it and is skipped, since A's pixels are all valid. The pixels are those of A where A
    /// lies and of B elsewhere, as if the files were read one after another.
    #[test]
    fn it_splits_off_pixels_next_to_no_data_and_cuts_to_the_area() {
        let bounds = GridBoundingBox2D::new_min_max(-1, 4, -1, 4).unwrap();
        // 6 x 6, the fill value in the first column and in a single pixel at [2, 3]
        let valid: Vec<bool> = (0..36).map(|i| i % 6 != 0 && i != 3 * 6 + 4).collect();
        let grid: GridOrEmpty<GridBoundingBox2D, u8> = MaskedGrid::new(
            Grid::new(bounds, (0..36).collect()).unwrap(),
            Grid::new(bounds, valid).unwrap(),
        )
        .unwrap()
        .into();

        let area = GridBoundingBox2D::new_min_max(0, 3, 0, 3).unwrap();
        let (certain, uncertain) = split_next_to_no_data(grid, area);
        let cut = certain.into_materialized_masked_grid();
        let uncertain = uncertain.into_materialized_masked_grid();

        assert_eq!(cut.inner_grid.shape, area);
        assert_eq!(uncertain.inner_grid.data, cut.inner_grid.data);
        assert_eq!(
            cut.inner_grid.data,
            vec![7, 8, 9, 10, 13, 14, 15, 16, 19, 20, 21, 22, 25, 26, 27, 28]
        );
        // next to the column of fill values, but not next to the single one
        assert_eq!(
            cut.validity_mask.data,
            [
                [false, true, true, true],
                [false, true, true, true],
                [false, true, true, false],
                [false, true, true, true],
            ]
            .concat()
        );
        assert_eq!(
            uncertain.validity_mask.data,
            [
                [true, false, false, false],
                [true, false, false, false],
                [true, false, false, false],
                [true, false, false, false],
            ]
            .concat()
        );

        let empty: GridOrEmpty<GridBoundingBox2D, u8> = EmptyGrid::new(bounds).into();
        let (certain, uncertain) = split_next_to_no_data(empty, area);
        assert!(certain.is_empty() && uncertain.is_empty());
    }

    /// A lossless JPEG 2000 file of 256 / `pixel_size` px in 32 px tiles over (0, 0) to
    /// (256, 256) with levels down to 16 times coarser. It has 5000 east of a diagonal edge and
    /// the fill value 0 west of it, which STAC declares as no data.
    fn jp2_with_edge(dir: &std::path::Path, pixel_size: usize) -> GdalDatasetParameters {
        let size = 256 / pixel_size;
        let file_path = dir.join(format!("edge_{pixel_size}.jp2"));
        let mut dataset = gdal::DriverManager::get_driver_by_name("MEM")
            .unwrap()
            .create_with_band_type::<u16, _>("", size, size, 1)
            .unwrap();
        dataset
            .set_spatial_ref(&gdal::spatial_ref::SpatialRef::from_epsg(32632).unwrap())
            .unwrap();
        let pixel = pixel_size as f64;
        dataset
            .set_geo_transform(&[0., pixel, 0., 256., 0., -pixel])
            .unwrap();
        let data = (0..size * size)
            .map(|i| {
                let (x, y) = ((i % size) as f64 + 0.5, (i / size) as f64 + 0.5);
                if x * pixel > 0.5 * y * pixel + 60.3 {
                    5000
                } else {
                    0
                }
            })
            .collect();
        dataset
            .rasterband(1)
            .unwrap()
            .write(
                (0, 0),
                (size, size),
                &mut gdal::raster::Buffer::new((size, size), data),
            )
            .unwrap();
        let mut options = gdal::raster::RasterCreationOptions::new();
        for (name, value) in [
            ("REVERSIBLE", "YES"),
            ("QUALITY", "100"),
            ("RESOLUTIONS", "5"),
            ("BLOCKXSIZE", "32"),
            ("BLOCKYSIZE", "32"),
        ] {
            options.add_name_value(name, value).unwrap();
        }
        dataset
            .create_copy(
                &gdal::DriverManager::get_driver_by_name("JP2OpenJPEG")
                    .expect("GDAL should have the JP2OpenJPEG driver"),
                &file_path,
                &options,
            )
            .unwrap();

        GdalDatasetParameters {
            file_path,
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (0., 256.).into(),
                x_pixel_size: NonZeroFiniteF64::new_unchecked(pixel),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-pixel),
            },
            width: size,
            height: size,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: Some(0.),
            properties_mapping: None,
            gdal_open_options: Some(vec!["USE_TILE_AS_BLOCK=YES".to_string()]),
            gdal_config_options: None,
            allow_alphaband_as_mask: false,
            retry: None,
        }
    }

    /// A 256 px `GeoTIFF` over the same area as [`jp2_with_edge`] that is 3000 everywhere.
    fn tiff_filled_with_3000(dir: &std::path::Path) -> GdalDatasetParameters {
        let file_path = dir.join("below.tif");
        let mut dataset = gdal::DriverManager::get_driver_by_name("GTiff")
            .unwrap()
            .create_with_band_type::<u16, _>(&file_path, 256, 256, 1)
            .unwrap();
        dataset
            .set_spatial_ref(&gdal::spatial_ref::SpatialRef::from_epsg(32632).unwrap())
            .unwrap();
        dataset
            .set_geo_transform(&[0., 1., 0., 256., 0., -1.])
            .unwrap();
        dataset
            .rasterband(1)
            .unwrap()
            .write(
                (0, 0),
                (256, 256),
                &mut gdal::raster::Buffer::new((256, 256), vec![3000; 256 * 256]),
            )
            .unwrap();
        dataset.build_overviews("NEAREST", &[2, 4], &[]).unwrap();
        dataset.close().unwrap();

        GdalDatasetParameters {
            file_path,
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (0., 256.).into(),
                x_pixel_size: NonZeroFiniteF64::new_unchecked(1.),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-1.),
            },
            width: 256,
            height: 256,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: false,
            retry: None,
        }
    }

    /// The levels of a JPEG 2000 file blend its data with the fill value at the edge of the
    /// data. Those mixed pixels must not cover the file below: each pixel is the data of the
    /// file above (5000, give or take the few percent its wavelet filter over- and undershoots
    /// next to the edge) or that of the file below (3000), never a mix. This holds at the
    /// levels of the file itself and of a file that is coarser than the dataset, on both
    /// read paths, also where the edge crosses the border of a tile.
    #[tokio::test]
    async fn it_reads_no_mixed_edge_pixels_from_jpeg_2000_levels() {
        let dir = tempfile::tempdir().unwrap();
        let above = jp2_with_edge(dir.path(), 1);
        let coarser_above = jp2_with_edge(dir.path(), 2);
        let below = tiff_filled_with_3000(dir.path());
        let dataset_grid = below.spatial_grid_definition();
        let is_above = |value: u16| value.abs_diff(5000) <= 250;

        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(60));

        for (above, level) in [
            (&above, 2_usize),
            (&above, 4),
            (&coarser_above, 2),
            (&coarser_above, 4),
        ] {
            let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
                original_dataset_grid: dataset_grid,
                overview_level: NonZeroUsize::new(level).unwrap(),
            });
            // 4 x 4 tiles, so that the edge crosses tile borders
            let level_size = 256 / level;
            let tile_size = level_size / 4;
            for blocks in [None, Some(&blocks)] {
                let mut level_pixels = vec![0_u16; level_size * level_size];
                for tile_y in 0..4 {
                    for tile_x in 0..4 {
                        let plan = TilePlan {
                            tile: TileInformation::new(
                                [tile_y as isize, tile_x as isize].into(),
                                GridShape2D::new([tile_size, tile_size]),
                                GeoTransform::new_unchecked(
                                    (0., 256.).into(),
                                    level as f64,
                                    -(level as f64),
                                ),
                            ),
                            time: TimeInterval::default(),
                            band: 0,
                            files: vec![above.clone(), below.clone()],
                            cache_hint: CacheHint::max_duration(),
                        };
                        let tile = assemble_tile::<u16>(plan, reader_mode, &gdal_worker, blocks)
                            .await
                            .unwrap()
                            .grid_array
                            .into_materialized_masked_grid();
                        assert!(
                            tile.validity_mask.data.iter().all(|&valid| valid),
                            "the file below fills the tile"
                        );
                        for y in 0..tile_size {
                            for x in 0..tile_size {
                                level_pixels[(tile_y * tile_size + y) * level_size
                                    + tile_x * tile_size
                                    + x] = tile.inner_grid.data[y * tile_size + x];
                            }
                        }
                    }
                }

                let case = format!(
                    "{} at level {level} through {}",
                    above.file_path.display(),
                    if blocks.is_some() { "blocks" } else { "GDAL" }
                );
                let mixed: Vec<(usize, usize, u16)> = level_pixels
                    .iter()
                    .enumerate()
                    .filter(|&(_, &value)| value != 3000 && !is_above(value))
                    .map(|(i, &value)| (i / level_size, i % level_size, value))
                    .collect();
                assert!(mixed.is_empty(), "{case}: mixed pixels {mixed:?}");
                // in each row the file below west of the edge, the file above east of it
                for row in level_pixels.chunks(level_size) {
                    let east = row.iter().position(|&value| is_above(value));
                    assert!(
                        east.is_some(),
                        "{case}: every row has data of the file above"
                    );
                    assert!(
                        row[east.unwrap_or_default()..]
                            .iter()
                            .all(|&value| is_above(value)),
                        "{case}: the file above has no gaps east of its edge: {row:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn it_treats_only_reduced_levels_of_jpeg_2000_files_as_low_pass() {
        let dir = tempfile::tempdir().unwrap();
        let tiff = tiled_tiff(dir.path(), 64, &[2]);
        let jp2 = GdalDatasetParameters {
            file_path: dir.path().join("a.JP2"),
            ..tiff.clone()
        };
        let coarser_jp2 = GdalDatasetParameters {
            geo_transform: GdalDatasetGeoTransform {
                x_pixel_size: NonZeroFiniteF64::new_unchecked(2.),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-2.),
                ..tiff.geo_transform
            },
            width: 32,
            height: 32,
            ..jp2.clone()
        };
        let dataset_grid = tiff.spatial_grid_definition();
        let full = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: dataset_grid,
        });
        let level = |level| {
            GdalReaderMode::OverviewLevel(OverviewReaderState {
                original_dataset_grid: dataset_grid,
                overview_level: NonZeroUsize::new(level).unwrap(),
            })
        };

        // other files are read as they are at every level
        for mode in [full, level(2), level(4)] {
            assert!(!reads_low_pass_level(&tiff, &mode));
        }
        assert!(!reads_low_pass_level(&jp2, &full));
        assert!(reads_low_pass_level(&jp2, &level(2)));
        // a file twice as coarse is read at its full resolution at level 2
        assert!(!reads_low_pass_level(&coarser_jp2, &level(2)));
        assert!(reads_low_pass_level(&coarser_jp2, &level(4)));
    }

    /// Without a file below, the uncertain pixels next to the fill value keep their value: the
    /// tiles are those of the same file read without the rule for JPEG 2000 levels.
    #[tokio::test]
    async fn it_keeps_uncertain_pixels_no_file_below_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let jp2 = jp2_with_edge(dir.path(), 1);
        // GDAL opens the file by its content, so the rule does not apply to it
        let plain = GdalDatasetParameters {
            file_path: dir.path().join("edge_1.raw"),
            ..jp2.clone()
        };
        std::fs::copy(&jp2.file_path, &plain.file_path).unwrap();
        let dataset_grid = jp2.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();

        for level in [2_usize, 4] {
            let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
                original_dataset_grid: dataset_grid,
                overview_level: NonZeroUsize::new(level).unwrap(),
            });
            let tile_size = 256 / level / 4;
            for tile_y in 0..4 {
                for tile_x in 0..4 {
                    let plan = |file: &GdalDatasetParameters| TilePlan {
                        tile: TileInformation::new(
                            [tile_y, tile_x].into(),
                            GridShape2D::new([tile_size, tile_size]),
                            GeoTransform::new_unchecked(
                                (0., 256.).into(),
                                level as f64,
                                -(level as f64),
                            ),
                        ),
                        time: TimeInterval::default(),
                        band: 0,
                        files: vec![file.clone()],
                        cache_hint: CacheHint::max_duration(),
                    };
                    let with_rule =
                        assemble_tile::<u16>(plan(&jp2), reader_mode, &gdal_worker, None)
                            .await
                            .unwrap();
                    let without_rule =
                        assemble_tile::<u16>(plan(&plain), reader_mode, &gdal_worker, None)
                            .await
                            .unwrap();
                    assert_eq!(
                        with_rule.grid_array.into_materialized_masked_grid(),
                        without_rule.grid_array.into_materialized_masked_grid(),
                        "level {level}, tile [{tile_y}, {tile_x}]"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn it_reads_files_of_a_tile_together_and_merges_them_in_file_order() {
        let dir = tempfile::tempdir().unwrap();
        let a = tiled_tiff(dir.path(), 64, &[2]);
        // the same pixels in a second file, 32 px to the right
        let b_dir = tempfile::tempdir().unwrap();
        let mut b = tiled_tiff(b_dir.path(), 64, &[2]);
        gdal::Dataset::open_ex(
            &b.file_path,
            gdal::DatasetOptions {
                open_flags: gdal::GdalOpenFlags::GDAL_OF_UPDATE,
                ..Default::default()
            },
        )
        .unwrap()
        .set_geo_transform(&[32., 1., 0., 64., 0., -1.])
        .unwrap();
        b.geo_transform.origin_coordinate = (32., 64.).into();
        let gpp = GdalProcessPool::new(
            4,
            4,
            4,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: SpatialGridDefinition::new(
                GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
                GridBoundingBox2D::new_min_max(0, 63, 0, 95).unwrap(),
            ),
        });
        let plan = TilePlan {
            tile: TileInformation::new(
                [0, 0].into(),
                GridShape2D::new([64, 96]),
                GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
            ),
            time: TimeInterval::default(),
            band: 0,
            files: vec![a.clone(), b, a],
            cache_hint: CacheHint::max_duration(),
        };

        let tile = assemble_tile::<u16>(plan, reader_mode, &gdal_worker, None)
            .await
            .unwrap()
            .grid_array
            .into_materialized_masked_grid();

        for y in 0..64 {
            for x in 0..96 {
                let from = if x < 64 { x } else { x - 32 };
                assert_eq!(
                    tile.inner_grid.data[y * 96 + x],
                    (y * 64 + from) as u16,
                    "pixel [{y}, {x}]"
                );
            }
        }
        assert_eq!(
            gpp.request_stats().reads + gpp.request_stats().reads_joined,
            2,
            "the copy of A is not read"
        );
    }

    #[tokio::test]
    async fn it_reads_only_remote_files_through_blocks_in_auto_mode() {
        let remote = |path: &str| GdalDatasetParameters {
            file_path: path.into(),
            ..ndvi_jan_2014_params()
        };
        let local = ndvi_jan_2014_params();
        let remote_paths = [
            "https://example.com/a.tif",
            "s3://bucket/a.tif",
            "/vsicurl/https://example.com/a.tif",
            "/vsis3/bucket/a.tif",
            "SENTINEL2_L2A:/vsis3/eodata/MTD_MSIL2A.xml:10m:EPSG_32632",
            "/vsizip//vsicurl/https://example.com/a.zip/a.tif",
        ];
        let blocks = |mode| {
            let mut ctx = crate::engine::MockExecutionContext::test_default()
                .mock_query_context_test_default();
            ctx.new_raster_cache = Some(Arc::new(
                NewRasterCacheEnum::new_lru(1_000_000).with_block_read_mode(mode),
            ));
            BlockIo::from_query_context(&ctx)
        };

        assert!(blocks(BlockReadMode::Off).is_none());
        let auto = blocks(BlockReadMode::Auto).unwrap();
        assert!(!auto.reads(&local));
        assert!(!auto.reads(&remote("/vsizip/local.zip/a.tif")));
        for path in remote_paths {
            assert!(auto.reads(&remote(path)), "{path}");
        }
        let on = blocks(BlockReadMode::On).unwrap();
        assert!(on.reads(&local));
        assert!(on.reads(&remote(remote_paths[0])));

        // one path per tile: a tile with a file the block reader does not read goes through GDAL
        let plan = |files| TilePlan {
            tile: TileInformation::new(
                [0, 0].into(),
                GridShape2D::new([8, 8]),
                GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
            ),
            time: TimeInterval::default(),
            band: 0,
            files,
            cache_hint: CacheHint::max_duration(),
        };
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: local.spatial_grid_definition(),
        });
        let all_remote = plan(vec![remote(remote_paths[0]), remote(remote_paths[1])]);
        let mixed = plan(vec![remote(remote_paths[0]), local.clone()]);
        assert!(matches!(
            ReadPath::of(Some(&auto), &all_remote, &reader_mode),
            ReadPath::Blocks(_)
        ));
        assert!(matches!(
            ReadPath::of(Some(&auto), &mixed, &reader_mode),
            ReadPath::Gdal
        ));
        assert!(matches!(
            ReadPath::of(Some(&on), &mixed, &reader_mode),
            ReadPath::Blocks(_)
        ));
        assert!(matches!(
            ReadPath::of(None, &all_remote, &reader_mode),
            ReadPath::Gdal
        ));

        // a file twice as coarse as the dataset has no level of the dataset: its tile goes
        // through GDAL as a whole
        let coarser = GdalDatasetParameters {
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: NonZeroFiniteF64::new_unchecked(0.2),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-0.2),
            },
            width: 1800,
            height: 900,
            ..local.clone()
        };
        let with_coarser = plan(vec![local.clone(), coarser]);
        assert!(matches!(
            ReadPath::of(Some(&on), &with_coarser, &reader_mode),
            ReadPath::Gdal
        ));
    }

    fn ndvi_jan_2014_params() -> GdalDatasetParameters {
        GdalDatasetParameters {
            file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (-180., 90.).into(),
                x_pixel_size: NonZeroFiniteF64::new_unchecked(0.1),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-0.1),
            },
            width: 3600,
            height: 1800,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: Some(0.),
            properties_mapping: Some(vec![
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: None,
                        key: "AREA_OR_POINT".to_string(),
                    },
                },
                GdalMetadataMapping {
                    source_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                    target_type: RasterPropertiesEntryType::String,
                    target_key: RasterPropertiesKey {
                        domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                        key: "COMPRESSION".to_string(),
                    },
                },
            ]),
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        }
    }

    #[tokio::test]
    async fn it_assembles_a_tile_from_one_file() {
        let params = ndvi_jan_2014_params();
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: params.spatial_grid_definition(),
        });
        // the file's own grid: a pixel size computed from bounds is off by rounding and does not
        // align with the file
        let tile_information = TileInformation {
            tile_size_in_pixels: [8, 8].into(),
            global_tile_position: [0, 0].into(),
            global_geo_transform: GeoTransform::new_unchecked((-180., 90.).into(), 0.1, -0.1),
        };

        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );

        let tile = assemble_tile::<u8>(
            TilePlan {
                tile: tile_information,
                time: TimeInterval::default(),
                band: 0,
                files: vec![params],
                cache_hint: CacheHint::no_cache(),
            },
            reader_mode,
            &gpp.get_gdal_worker(),
            None,
        )
        .await
        .unwrap();

        assert!(!tile.is_empty());
        let grid = tile.grid_array.into_materialized_masked_grid();
        assert_eq!(grid.inner_grid.data, &[255; 64]);
        assert_eq!(grid.validity_mask.data, &[true; 64]);

        let properties = tile.properties;
        assert!(properties.scale_option().is_none());
        assert!(properties.offset_option().is_none());
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                key: "COMPRESSION".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("LZW".to_string()))
        );
        assert_eq!(
            properties.get_property(&RasterPropertiesKey {
                domain: None,
                key: "AREA_OR_POINT".to_string(),
            }),
            Some(&RasterPropertiesEntry::String("Area".to_string()))
        );
    }

    /// A 64x64 u16 `GeoTIFF` in 16x16 blocks with overviews 2 and 4, every pixel distinct.
    fn tiled_tiff_with_overviews(dir: &std::path::Path) -> GdalDatasetParameters {
        tiled_tiff(dir, 64, &[2, 4])
    }

    /// A `size`² `u16` file in 16 px blocks whose pixel values are `y * size + x`, with origin
    /// (0, 64) and the given overviews.
    fn tiled_tiff(dir: &std::path::Path, size: usize, overviews: &[i32]) -> GdalDatasetParameters {
        let file_path = dir.join(format!("blocks_{size}.tif"));
        let driver = gdal::DriverManager::get_driver_by_name("GTiff").unwrap();
        let options: gdal::cpl::CslStringList = ["TILED=YES", "BLOCKXSIZE=16", "BLOCKYSIZE=16"]
            .into_iter()
            .collect();
        let mut dataset = driver
            .create_with_band_type_with_options::<u16, _>(&file_path, size, size, 1, &options)
            .unwrap();
        dataset
            .set_spatial_ref(&gdal::spatial_ref::SpatialRef::from_epsg(4326).unwrap())
            .unwrap();
        dataset
            .set_geo_transform(&[0., 1., 0., 64., 0., -1.])
            .unwrap();
        let mut buffer =
            gdal::raster::Buffer::new((size, size), (0..size * size).map(|v| v as u16).collect());
        dataset
            .rasterband(1)
            .unwrap()
            .write((0, 0), (size, size), &mut buffer)
            .unwrap();
        if !overviews.is_empty() {
            dataset.build_overviews("NEAREST", overviews, &[]).unwrap();
        }
        dataset.close().unwrap();

        GdalDatasetParameters {
            file_path,
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: (0., 64.).into(),
                x_pixel_size: NonZeroFiniteF64::new_unchecked(1.),
                y_pixel_size: NonZeroFiniteF64::new_unchecked(-1.),
            },
            width: size,
            height: size,
            file_not_found_handling: FileNotFoundHandling::Error,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: true,
            retry: None,
        }
    }

    #[test]
    fn it_reads_only_declared_overviews_and_downsamples_the_rest() {
        let native = SpatialResolution::new_unchecked(10., 10.);
        let target = SpatialResolution::new_unchecked(80., 80.);
        let level = |level| NonZeroU32::new(level).unwrap();
        let source = |level: NonZeroU32| {
            crate::source::GdalSource {
                params: crate::source::GdalSourceParameters::new(NamedData::with_system_name(
                    "data",
                ))
                .with_overview_level(Some(level)),
            }
            .typed()
        };
        let json = |operator: &TypedRasterOperator| serde_json::to_value(operator).unwrap();

        assert_eq!(
            json(&overview_source(native, target, None, source)),
            json(&source(level(8))),
            "unknown levels: as before"
        );
        assert_eq!(
            json(&overview_source(native, target, Some(level(16)), source)),
            json(&source(level(8)))
        );

        let downsampled = json(&overview_source(native, target, Some(level(4)), source));
        assert_eq!(downsampled["type"], "Downsampling");
        assert_eq!(downsampled["params"]["outputResolution"]["x"], 80.);
        assert_eq!(downsampled["sources"]["raster"], json(&source(level(4))));

        let full = json(&overview_source(native, target, Some(level(1)), source));
        assert_eq!(full["sources"]["raster"], json(&source(level(1))));
    }

    #[test]
    fn it_rejects_undeclared_overview_levels() {
        let data = NamedData::with_system_name("data");
        let level = |level| std::num::NonZeroU32::new(level).unwrap();
        assert!(ensure_overview_level_exists(&data, level(8), None).is_ok());
        assert!(ensure_overview_level_exists(&data, level(4), Some(level(4))).is_ok());
        assert!(ensure_overview_level_exists(&data, level(1), Some(level(1))).is_ok());
        assert!(ensure_overview_level_exists(&data, level(8), Some(level(4))).is_err());
        assert!(ensure_overview_level_exists(&data, level(3), Some(level(4))).is_err());
        assert!(ensure_overview_level_exists(&data, level(2), Some(level(1))).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tiles_that_share_a_unit_read_and_store_it_once() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(60));

        // the 64 px file is one unit, and one piece, of the 16 px blocks grouped to 512 px
        let tiles = (0..2)
            .flat_map(|y| (0..2).map(move |x| [y, x]))
            .map(|position| {
                let plan = TilePlan {
                    tile: TileInformation::new(
                        position.into(),
                        GridShape2D::new([32, 32]),
                        GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
                    ),
                    time: TimeInterval::default(),
                    band: 0,
                    files: vec![params.clone()],
                    cache_hint: CacheHint::max_duration(),
                };
                let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
                    dataset_spatial_grid: file_grid,
                });
                let (gdal_worker, blocks) = (gdal_worker.clone(), blocks.clone());
                tokio::spawn(async move {
                    assemble_tile::<u16>(plan, reader_mode, &gdal_worker, Some(&blocks)).await
                })
            });
        for tile in futures::future::join_all(tiles.collect::<Vec<_>>()).await {
            tile.unwrap().unwrap();
        }

        let stats = cache.stats();
        assert_eq!(stats.stored, 1);
        assert_eq!(stats.inserts_joined, 0);
        assert_eq!(
            gpp.request_stats().reads + gpp.request_stats().reads_joined,
            1
        );
    }

    #[tokio::test]
    async fn tiles_read_windows_when_blocks_would_not_stay_cached() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        // no minimum lifetime and a tile that must not be cached, as in the mock query contexts
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(0));

        let plan = TilePlan {
            tile: TileInformation::new(
                [0, 0].into(),
                GridShape2D::new([32, 32]),
                GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
            ),
            time: TimeInterval::default(),
            band: 0,
            files: vec![params.clone()],
            cache_hint: CacheHint::no_cache(),
        };
        let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
            dataset_spatial_grid: file_grid,
        });
        let from_blocks =
            assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, Some(&blocks))
                .await
                .unwrap();
        let from_window = assemble_tile::<u16>(plan, reader_mode, &gdal_worker, None)
            .await
            .unwrap();

        assert_eq!(from_blocks.grid_array, from_window.grid_array);
        assert_eq!(cache.stats().stored, 0, "no block is stored");
        assert_eq!(gpp.request_stats().headers, 0, "no header is read");
    }

    #[tokio::test]
    async fn block_reads_reuse_the_header_until_caches_are_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache, gdal_worker.clone(), CacheTtlSeconds::new(60));

        let read = || {
            let plan = TilePlan {
                tile: TileInformation::new(
                    [0, 0].into(),
                    GridShape2D::new([32, 32]),
                    GeoTransform::new_unchecked((0., 64.).into(), 1., -1.),
                ),
                time: TimeInterval::default(),
                band: 0,
                files: vec![params.clone()],
                cache_hint: CacheHint::max_duration(),
            };
            let reader_mode = GdalReaderMode::OriginalResolution(ReaderState {
                dataset_spatial_grid: file_grid,
            });
            assemble_tile::<u16>(plan, reader_mode, &gdal_worker, Some(&blocks))
        };

        read().await.unwrap();
        read().await.unwrap();
        assert_eq!(gpp.request_stats().headers, 1);

        gpp.clear_caches().await.unwrap();
        read().await.unwrap();
        assert_eq!(gpp.request_stats().headers, 2);
    }

    /// A file without any overview stays on the block path: its level 4 and 8 are resampled
    /// from blocks of the full resolution, with the pixels of GDAL's window read.
    #[tokio::test]
    async fn files_without_overviews_are_resampled_from_full_resolution_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff(dir.path(), 64, &[]);
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();

        for level in [4_usize, 8] {
            let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
            let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(60));
            let size = 64 / level;
            let plan = TilePlan {
                tile: TileInformation::new(
                    [0, 0].into(),
                    GridShape2D::new([size, size]),
                    GeoTransform::new_unchecked((0., 64.).into(), level as f64, -(level as f64)),
                ),
                time: TimeInterval::default(),
                band: 0,
                files: vec![params.clone()],
                cache_hint: CacheHint::max_duration(),
            };
            for repeated in [false, true] {
                let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
                    original_dataset_grid: file_grid,
                    overview_level: std::num::NonZeroUsize::new(level).unwrap(),
                });
                let from_gdal = assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, None)
                    .await
                    .unwrap();
                let reads = gpp.request_stats().reads;
                let tile =
                    assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, Some(&blocks))
                        .await
                        .unwrap();
                assert_eq!(tile, from_gdal, "level {level}, repeated: {repeated}");
                assert!(cache.byte_size() > 0, "level {level}: blocks were cached");
                if repeated {
                    assert_eq!(
                        gpp.request_stats().reads,
                        reads,
                        "level {level}: the second read took the cached blocks"
                    );
                }
            }
        }
    }

    /// A resampling read does not hold its level window: level 16 of a 256 px file without
    /// overviews spans the whole file, 131 kB of pixels, and reads it through a block cache of
    /// 20 kB with the pixels of GDAL's window read.
    #[tokio::test]
    async fn resampling_reads_do_not_need_their_window_in_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff(dir.path(), 256, &[]);
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(20_000));
        let blocks = BlockIo::new(cache, gdal_worker.clone(), CacheTtlSeconds::new(60));

        let plan = TilePlan {
            tile: TileInformation::new(
                [0, 0].into(),
                GridShape2D::new([16, 16]),
                GeoTransform::new_unchecked((0., 64.).into(), 16., -16.),
            ),
            time: TimeInterval::default(),
            band: 0,
            files: vec![params.clone()],
            cache_hint: CacheHint::max_duration(),
        };
        let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
            original_dataset_grid: file_grid,
            overview_level: std::num::NonZeroUsize::new(16).unwrap(),
        });
        let from_gdal = assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, None)
            .await
            .unwrap();
        let tile = assemble_tile::<u16>(plan, reader_mode, &gdal_worker, Some(&blocks))
            .await
            .unwrap();
        assert_eq!(tile, from_gdal);
    }

    #[tokio::test]
    async fn reads_of_a_level_the_file_lacks_give_the_same_pixels_on_both_paths() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache, gdal_worker.clone(), CacheTtlSeconds::new(60));

        let plan = TilePlan {
            tile: TileInformation::new(
                [0, 0].into(),
                GridShape2D::new([8, 8]),
                GeoTransform::new_unchecked((0., 64.).into(), 8., -8.),
            ),
            time: TimeInterval::default(),
            band: 0,
            files: vec![params.clone()],
            cache_hint: CacheHint::max_duration(),
        };
        // the file has the overviews 2 and 4
        let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
            original_dataset_grid: file_grid,
            overview_level: std::num::NonZeroUsize::new(8).unwrap(),
        });

        let from_gdal = assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, None)
            .await
            .unwrap();
        for blocks in [None, Some(&blocks)] {
            let tile = assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, blocks)
                .await
                .unwrap();
            assert_eq!(tile, from_gdal, "blocks: {}", blocks.is_some());
        }
    }

    /// GDAL scales a window by the sizes of the level and of the file, not by the level's factor.
    /// Level 4 of 61 px has 16 px, 3.8 times fewer, so a window read that starts at its pixel 11
    /// or later starts one pixel later in the level. Blocks resample such a level from itself:
    /// each tile pixel takes the level pixel under its center, in every tile alike.
    #[tokio::test]
    async fn levels_whose_size_does_not_fit_are_resampled_from_themselves() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff(dir.path(), 61, &[4]);
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(60));
        let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
            original_dataset_grid: file_grid,
            overview_level: std::num::NonZeroUsize::new(4).unwrap(),
        });
        let plan = |tile_size: usize, position: [isize; 2]| TilePlan {
            tile: TileInformation::new(
                position.into(),
                GridShape2D::new([tile_size, tile_size]),
                GeoTransform::new_unchecked((0., 64.).into(), 4., -4.),
            ),
            time: TimeInterval::default(),
            band: 0,
            files: vec![params.clone()],
            cache_hint: CacheHint::max_duration(),
        };

        // the level as it is in the file
        let level = assemble_tile::<u16>(plan(16, [0, 0]), reader_mode, &gdal_worker, None)
            .await
            .unwrap()
            .grid_array
            .into_materialized_masked_grid();
        let whole =
            assemble_tile::<u16>(plan(16, [0, 0]), reader_mode, &gdal_worker, Some(&blocks))
                .await
                .unwrap()
                .grid_array
                .into_materialized_masked_grid();
        // the center of tile pixel `p` lies at 4 p + 2 of the file's 61 px, which its 16 level
        // pixels share
        let under_center = |pixel: usize| ((2 * pixel + 1) * 4 * 16 / (2 * 61)).min(15);
        assert_eq!(under_center(9), 9);
        assert_eq!(under_center(10), 11);
        for y in 0..16 {
            for x in 0..16 {
                assert_eq!(
                    whole.inner_grid.data[y * 16 + x],
                    level.inner_grid.data[under_center(y) * 16 + under_center(x)],
                    "pixel [{y}, {x}]"
                );
            }
        }

        for tile_size in [3_usize, 4, 5] {
            for tile_y in 0..16_usize.div_ceil(tile_size) {
                for tile_x in 0..16_usize.div_ceil(tile_size) {
                    let tile = assemble_tile::<u16>(
                        plan(tile_size, [tile_y as isize, tile_x as isize]),
                        reader_mode,
                        &gdal_worker,
                        Some(&blocks),
                    )
                    .await
                    .unwrap()
                    .grid_array
                    .into_materialized_masked_grid();
                    for y in 0..tile_size.min(16 - tile_y * tile_size) {
                        for x in 0..tile_size.min(16 - tile_x * tile_size) {
                            let (global_y, global_x) =
                                (tile_y * tile_size + y, tile_x * tile_size + x);
                            assert_eq!(
                                tile.inner_grid.data[y * tile_size + x],
                                whole.inner_grid.data[global_y * 16 + global_x],
                                "tile size {tile_size}, pixel [{global_y}, {global_x}]"
                            );
                        }
                    }
                }
            }
        }
        let stats = gpp.request_stats();
        assert!(
            cache.stats().stored > 0,
            "the reads went through the blocks"
        );
        assert_eq!(stats.headers, 1, "one header for the file");
    }

    #[tokio::test]
    async fn whole_number_levels_are_resampled_alike_in_every_tile() {
        let dir = tempfile::tempdir().unwrap();
        // only the level 3, whose 22 px are 2.9 times smaller than the file's 64 px
        let params = tiled_tiff(dir.path(), 64, &[3]);
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(60));
        let plan = |level: u32, tile_size: usize, position: [isize; 2]| TilePlan {
            tile: TileInformation::new(
                position.into(),
                GridShape2D::new([tile_size, tile_size]),
                GeoTransform::new_unchecked((0., 64.).into(), f64::from(level), -f64::from(level)),
            ),
            time: TimeInterval::default(),
            band: 0,
            files: vec![params.clone()],
            cache_hint: CacheHint::max_duration(),
        };

        for level in [4_u32, 8] {
            let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
                original_dataset_grid: file_grid,
                overview_level: std::num::NonZeroUsize::new(level as usize).unwrap(),
            });
            let size = 64 / level as usize;
            // one tile of the whole file: GDAL's window then maps onto the level exactly
            let whole = assemble_tile::<u16>(
                plan(level, size, [0, 0]),
                reader_mode,
                &gdal_worker,
                Some(&blocks),
            )
            .await
            .unwrap();
            let from_gdal =
                assemble_tile::<u16>(plan(level, size, [0, 0]), reader_mode, &gdal_worker, None)
                    .await
                    .unwrap();
            assert_eq!(
                whole.grid_array, from_gdal.grid_array,
                "level {level}, whole file"
            );
            let whole = whole.grid_array.into_materialized_masked_grid();

            // smaller tiles take the same pixels; GDAL rounds each tile's window into the level
            // to whole pixels, so its pixels depend on the tile
            for tile_size in [3_usize, 4, 5] {
                for tile_y in 0..size.div_ceil(tile_size) {
                    for tile_x in 0..size.div_ceil(tile_size) {
                        let tile = assemble_tile::<u16>(
                            plan(level, tile_size, [tile_y as isize, tile_x as isize]),
                            reader_mode,
                            &gdal_worker,
                            Some(&blocks),
                        )
                        .await
                        .unwrap()
                        .grid_array
                        .into_materialized_masked_grid();
                        for y in 0..tile_size.min(size - tile_y * tile_size) {
                            for x in 0..tile_size.min(size - tile_x * tile_size) {
                                let (global_y, global_x) =
                                    (tile_y * tile_size + y, tile_x * tile_size + x);
                                assert_eq!(
                                    tile.inner_grid.data[y * tile_size + x],
                                    whole.inner_grid.data[global_y * size + global_x],
                                    "level {level}, tile size {tile_size}, pixel [{global_y}, {global_x}]"
                                );
                            }
                        }
                    }
                }
            }
        }
        assert!(
            cache.stats().stored > 0,
            "the reads went through the blocks"
        );
    }

    #[tokio::test]
    async fn levels_a_file_lacks_are_subsampled_from_its_coarsest_level_through_blocks() {
        for (overviews, levels) in [(vec![2, 4], vec![8_u32, 16])] {
            let dir = tempfile::tempdir().unwrap();
            let params = tiled_tiff(dir.path(), 64, &overviews);
            let file_grid = params.spatial_grid_definition();
            let gpp = GdalProcessPool::new(
                2,
                2,
                2,
                DEFAULT_HEADER_CACHE_ENTRIES,
                WorkerConfig::default(),
            );
            let gdal_worker = gpp.get_gdal_worker();
            let block_pool = GdalProcessPool::new(
                2,
                2,
                2,
                DEFAULT_HEADER_CACHE_ENTRIES,
                WorkerConfig::default(),
            );
            let block_worker = block_pool.get_gdal_worker();
            let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
            let blocks = BlockIo::new(
                cache.clone(),
                block_worker.clone(),
                CacheTtlSeconds::new(60),
            );

            for (overview_level, tile_size) in levels
                .iter()
                .flat_map(|&level| [8_usize, 4, 3].map(|tile_size| (level, tile_size)))
            {
                let pixel_size = f64::from(overview_level);
                let tiles_per_axis = (64 / overview_level as usize).div_ceil(tile_size);

                let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
                    original_dataset_grid: file_grid,
                    overview_level: std::num::NonZeroUsize::new(overview_level as usize).unwrap(),
                });
                for tile_y in 0..tiles_per_axis {
                    for tile_x in 0..tiles_per_axis {
                        let plan = TilePlan {
                            tile: TileInformation::new(
                                [tile_y as isize, tile_x as isize].into(),
                                GridShape2D::new([tile_size, tile_size]),
                                GeoTransform::new_unchecked(
                                    (0., 64.).into(),
                                    pixel_size,
                                    -pixel_size,
                                ),
                            ),
                            time: TimeInterval::default(),
                            band: 0,
                            files: vec![params.clone()],
                            cache_hint: CacheHint::max_duration(),
                        };
                        let from_gdal =
                            assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, None)
                                .await
                                .unwrap();
                        let from_blocks =
                            assemble_tile::<u16>(plan, reader_mode, &block_worker, Some(&blocks))
                                .await
                                .unwrap();
                        assert_eq!(
                            from_blocks, from_gdal,
                            "level {overview_level}, tile size {tile_size}, tile [{tile_y}, {tile_x}]"
                        );
                    }
                }
            }
            // every read of the block pass stored a unit: no tile fell back to GDAL
            let stats = block_pool.request_stats();
            assert!(
                cache.stats().stored > 0,
                "the reads went through the blocks"
            );
            assert_eq!(stats.reads + stats.reads_joined, cache.stats().stored);
        }
    }

    #[tokio::test]
    async fn files_shifted_against_the_tile_grid_read_their_levels_through_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        // the block reads get their own pool, so that its counters show every read they caused
        let block_pool = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let block_worker = block_pool.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(
            cache.clone(),
            block_worker.clone(),
            CacheTtlSeconds::new(60),
        );
        // as MGRS tiles: the file starts 3 px right of and below a pixel edge of the level-4 tile grid
        let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
            original_dataset_grid: file_grid,
            overview_level: std::num::NonZeroUsize::new(4).unwrap(),
        });

        for tile_y in 0..3 {
            for tile_x in 0..3 {
                let plan = TilePlan {
                    tile: TileInformation::new(
                        [tile_y, tile_x].into(),
                        GridShape2D::new([8, 8]),
                        GeoTransform::new_unchecked((-3., 67.).into(), 4., -4.),
                    ),
                    time: TimeInterval::default(),
                    band: 0,
                    files: vec![params.clone()],
                    cache_hint: CacheHint::max_duration(),
                };
                if let Some(read_advise) = reader_mode
                    .tiling_to_dataset_read_advise(&file_grid, &plan.tile.spatial_grid_definition())
                {
                    assert!(
                        level_read_advise(read_advise, NonZeroUsize::new(4).unwrap()).is_some(),
                        "the read of tile [{tile_y}, {tile_x}] starts on a pixel of level 4"
                    );
                }
                let from_gdal = assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, None)
                    .await
                    .unwrap();
                let from_blocks =
                    assemble_tile::<u16>(plan, reader_mode, &block_worker, Some(&blocks))
                        .await
                        .unwrap();
                assert_eq!(from_blocks, from_gdal, "tile [{tile_y}, {tile_x}]");
            }
        }
        // every read of the block pass stored a unit: no tile fell back to a window read
        let stats = block_pool.request_stats();
        assert!(
            cache.stats().stored > 0,
            "the reads went through the blocks"
        );
        assert_eq!(stats.reads + stats.reads_joined, cache.stats().stored);
        assert_eq!(stats.headers, 1, "one header for the file");
    }

    #[tokio::test]
    async fn block_reads_equal_window_reads() {
        let dir = tempfile::tempdir().unwrap();
        let params = tiled_tiff_with_overviews(dir.path());
        let file_grid = params.spatial_grid_definition();
        let gpp = GdalProcessPool::new(
            2,
            2,
            2,
            DEFAULT_HEADER_CACHE_ENTRIES,
            WorkerConfig::default(),
        );
        let gdal_worker = gpp.get_gdal_worker();
        let cache = Arc::new(NewRasterCacheEnum::new_lru(100_000_000));
        let blocks = BlockIo::new(cache.clone(), gdal_worker.clone(), CacheTtlSeconds::new(60));

        for (overview_level, tile_size) in [(0_u32, 8_usize), (0, 12), (2, 8), (2, 12), (4, 12)] {
            let reader_mode = {
                if overview_level == 0 {
                    GdalReaderMode::OriginalResolution(ReaderState {
                        dataset_spatial_grid: file_grid,
                    })
                } else {
                    GdalReaderMode::OverviewLevel(OverviewReaderState {
                        original_dataset_grid: file_grid,
                        overview_level: std::num::NonZeroUsize::new(overview_level as usize)
                            .unwrap(),
                    })
                }
            };
            let pixel_size = f64::from(overview_level.max(1));
            let tiles_per_axis = (64 / overview_level.max(1) as usize).div_ceil(tile_size);

            for tile_y in 0..tiles_per_axis {
                for tile_x in 0..tiles_per_axis {
                    let plan = TilePlan {
                        tile: TileInformation::new(
                            [tile_y as isize, tile_x as isize].into(),
                            GridShape2D::new([tile_size, tile_size]),
                            GeoTransform::new_unchecked((0., 64.).into(), pixel_size, -pixel_size),
                        ),
                        time: TimeInterval::default(),
                        band: 0,
                        files: vec![params.clone()],
                        cache_hint: CacheHint::max_duration(),
                    };

                    // GDAL picks the overview itself; the sizes divide, so it reads the same pixels
                    let from_gdal =
                        assemble_tile::<u16>(plan.clone(), reader_mode, &gdal_worker, None)
                            .await
                            .unwrap();
                    let from_blocks =
                        assemble_tile::<u16>(plan, reader_mode, &gdal_worker, Some(&blocks))
                            .await
                            .unwrap();

                    let case = format!(
                        "overview {overview_level}, tile size {tile_size}, tile [{tile_y}, {tile_x}]"
                    );
                    assert_eq!(from_blocks, from_gdal, "{case}");
                }
            }
        }

        assert!(cache.byte_size() > 0, "the reads went through the blocks");
    }
}
