use futures::StreamExt;
use futures::stream::FuturesOrdered;
use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::{CacheHint, CacheTtlSeconds, TimeInterval},
    raster::{
        ChangeGridBounds, EmptyGrid, GeoTransform, GridBoundingBox2D, GridBounds, GridOrEmpty,
        GridShapeAccess, Pixel, RasterProperties, RasterTile2D, SpatialGridDefinition,
        TileInformation,
    },
};
use num::FromPrimitive;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tracing::{debug, trace};

use crate::cache::new_raster_cache::SupportedRasterDataType;
use crate::source::gdal_read::{BlockIo, MdSlice, Picked, pick_from_window};
use crate::source::gdal_worker_process::process_common::{GdalReadAdvise, GdalReadWindow};
use crate::source::gdal_worker_process::reader::GdalPoolReader;
use crate::source::gdal_worker_process::{
    GdalDatasetParameters, GdalPoolDispatcher, GdalProcessReadResult, GdalReaderMode,
};
use crate::source::md_gdal_source::{
    MdDatasetFile, MdGdalSourceError, MdLoadingInfo, MdPresentation,
};

/// One z-slice request: which file, and which file-local z index within it.
/// The Geo Engine band follows from the file (`MdDatasetFile::output_band`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MdTileRequest {
    /// index into [`MdLoadingInfo::files`]
    pub file_idx: usize,
    /// z index within that file, file-local
    pub local_z: usize,
}

/// Reads one z slice for one query tile: every file in `requests` that holds that slice is
/// blitted into the same tile, the way the spatial pieces of one time step are mosaicked.
/// A tile no file intersects comes back empty, never dropped: the stream must stay gap-free.
/// Returns `None` only for empty `requests`, which `MdLoadingInfo::z_slices` never produces.
/// Reads go through `blocks` if given and the cache hint allows it, like the reads of a
/// `MultiBandGdalSource`.
///
/// # Errors
/// Returns a `MdGdalSourceError` if a read window cannot be computed or a pool read fails.
#[allow(clippy::too_many_arguments)]
pub async fn load_md_tile_from_files_async<
    T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType,
>(
    loading_info: &MdLoadingInfo,
    requests: &[MdTileRequest],
    reader_mode: GdalReaderMode,
    level_factor: NonZeroUsize,
    tile_information: TileInformation,
    gdal_worker: &GdalPoolDispatcher,
    blocks: Option<&BlockIo>,
    default_cache_ttl: CacheTtlSeconds,
) -> Result<Option<RasterTile2D<T>>, MdGdalSourceError> {
    let cache_hint = loading_info.cache_hint(default_cache_ttl);
    let blocks = blocks.filter(|blocks| blocks.caches(cache_hint));
    let Some(first) = requests.first() else {
        return Ok(None);
    };
    let mut frame: GridOrEmpty<GridBoundingBox2D, T> =
        GridOrEmpty::from(EmptyGrid::new(tile_information.global_pixel_bounds()));
    let mut properties = RasterProperties::default();

    // All files and read windows of the slice are read at the same time and blitted in their
    // order, so where they overlap the later one still wins, as when read one after another.
    let reader = GdalPoolReader::from(gdal_worker.clone());
    let mut reads: FuturesOrdered<_> = requests
        .iter()
        .flat_map(|request| {
            md_file_reads::<T>(
                loading_info,
                request,
                reader_mode,
                level_factor,
                tile_information,
                &reader,
                blocks,
                cache_hint,
            )
        })
        .collect();
    while let Some(read) = reads.next().await {
        match read? {
            GdalProcessReadResult::Grid(grid_and_properties) => {
                frame.grid_blit_valid_only(&grid_and_properties.grid);
                properties = grid_and_properties.properties;
            }
            // leave the frame empty to signal no data
            GdalProcessReadResult::FileNotFoundAsNoData => {}
        }
    }

    let file = &loading_info.files()[first.file_idx];
    let global_z = file.z_start
        + first
            .local_z
            .checked_sub(file.local_offset)
            .expect("z_slices sets local_z to at least local_offset");
    Ok(Some(raster_tile_from_frame(
        file,
        tile_information,
        global_z,
        frame,
        properties,
        cache_hint,
        loading_info.time_steps(),
    )))
}

/// The reads of one file's windows for one z slice, none if the file does not intersect the
/// tile. Each read goes through `blocks` if they can serve it, else straight to the MD read of
/// the workers. A tile `level_factor` times coarser than the full resolution takes the pixel of
/// the full resolution under the center of each of its pixels, see [`LevelPick`].
#[allow(clippy::too_many_arguments)]
fn md_file_reads<'a, T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType>(
    loading_info: &'a MdLoadingInfo,
    request: &MdTileRequest,
    reader_mode: GdalReaderMode,
    level_factor: NonZeroUsize,
    tile_information: TileInformation,
    reader: &'a GdalPoolReader,
    blocks: Option<&'a BlockIo>,
    cache_hint: CacheHint,
) -> Vec<impl Future<Output = Result<GdalProcessReadResult<T>, MdGdalSourceError>> + 'a> {
    let file: &MdDatasetFile = &loading_info.files()[request.file_idx];
    let dataset_params = &file.params;
    let tile_spatial_grid = full_resolution_tile(&tile_information, level_factor);

    debug!(
        "loading md tile {:?} for z-index {} of array {}",
        tile_information.global_tile_position.inner(),
        request.local_z,
        file.array_name,
    );

    let advises = MdPresentation::from_wrap(loading_info.wrap()).read_advises(
        dataset_params,
        &tile_spatial_grid,
        reader_mode,
    );
    if advises.is_empty() {
        trace!("no read advise for tile, skipping.");
    }

    let blocks = blocks.filter(|blocks| blocks.reads(dataset_params));
    let slice = Arc::new(MdSlice {
        group: file.group.clone(),
        array_name: file.array_name.clone(),
        z_index: request.local_z,
        axis_mapping: loading_info.variable_of(file).axis_mapping.clone(),
    });

    advises
        .into_iter()
        .map(|advise| {
            let slice = Arc::clone(&slice);
            let pick = LevelPick::of(advise, level_factor);
            async move {
                if let Some(pick) = pick {
                    return read_picked::<T>(
                        dataset_params,
                        &slice,
                        &pick,
                        reader,
                        blocks,
                        cache_hint,
                    )
                    .await;
                }
                if let Some(blocks) = blocks
                    && let Some(read) = blocks
                        .read_md::<T>(dataset_params, &slice, advise, cache_hint)
                        .await?
                {
                    return Ok(read);
                }
                Ok(reader
                    .read_md_slice_data::<T>(
                        dataset_params.clone(),
                        advise,
                        slice.group.as_deref(),
                        &slice.array_name,
                        slice.z_index,
                        &slice.axis_mapping,
                    )
                    .await?)
            }
        })
        .collect()
}

/// Where the pixels of a tile `level_factor` times coarser than the full resolution are, as a tile
/// of the full resolution: the same area in pixels `level_factor` times smaller.
fn full_resolution_tile(
    tile_information: &TileInformation,
    level_factor: NonZeroUsize,
) -> SpatialGridDefinition {
    let tile = tile_information.spatial_grid_definition();
    if level_factor == NonZeroUsize::MIN {
        return tile;
    }
    let factor = level_factor.get() as isize;
    let geo_transform = tile.geo_transform();
    let bounds = tile.grid_bounds();
    SpatialGridDefinition::new(
        GeoTransform::new_unchecked(
            geo_transform.origin_coordinate,
            geo_transform.x_pixel_size().get() / factor as f64,
            geo_transform.y_pixel_size().get() / factor as f64,
        ),
        GridBoundingBox2D::new_unchecked(
            [bounds.y_min() * factor, bounds.x_min() * factor],
            [
                (bounds.y_max() + 1) * factor - 1,
                (bounds.x_max() + 1) * factor - 1,
            ],
        ),
    )
}

/// The read of a coarser tile from one read of the full resolution: each pixel of the tile takes
/// the pixel of the full resolution under its center, the nearest pixel as in a `Downsampling`.
struct LevelPick {
    /// the read of the full resolution that holds the picked pixels, in the pixels of the slice
    window: GdalReadAdvise,
    /// the picked pixels, in the pixels of the slice
    picked: Picked,
    /// where the picked pixels go in the tile
    read_advise: GdalReadAdvise,
}

impl LevelPick {
    /// The pick for `advise`, a read of the full resolution tile of a tile `level_factor` times
    /// coarser (see [`full_resolution_tile`]). `None` at the full resolution, or if no center of a
    /// pixel of the tile lies in the read.
    fn of(advise: GdalReadAdvise, level_factor: NonZeroUsize) -> Option<Self> {
        if level_factor == NonZeroUsize::MIN {
            return None;
        }
        let factor = level_factor.get() as isize;
        let target = advise.read_window_bounds;
        let (window_x, window_y) = advise.gdal_read_widow.gdal_window_start();
        // the pixels of the tile whose centers are in the read, and the pixels under the centers
        let centers = |min: isize, max: isize| {
            let first = (min - factor / 2 + factor - 1).div_euclid(factor);
            let last = (max - factor / 2).div_euclid(factor);
            (first <= last).then_some((first, last))
        };
        let (row_first, row_last) = centers(target.y_min(), target.y_max())?;
        let (column_first, column_last) = centers(target.x_min(), target.x_max())?;
        let center = |pixel: isize| pixel * factor + factor / 2;
        // a south-up slice is read top row last, see `flip_grid_y_if_needed`
        let mut rows: Vec<usize> = (row_first..=row_last)
            .map(|row| {
                let offset = if advise.flip_y {
                    target.y_max() - center(row)
                } else {
                    center(row) - target.y_min()
                };
                (window_y + offset) as usize
            })
            .collect();
        rows.sort_unstable();
        let columns: Vec<usize> = (column_first..=column_last)
            .map(|column| (window_x + center(column) - target.x_min()) as usize)
            .collect();

        let (first_row, last_row) = (rows[0] as isize, rows[rows.len() - 1] as isize);
        let (first_column, last_column) =
            (columns[0] as isize, columns[columns.len() - 1] as isize);
        let window_bounds =
            GridBoundingBox2D::new_min_max(first_row, last_row, first_column, last_column).ok()?;
        let tile_bounds =
            GridBoundingBox2D::new_min_max(row_first, row_last, column_first, column_last).ok()?;
        Some(Self {
            window: GdalReadAdvise {
                gdal_read_widow: GdalReadWindow::new(
                    window_bounds.min_index(),
                    window_bounds.grid_shape(),
                ),
                read_window_bounds: window_bounds,
                bounds_of_target: window_bounds,
                flip_y: false,
            },
            picked: Picked { rows, columns },
            read_advise: GdalReadAdvise {
                read_window_bounds: tile_bounds,
                bounds_of_target: tile_bounds,
                ..advise
            },
        })
    }
}

/// Reads `pick` through `blocks` if they can serve it, else its window straight from the workers.
async fn read_picked<T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType>(
    params: &GdalDatasetParameters,
    slice: &MdSlice,
    pick: &LevelPick,
    reader: &GdalPoolReader,
    blocks: Option<&BlockIo>,
    cache_hint: CacheHint,
) -> Result<GdalProcessReadResult<T>, MdGdalSourceError> {
    if let Some(blocks) = blocks
        && let Some(read) = blocks
            .read_md_picked::<T>(
                params,
                slice,
                pick.window,
                &pick.picked,
                pick.read_advise,
                cache_hint,
            )
            .await?
    {
        return Ok(read);
    }
    let window = reader
        .read_md_slice_data::<T>(
            params.clone(),
            pick.window,
            slice.group.as_deref(),
            &slice.array_name,
            slice.z_index,
            &slice.axis_mapping,
        )
        .await?;
    Ok(pick_from_window(
        params,
        window,
        &pick.picked,
        pick.read_advise,
    )?)
}

#[allow(clippy::too_many_arguments)]
fn raster_tile_from_frame<T: Pixel>(
    file: &MdDatasetFile,
    tile_information: TileInformation,
    global_z: usize,
    frame: GridOrEmpty<GridBoundingBox2D, T>,
    properties: RasterProperties,
    cache_hint: CacheHint,
    time_steps: &[TimeInterval],
) -> RasterTile2D<T> {
    RasterTile2D::new_with_properties(
        time_steps[global_z],
        tile_information.global_tile_position,
        file.output_band,
        tile_information.global_geo_transform,
        frame.unbounded(),
        properties,
        cache_hint,
    )
}
