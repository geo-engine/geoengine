use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::{CacheHint, CacheTtlSeconds, TimeInterval},
    raster::{
        ChangeGridBounds, EmptyGrid, GridBoundingBox2D, GridOrEmpty, Pixel, RasterProperties,
        RasterTile2D, TileInformation,
    },
};
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use num::FromPrimitive;
use std::sync::Arc;
use tracing::{debug, trace};

use crate::cache::new_raster_cache::SupportedRasterDataType;
use crate::source::gdal_read::{BlockIo, MdSlice};
use crate::source::gdal_worker_process::reader::GdalPoolReader;
use crate::source::gdal_worker_process::{
    GdalPoolDispatcher, GdalProcessReadResult, GdalReaderMode,
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
pub async fn load_md_tile_from_files_async<
    T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType,
>(
    loading_info: &MdLoadingInfo,
    requests: &[MdTileRequest],
    reader_mode: GdalReaderMode,
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
/// the workers.
fn md_file_reads<'a, T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType>(
    loading_info: &'a MdLoadingInfo,
    request: &MdTileRequest,
    reader_mode: GdalReaderMode,
    tile_information: TileInformation,
    reader: &'a GdalPoolReader,
    blocks: Option<&'a BlockIo>,
    cache_hint: CacheHint,
) -> Vec<impl Future<Output = Result<GdalProcessReadResult<T>, MdGdalSourceError>> + 'a> {
    let file: &MdDatasetFile = &loading_info.files()[request.file_idx];
    let dataset_params = &file.params;
    let tile_spatial_grid = tile_information.spatial_grid_definition();

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
            async move {
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
