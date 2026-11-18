use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::{CacheHint, CacheTtlSeconds, TimeInterval},
    raster::{
        ChangeGridBounds, EmptyGrid, GridBoundingBox2D, GridOrEmpty, Pixel, RasterProperties,
        RasterTile2D, TileInformation,
    },
};
use num::FromPrimitive;
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

    for request in requests {
        blit_md_file(
            loading_info,
            request,
            reader_mode,
            tile_information,
            gdal_worker,
            blocks,
            cache_hint,
            &mut frame,
            &mut properties,
        )
        .await?;
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

/// Blits one file's read windows for one z slice into `frame`. Leaves `frame` untouched if
/// the file does not intersect the tile.
#[allow(clippy::too_many_arguments)]
async fn blit_md_file<T: Pixel + GdalType + FromPrimitive + SupportedRasterDataType>(
    loading_info: &MdLoadingInfo,
    request: &MdTileRequest,
    reader_mode: GdalReaderMode,
    tile_information: TileInformation,
    gdal_worker: &GdalPoolDispatcher,
    blocks: Option<&BlockIo>,
    cache_hint: CacheHint,
    frame: &mut GridOrEmpty<GridBoundingBox2D, T>,
    properties: &mut RasterProperties,
) -> Result<(), MdGdalSourceError> {
    let file: &MdDatasetFile = &loading_info.files()[request.file_idx];
    let z_index = request.local_z;
    let dataset_params = file.params.clone();
    let array_name = file.array_name.clone();
    let axis_mapping = loading_info.variable_of(file).axis_mapping.as_slice();
    let tile_spatial_grid = tile_information.spatial_grid_definition();

    debug!(
        "loading md tile {:?} for z-index {z_index} of array {array_name}",
        tile_information.global_tile_position.inner(),
    );

    let advises = MdPresentation::from_wrap(loading_info.wrap()).read_advises(
        &dataset_params,
        &tile_spatial_grid,
        reader_mode,
    );
    if advises.is_empty() {
        trace!("no read advise for tile, skipping.");
        return Ok(());
    }

    let reader = GdalPoolReader::from(gdal_worker.clone());
    let blocks = blocks.filter(|blocks| blocks.reads(&dataset_params));
    let slice = MdSlice {
        group: file.group.clone(),
        array_name,
        z_index,
        axis_mapping: axis_mapping.to_vec(),
    };

    for advise in advises {
        let from_blocks = match blocks {
            Some(blocks) => {
                blocks
                    .read_md::<T>(&dataset_params, &slice, advise, cache_hint)
                    .await?
            }
            None => None,
        };
        let read = match from_blocks {
            Some(read) => read,
            None => {
                reader
                    .read_md_slice_data::<T>(
                        dataset_params.clone(),
                        advise,
                        slice.group.as_deref(),
                        &slice.array_name,
                        slice.z_index,
                        &slice.axis_mapping,
                    )
                    .await?
            }
        };
        match read {
            GdalProcessReadResult::Grid(grid_and_properties) => {
                frame.grid_blit_valid_only(&grid_and_properties.grid);
                *properties = grid_and_properties.properties;
            }
            // leave the frame empty to signal no data
            GdalProcessReadResult::FileNotFoundAsNoData => {}
        }
    }

    Ok(())
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
