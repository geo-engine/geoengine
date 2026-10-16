use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::{CacheHint, CacheTtlSeconds, TimeInterval},
    raster::{
        ChangeGridBounds, EmptyGrid, GridBoundingBox2D, GridBounds, GridOrEmpty, Pixel,
        RasterProperties, RasterTile2D, SpatialGridDefinition, TileInformation,
    },
};
use num::FromPrimitive;
use tracing::{debug, trace};

use crate::source::gdal_worker_process::{
    GdalPoolDispatcher, GdalReaderMode,
    process_common::{GdalReadAdvise, GdalReadWindow},
};
use crate::source::md_gdal_source::loading_info::time_and_band;
use crate::source::md_gdal_source::{MdDatasetFile, MdGdalSourceError, MdLoadingInfo};

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
/// Returns `None` if no file intersects the tile.
///
/// # Errors
/// Returns a `MdGdalSourceError` if a read window cannot be computed or a pool read fails.
pub async fn load_md_tile_from_files_async<T: Pixel + GdalType + FromPrimitive>(
    loading_info: &MdLoadingInfo,
    requests: &[MdTileRequest],
    reader_mode: GdalReaderMode,
    tile_information: TileInformation,
    gdal_worker: &GdalPoolDispatcher,
    default_cache_ttl: CacheTtlSeconds,
) -> Result<Option<RasterTile2D<T>>, MdGdalSourceError> {
    let Some(first) = requests.first() else {
        return Ok(None);
    };
    let mut frame: GridOrEmpty<GridBoundingBox2D, T> =
        GridOrEmpty::from(EmptyGrid::new(tile_information.global_pixel_bounds()));
    let mut properties = RasterProperties::default();

    let mut intersects = false;
    for request in requests {
        intersects |= blit_md_file(
            loading_info,
            request,
            reader_mode,
            tile_information,
            gdal_worker,
            &mut frame,
            &mut properties,
        )
        .await?;
    }
    if !intersects {
        return Ok(None);
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
        loading_info.cache_hint(default_cache_ttl),
        loading_info.z_is_band(),
        loading_info.time_steps(),
    )))
}

/// Blits one file's read windows for one z slice into `frame`. Returns `false` if the file
/// does not intersect the tile.
///
/// For wrapped (0..360° stored, −180..180° presented) datasets the read window is computed
/// directly in the tile lattice, because the presented frame relabels the stored longitudes
/// (see [`wrapped_splitted_advises`]).
#[allow(clippy::too_many_arguments)]
async fn blit_md_file<T: Pixel + GdalType + FromPrimitive>(
    loading_info: &MdLoadingInfo,
    request: &MdTileRequest,
    reader_mode: GdalReaderMode,
    tile_information: TileInformation,
    gdal_worker: &GdalPoolDispatcher,
    frame: &mut GridOrEmpty<GridBoundingBox2D, T>,
    properties: &mut RasterProperties,
) -> Result<bool, MdGdalSourceError> {
    let file: &MdDatasetFile = &loading_info.files()[request.file_idx];
    let z_index = request.local_z;
    let dataset_params = file.params.clone();
    let array_name = file.array_name.clone();
    let axis_mapping = loading_info.variable_of(file).axis_mapping.as_slice();
    let ds_spatial_grid = dataset_params.spatial_grid_definition();
    let tile_spatial_grid = tile_information.spatial_grid_definition();

    debug!(
        "loading md tile {:?} for z-index {z_index} of array {array_name}",
        tile_information.global_tile_position.inner(),
    );

    let advises = if loading_info.wrap() {
        let advises = wrapped_splitted_advises(&dataset_params, &tile_spatial_grid)?;
        // Same contract as the non-wrap arm below: a tile that does not reach the presented
        // extent yields no tile at all. Without this the empty `advises` fell through to the
        // `EmptyGrid` frames below and emitted a real, fully empty tile, so a query reaching
        // past the extent (e.g. below the southernmost stored latitude) returned a full
        // rectangle of empty tiles where `GdalSource` would return none.
        if advises.is_empty() {
            trace!("no read advise for tile, skipping.");
            return Ok(false);
        }
        advises
    } else {
        // The read advise is the tile/dataset intersection in the dataset's own frame;
        // the overlap is then read from the stored array.
        match reader_mode.tiling_to_dataset_read_advise(&ds_spatial_grid, &tile_spatial_grid) {
            Some(advise) => vec![advise],
            None => {
                trace!("no read advise for tile, skipping.",);
                return Ok(false);
            }
        }
    };

    let reader =
        crate::source::gdal_worker_process::reader::GdalPoolReader::from(gdal_worker.clone());

    for advise in advises {
        match reader
            .read_md_slice_data::<T>(
                dataset_params.clone(),
                advise,
                file.group.as_deref(),
                &array_name,
                z_index,
                axis_mapping,
            )
            .await?
        {
            crate::source::gdal_worker_process::reader::GdalProcessMdReadResult::Grid(grid_and_properties) => {
                frame.grid_blit_valid_only(&grid_and_properties.grid);
                *properties = grid_and_properties.properties;
            }
            crate::source::gdal_worker_process::reader::GdalProcessMdReadResult::FileNotFoundAsNoData => {
                // leave the frame empty to signal no data
            }
        }
    }

    Ok(true)
}

/// Computes the read windows for a wrapped (0..360° stored, −180..180° presented) dataset.
///
/// The tile lattice is the dataset lattice re-anchored to the global origin, so the stored
/// array occupies exact tile-lattice columns `[col0, col0 + width)` and rows
/// `[row0, row0 + height)`. The wrapped presentation covers the tile-lattice columns
/// `[col0 − half_width, col0 + half_width)` (world lon −180..180), where a tile-lattice
/// column `c` holds stored column `(c − col0) mod width`. A tile crossing the seam column
/// `col0` (world longitude 0°) is split into two contiguous stored runs.
///
/// shortcut: `col0` is 0 in practice, so the seam split is only reached with a narrow synthetic tile (`wrap_tile_straddling_seam_splits`), upgrade when a wrapped file stores a non-zero raw origin: detect the coverage from the array's coordinates
fn wrapped_splitted_advises(
    dataset_params: &crate::source::gdal_worker_process::GdalDatasetParameters,
    tile_spatial_grid: &SpatialGridDefinition,
) -> Result<Vec<GdalReadAdvise>, MdGdalSourceError> {
    let gt = dataset_params.geo_transform;
    let width = dataset_params.width as isize;
    let height = dataset_params.height as isize;
    let half_width = width / 2;
    if width % 2 != 0 {
        return Err(MdGdalSourceError::UnsupportedBandRequest {
            message: format!("wrap-around MD arrays must have an even width, found {width}"),
        });
    }
    let tile_origin = tile_spatial_grid.geo_transform();
    let tile_bounds = tile_spatial_grid.grid_bounds();

    // offsets of the stored array in tile-lattice indices; exact because both grids
    // share the same pixel lattice (the tiling grid is the dataset grid, re-anchored)
    let col0 = ((gt.origin_coordinate.x - tile_origin.origin_coordinate.x) / gt.x_pixel_size)
        .round() as isize;
    let ady = gt.y_pixel_size.abs();
    let top_edge = if gt.y_pixel_size < 0.0 {
        gt.origin_coordinate.y
    } else {
        gt.origin_coordinate.y + height as f64 * ady
    };
    let row0 = ((tile_origin.origin_coordinate.y - top_edge) / ady).round() as isize;

    let t_min = tile_bounds.min_index();
    let t_max = tile_bounds.max_index();
    let q0 = t_min.x().max(col0 - half_width);
    let q1 = t_max.x().min(col0 + half_width - 1);
    let r0 = t_min.y().max(row0);
    let r1 = t_max.y().min(row0 + height - 1);
    if q0 > q1 || r0 > r1 {
        return Ok(vec![]);
    }

    let size_y = (r1 - r0 + 1) as usize;
    // stored rows in raw array order; `flip_y` lets the pool turn them back north-up
    let (start_y, flip_y) = if gt.y_pixel_size < 0.0 {
        (r0 - row0, false)
    } else {
        (height - 1 - (r1 - row0), true)
    };

    let make_run = |a: isize, b: isize| GdalReadAdvise {
        gdal_read_widow: GdalReadWindow {
            start_x: (a - col0).rem_euclid(width),
            start_y,
            size_x: (b - a + 1) as usize,
            size_y,
        },
        read_window_bounds: GridBoundingBox2D::new_unchecked([r0, a], [r1, b]),
        bounds_of_target: tile_bounds,
        flip_y,
    };

    Ok(if q0 < col0 && col0 <= q1 {
        vec![make_run(q0, col0 - 1), make_run(col0, q1)]
    } else {
        vec![make_run(q0, q1)]
    })
}

#[allow(clippy::too_many_arguments)]
fn raster_tile_from_frame<T: Pixel>(
    file: &MdDatasetFile,
    tile_information: TileInformation,
    global_z: usize,
    frame: GridOrEmpty<GridBoundingBox2D, T>,
    properties: RasterProperties,
    cache_hint: CacheHint,
    z_is_band: bool,
    time_steps: &[TimeInterval],
) -> RasterTile2D<T> {
    let (time, band) = time_and_band(z_is_band, global_z, file.output_band, time_steps);

    RasterTile2D::new_with_properties(
        time,
        tile_information.global_tile_position,
        band,
        tile_information.global_geo_transform,
        frame.unbounded(),
        properties,
        cache_hint,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::GdalDatasetGeoTransform;
    use crate::source::gdal_worker_process::{FileNotFoundHandling, GdalDatasetParameters};
    use geoengine_datatypes::raster::GeoTransform;

    fn params(
        origin: (f64, f64),
        pixel: (f64, f64),
        width: usize,
        height: usize,
    ) -> GdalDatasetParameters {
        GdalDatasetParameters {
            file_path: "/test.nc".into(),
            rasterband_channel: 1,
            geo_transform: GdalDatasetGeoTransform {
                origin_coordinate: origin.into(),
                x_pixel_size: pixel.0,
                y_pixel_size: pixel.1,
            },
            width,
            height,
            file_not_found_handling: FileNotFoundHandling::NoData,
            no_data_value: None,
            properties_mapping: None,
            gdal_open_options: None,
            gdal_config_options: None,
            allow_alphaband_as_mask: false,
            retry: None,
        }
    }

    fn tile(pixel_size: f64, bounds: ([isize; 2], [isize; 2])) -> SpatialGridDefinition {
        SpatialGridDefinition::new(
            GeoTransform::new((0., 0.).into(), pixel_size, -pixel_size),
            GridBoundingBox2D::new_unchecked(bounds.0, bounds.1),
        )
    }

    /// the 0..360° fixture grid: stored lon 0..360, lat 45..35, presented −180..180
    fn wrap_params() -> GdalDatasetParameters {
        params((0., 45.), (1., -1.), 360, 10)
    }

    #[test]
    fn wrap_tile_west_of_seam() {
        // tile (−1, −1): covers world cols −512..−1 -> presented cols −180..−1 = stored 180..359
        let advises =
            wrapped_splitted_advises(&wrap_params(), &tile(1.0, ([-512, -512], [-1, -1]))).unwrap();
        assert_eq!(advises.len(), 1);
        let w = advises[0].gdal_read_widow;
        assert_eq!((w.start_x, w.size_x), (180, 180));
        assert_eq!((w.start_y, w.size_y), (0, 10));
        assert_eq!(advises[0].read_window_bounds.min_index().x(), -180);
        assert_eq!(advises[0].read_window_bounds.max_index().x(), -1);
        assert!(!advises[0].flip_y);
    }

    #[test]
    fn wrap_tile_east_of_seam() {
        // tile (0, −1): covers world cols 0..511 -> presented cols 0..179 = stored 0..179
        let advises =
            wrapped_splitted_advises(&wrap_params(), &tile(1.0, ([-512, 0], [-1, 511]))).unwrap();
        assert_eq!(advises.len(), 1);
        let w = advises[0].gdal_read_widow;
        assert_eq!((w.start_x, w.size_x), (0, 180));
        assert_eq!((w.start_y, w.size_y), (0, 10));
        assert_eq!(advises[0].read_window_bounds.min_index().x(), 0);
        assert_eq!(advises[0].read_window_bounds.max_index().x(), 179);
    }

    #[test]
    fn wrap_tile_straddling_seam_splits() {
        // a narrow tile crossing world lon 0° splits into two stored runs
        let advises =
            wrapped_splitted_advises(&wrap_params(), &tile(1.0, ([-45, -10], [-36, 10]))).unwrap();
        assert_eq!(advises.len(), 2);
        assert_eq!(
            (
                advises[0].gdal_read_widow.start_x,
                advises[0].gdal_read_widow.size_x
            ),
            (350, 10)
        );
        assert_eq!(
            (
                advises[1].gdal_read_widow.start_x,
                advises[1].gdal_read_widow.size_x
            ),
            (0, 11)
        );
        assert_eq!(advises[1].read_window_bounds.min_index().x(), 0);
        assert_eq!(advises[1].read_window_bounds.max_index().x(), 10);
    }

    #[test]
    fn wrap_tile_outside_presented_extent_is_empty() {
        let advises =
            wrapped_splitted_advises(&wrap_params(), &tile(1.0, ([-45, 200], [-36, 300]))).unwrap();
        assert!(advises.is_empty());
    }

    #[test]
    fn wrap_ascending_y_reads_array_rows_and_flips() {
        // ascending latitudes (row 0 = south), 0..360 stored grid: edges lon 0..360, lat -5..5
        let p = params((0., -5.), (1., 1.), 360, 10);
        let tile_grid = SpatialGridDefinition::new(
            GeoTransform::new((0., 0.).into(), 1.0, -1.0),
            GridBoundingBox2D::new_unchecked([-512, 0], [-1, 511]),
        );
        let advises = wrapped_splitted_advises(&p, &tile_grid).unwrap();
        assert_eq!(advises.len(), 1);
        let w = advises[0].gdal_read_widow;
        // world cols 0..179 = stored 0..179; presented rows -5..-1 are array rows 5..9
        // (array row 0 = south) -> read in array order and flip back
        assert_eq!((w.start_x, w.size_x), (0, 180));
        assert_eq!((w.start_y, w.size_y), (5, 5));
        assert!(advises[0].flip_y);
        assert_eq!(advises[0].read_window_bounds.min_index().x(), 0);
        assert_eq!(advises[0].read_window_bounds.max_index().x(), 179);
        assert_eq!(advises[0].read_window_bounds.min_index().y(), -5);
        assert_eq!(advises[0].read_window_bounds.max_index().y(), -1);
    }

    #[test]
    fn wrap_odd_width_errors() {
        let p = params((0., 45.), (1., -1.), 361, 10);
        let err = wrapped_splitted_advises(&p, &tile(1.0, ([-512, -512], [-1, -1]))).unwrap_err();
        assert!(matches!(
            err,
            MdGdalSourceError::UnsupportedBandRequest { .. }
        ));
    }
}
