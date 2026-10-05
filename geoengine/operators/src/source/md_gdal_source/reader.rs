use std::ops::Range;

use gdal::raster::GdalType;
use geoengine_datatypes::{
    primitives::{CacheHint, TimeInterval},
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
use crate::source::md_gdal_source::{MdDatasetFile, MdGdalSourceError, MdLoadingInfo, ZRole};

/// One z-slice batch request: which file, and which local z range within it.
/// The Geo Engine band follows from the file (`MdDatasetFile::output_band`).
#[derive(Debug, Clone)]
pub struct MdTileRequest {
    /// index into [`MdLoadingInfo::files`]
    pub file_idx: usize,
    /// slice range within that file, file-local and end-exclusive
    pub local_z: Range<usize>,
}

/// Reads one z-slice batch of one MD array file for one query tile and returns one
/// `RasterTile2D` per requested slice.
///
/// For wrapped (0..360° stored, −180..180° presented) datasets the read window is computed
/// directly in the tile lattice, because the presented frame relabels the stored longitudes
/// (see [`wrapped_splitted_advises`]).
///
/// # Errors
/// Returns a `MdGdalSourceError` if the read window cannot be computed or the pool read fails.
#[allow(clippy::too_many_arguments)]
pub async fn load_md_tile_from_files_async<T: Pixel + GdalType + FromPrimitive>(
    loading_info: &MdLoadingInfo,
    request: &MdTileRequest,
    reader_mode: GdalReaderMode,
    tile_information: TileInformation,
    gdal_worker: &GdalPoolDispatcher,
) -> Result<Vec<RasterTile2D<T>>, MdGdalSourceError> {
    let file: &MdDatasetFile = &loading_info.files()[request.file_idx];
    let z_range = request.local_z.clone();
    let dataset_params = file.params.clone();
    let array_name = file.array_name.clone();
    let ds_spatial_grid = dataset_params.spatial_grid_definition();
    let tile_spatial_grid = tile_information.spatial_grid_definition();

    debug!(
        "loading md tile {:?} for z-range {:?} of array {array_name}",
        tile_information.global_tile_position.inner(),
        z_range,
    );

    let advises = if loading_info.wrap() {
        wrapped_splitted_advises(&dataset_params, &tile_spatial_grid)?
    } else {
        // The read advise is the tile/dataset intersection in the dataset's own frame;
        // the overlap is then read from the stored array.
        match reader_mode.tiling_to_dataset_read_advise(&ds_spatial_grid, &tile_spatial_grid) {
            Some(advise) => vec![advise],
            None => {
                trace!("no read advise for tile, skipping.",);
                return Ok(vec![]);
            }
        }
    };

    let number_of_z_slices = z_range.len();
    let mut frames: Vec<GridOrEmpty<GridBoundingBox2D, T>> =
        vec![
            GridOrEmpty::from(EmptyGrid::new(tile_information.global_pixel_bounds()));
            number_of_z_slices
        ];
    let mut properties = RasterProperties::default();

    let reader =
        crate::source::gdal_worker_process::reader::GdalPoolReader::from(gdal_worker.clone());

    for advise in advises {
        match reader
            .read_md_batch_data::<T>(
                dataset_params.clone(),
                advise,
                file.group.as_deref(),
                &array_name,
                z_range.clone(),
            )
            .await?
        {
            crate::source::gdal_worker_process::reader::GdalProcessMdReadResult::Grids(grids) => {
                for (k, grid_and_properties) in grids.into_iter().enumerate() {
                    frames[k].grid_blit_valid_only(&grid_and_properties.grid);
                    properties = grid_and_properties.properties;
                }
            }
            crate::source::gdal_worker_process::reader::GdalProcessMdReadResult::FileNotFoundAsNoData => {
                // leave the frames empty to signal no data
            }
        }
    }

    let cache_hint = loading_info.cache_hint();

    Ok(frames
        .into_iter()
        .enumerate()
        .map(|(k, frame)| {
            let global_z = file.z_start + (z_range.start - file.local_offset) + k;
            raster_tile_from_frame(
                file,
                tile_information,
                global_z,
                frame,
                properties.clone(),
                cache_hint,
                loading_info.z_role(),
                loading_info.time_steps(),
            )
        })
        .collect())
}

/// Computes the read windows for a wrapped (0..360° stored, −180..180° presented) dataset.
///
/// The tile lattice is the dataset lattice re-anchored to the global origin, so the stored
/// array occupies exact tile-lattice columns `[col0, col0 + width)` and rows
/// `[row0, row0 + height)`. The wrapped presentation covers the tile-lattice columns
/// `[col0 − half_width, col0 + half_width)` (world lon −180..180), where a tile-lattice
/// column `c` holds stored column `(c − col0) mod width`. A tile crossing the seam column
/// `col0` (world longitude 0°) is split into two contiguous stored runs.
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
    z_role: ZRole,
    time_steps: &[TimeInterval],
) -> RasterTile2D<T> {
    let (time, band) = match z_role {
        // Band-role intervals are synthetic `[k, k+1)` unit steps indexed by band, so the
        // tile stamp is `time_steps[global_z]` -- the same rule `empty_tile` uses, which is
        // what keeps gap tiles and data tiles in one query consistent
        ZRole::Band => (time_steps[global_z], global_z as u32),
        ZRole::Variable => (time_steps[global_z], file.output_band),
    };

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
