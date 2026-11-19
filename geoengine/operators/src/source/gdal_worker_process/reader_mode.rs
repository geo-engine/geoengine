use geoengine_datatypes::raster::{
    GeoTransform, GridBoundingBox2D, GridBounds, GridContains, GridIdx2D, GridShapeAccess,
    GridSize, SpatialGridDefinition,
};
use std::num::NonZeroUsize;
use tracing::trace;

use crate::source::gdal_worker_process::process_common::{GdalReadAdvise, GdalReadWindow};

#[derive(Copy, Clone, Debug)]
pub struct ReaderState {
    pub dataset_spatial_grid: SpatialGridDefinition,
}

impl ReaderState {
    /// Returns the read advise for the tiling based bounds.
    pub fn tiling_to_dataset_read_advise(
        &self,
        actual_gdal_dataset_spatial_grid_definition: &SpatialGridDefinition,
        tile: &SpatialGridDefinition,
    ) -> Option<GdalReadAdvise> {
        // Check if the y_axis is flipped.
        let (actual_gdal_dataset_spatial_grid_definition, flip_y) =
            if actual_gdal_dataset_spatial_grid_definition
                .geo_transform()
                .y_axis_is_neg()
                == self.dataset_spatial_grid.geo_transform().y_axis_is_neg()
            {
                (*actual_gdal_dataset_spatial_grid_definition, false)
            } else {
                (
                    actual_gdal_dataset_spatial_grid_definition
                        .flip_axis_y() // first: reverse the coordinate system to match the one used by tiling
                        .shift_bounds_relative_by_pixel_offset(GridIdx2D::new_y_x(
                            // second: move the origin to the other end of the y-axis
                            actual_gdal_dataset_spatial_grid_definition
                                .grid_bounds()
                                .axis_size_y() as isize,
                            0,
                        )),
                    true,
                )
            };

        // Now we can work with a matching dataset. However, we need to reverse the read window later!

        // let's only look at data in the geo engine dataset definition! The intersection is relative to the first elements origin coordinate.
        let dataset_gdal_data_intersection =
            actual_gdal_dataset_spatial_grid_definition.intersection(&self.dataset_spatial_grid)?;

        // Now, we need the tile in the gdal dataset bounds to identify readable areas
        let tile_in_gdal_dataset_bounds = tile.with_moved_origin_exact_grid(
            actual_gdal_dataset_spatial_grid_definition
                .geo_transform()
                .origin_coordinate,
        )?; // TODO: raise error if this fails!

        // Then, calculate the intersection between the datataset and the tile. Again, the intersection is relative to the first elements orrigin coordinate.
        let tile_gdal_dataset_intersection =
            dataset_gdal_data_intersection.intersection(&tile_in_gdal_dataset_bounds)?;

        // if we need to unflip the dataset grid now is the time to do this.
        let tile_intersection_for_read_window = if flip_y {
            tile_gdal_dataset_intersection
                .flip_axis_y() // first: reverse the coordinate system to match the one used by tiling
                .shift_bounds_relative_by_pixel_offset(GridIdx2D::new_y_x(
                    // second: move the origin to the other end of the y-axis
                    actual_gdal_dataset_spatial_grid_definition
                        .grid_bounds()
                        .axis_size_y() as isize,
                    0,
                ))
        } else {
            tile_gdal_dataset_intersection
        };

        // generate the read window for GDAL

        let gdal_read_window = GdalReadWindow::new(
            tile_intersection_for_read_window.grid_bounds.min_index(),
            tile_intersection_for_read_window.grid_bounds.grid_shape(),
        );

        // if the read window has the same shape as the tiling based bounds we can fill that completely
        if tile_in_gdal_dataset_bounds == tile_gdal_dataset_intersection {
            return Some(GdalReadAdvise {
                gdal_read_widow: gdal_read_window,
                read_window_bounds: tile.grid_bounds,
                bounds_of_target: tile.grid_bounds,
                flip_y,
            });
        }

        // we need to crop the window to the intersection of the tiling based bounds and the dataset bounds
        let crop_tl =
            tile_gdal_dataset_intersection.min_index() - tile_in_gdal_dataset_bounds.min_index();
        let crop_lr =
            tile_gdal_dataset_intersection.max_index() - tile_in_gdal_dataset_bounds.max_index();

        let shifted_tl = tile.grid_bounds.min_index() + crop_tl;
        let shifted_lr = tile.grid_bounds.max_index() + crop_lr;

        // now we need to adapt the target pixel space read window to the clipped dataset intersection area
        let shifted_readable_bounds = GridBoundingBox2D::new_unchecked(shifted_tl, shifted_lr);
        debug_assert!(
            tile.grid_bounds().contains(&shifted_readable_bounds),
            "readable bounds must be contained in tile bounds"
        );

        Some(GdalReadAdvise {
            gdal_read_widow: gdal_read_window,
            read_window_bounds: shifted_readable_bounds,
            bounds_of_target: tile.grid_bounds,
            flip_y,
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub enum GdalReaderMode {
    OriginalResolution(ReaderState),
    OverviewLevel(OverviewReaderState),
}

impl GdalReaderMode {
    pub fn tiling_to_dataset_read_advise(
        &self,
        actual_gdal_dataset_spatial_grid_definition: &SpatialGridDefinition,
        tile: &SpatialGridDefinition,
    ) -> Option<GdalReadAdvise> {
        match self {
            GdalReaderMode::OriginalResolution(re) => {
                re.tiling_to_dataset_read_advise(actual_gdal_dataset_spatial_grid_definition, tile)
            }
            GdalReaderMode::OverviewLevel(rs) => {
                rs.tiling_to_dataset_read_advise(actual_gdal_dataset_spatial_grid_definition, tile)
            }
        }
    }

    /// How many times coarser than the dataset's native resolution a file is, if it is coarser.
    ///
    /// Metadata returns such files for overview levels, to read less data. GDAL resamples them
    /// to the tile: a tile with such a file is read through GDAL as a whole, not through blocks.
    pub fn coarser_file_factor(&self, file: &SpatialGridDefinition) -> Option<NonZeroUsize> {
        let native = match self {
            GdalReaderMode::OriginalResolution(rs) => rs.dataset_spatial_grid,
            GdalReaderMode::OverviewLevel(rs) => rs.original_dataset_grid,
        };
        coarser_file_factor(&native, file).filter(|factor| factor.get() > 1)
    }

    /// How many times coarser than the full resolution the reads are: 1 at the full resolution.
    pub fn level_factor(&self) -> NonZeroUsize {
        match self {
            GdalReaderMode::OriginalResolution(_) => NonZeroUsize::MIN,
            GdalReaderMode::OverviewLevel(rs) => rs.overview_level,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct OverviewReaderState {
    pub original_dataset_grid: SpatialGridDefinition,
    /// How many times coarser than the full resolution the read level is, 2 or more: the
    /// sources read levels 0 and 1 in [`GdalReaderMode::OriginalResolution`].
    pub overview_level: NonZeroUsize,
}

impl OverviewReaderState {
    /// Compute the `ReadAdvise` for a given input gdal dataset (file) and output tile, i.e., which pixels we need to read and where to put them in the output tile.
    ///
    /// There are some special cases here, because while the grid of the input file must be aligned with the output tile,
    /// the overviews of (a) the geo engine raster and (b) the indiviual file at hand, must not necessarily be aligned.
    /// This is, e.g., the case if the file's origin distance from the geo engine datasets's global origin is not a multiple of 2.
    /// In that case, for building a pixel in the overview we would need to take one pixel from one file, and another pixel from a different file.
    /// Currently, we do not do this, however we need to correctly calculate the read window for the overview that
    /// (1) reads from the file s.t. the existing overviews (pyramid levels) can be used
    /// (2) all pixels in the output tile that can be filled, are filled
    pub fn tiling_to_dataset_read_advise(
        &self,
        actual_gdal_dataset_spatial_grid_definition: &SpatialGridDefinition, // This is the spatial grid of an actual gdal file
        tile: &SpatialGridDefinition, // This is a tile inside the grid we use for the global dataset consisting of potentially many gdal files...
    ) -> Option<GdalReadAdvise> {
        let (native_file_grid, coarseness) =
            self.native_file_grid(actual_gdal_dataset_spatial_grid_definition)?;
        let actual_gdal_dataset_spatial_grid_definition = &native_file_grid;

        // Check if the y_axis is fliped.
        let (file_grid, flip_y) =
            self.normalize_y_axis(actual_gdal_dataset_spatial_grid_definition);

        let file_geo_transform = file_grid.geo_transform();
        let overview_level = self.overview_level.get() as isize;

        debug_assert!(
            float_cmp::approx_eq!(
                f64,
                tile.geo_transform().x_pixel_size().get(),
                file_geo_transform.x_pixel_size().get() * overview_level as f64
            ) && float_cmp::approx_eq!(
                f64,
                tile.geo_transform().y_pixel_size().get(),
                file_geo_transform.y_pixel_size().get() * overview_level as f64
            ),
            "tile pixel size must be the file pixel size times the overview level"
        );

        // Only the part of the file that belongs to the geo engine dataset is readable.
        // The intersection uses the geo transform of the file.
        let Some(readable) = file_grid.intersection(&self.original_dataset_grid) else {
            trace!(
                "no intersection between dataset grid {:?} and file grid {:?}, skipping read advise generation",
                self.original_dataset_grid, file_grid
            );
            return None;
        };
        let readable_min = readable.min_index() - file_grid.min_index();
        let readable_max = readable.max_index() - file_grid.min_index();

        // distance from the tile grid's origin to the file's first pixel in original resolution pixels
        let file_upper_left =
            file_geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(file_grid.min_index());
        let tile_origin = tile.geo_transform().origin_coordinate();
        let file_offset_x = ((file_upper_left.x - tile_origin.x)
            / file_geo_transform.x_pixel_size().get())
        .round() as isize;
        let file_offset_y = ((file_upper_left.y - tile_origin.y)
            / file_geo_transform.y_pixel_size().get())
        .round() as isize;

        let file_size_x = file_grid.grid_bounds().axis_size_x() as isize;
        let file_size_y = file_grid.grid_bounds().axis_size_y() as isize;

        // GDAL computes the overviews from the first row of the actual file. If the y-axis is flipped,
        // that is the last row of the normalized file, so the overview pixel edges are shifted.
        let phase_y = if flip_y {
            file_size_y.rem_euclid(overview_level)
        } else {
            0
        };

        let Some(axis_x) = OverviewAxisReadAdvise::new(
            file_offset_x,
            file_size_x,
            [readable_min.x(), readable_max.x()],
            0,
            overview_level,
            tile.grid_bounds().x_bounds(),
        ) else {
            trace!("tile {tile:?} does not intersect file {file_grid:?} in x direction");
            return None;
        };

        let Some(axis_y) = OverviewAxisReadAdvise::new(
            file_offset_y,
            file_size_y,
            [readable_min.y(), readable_max.y()],
            phase_y,
            overview_level,
            tile.grid_bounds().y_bounds(),
        ) else {
            trace!("tile {tile:?} does not intersect file {file_grid:?} in y direction");
            return None;
        };

        // the read window refers to the actual file, so we need to reverse the y-axis if it is flipped
        let window_start_y = if flip_y {
            file_size_y - (axis_y.window_start + axis_y.window_size as isize)
        } else {
            axis_y.window_start
        };

        let read_window = coarsen_read_window(
            GdalReadWindow {
                start_x: axis_x.window_start,
                start_y: window_start_y,
                size_x: axis_x.window_size,
                size_y: axis_y.window_size,
            },
            coarseness,
        );

        let read_window_bounds = GridBoundingBox2D::new_unchecked(
            [axis_y.target_bounds[0], axis_x.target_bounds[0]],
            [axis_y.target_bounds[1], axis_x.target_bounds[1]],
        );

        debug_assert!(
            tile.grid_bounds().contains(&read_window_bounds),
            "readable bounds must be contained in tile bounds"
        );

        Some(GdalReadAdvise {
            gdal_read_widow: read_window,
            read_window_bounds,
            bounds_of_target: tile.grid_bounds,
            flip_y,
        })
    }

    /// A file coarser than the dataset is treated as a file of the dataset's resolution: the
    /// read window is computed in the dataset's pixels and then rounded to the file's pixels.
    /// Returns the file's grid in the dataset's resolution and how many times coarser the file is.
    fn native_file_grid(
        &self,
        file: &SpatialGridDefinition,
    ) -> Option<(SpatialGridDefinition, NonZeroUsize)> {
        let Some(coarseness) = coarser_file_factor(&self.original_dataset_grid, file) else {
            trace!(
                "file grid {file:?} is not an integer multiple of dataset grid {:?}, skipping it",
                self.original_dataset_grid
            );
            return None;
        };
        if coarseness.get() == 1 {
            return Some((*file, coarseness));
        }
        Some((refine_grid(file, coarseness)?, coarseness))
    }

    fn normalize_y_axis(
        &self,
        actual_gdal_dataset_spatial_grid_definition: &SpatialGridDefinition,
    ) -> (SpatialGridDefinition, bool) {
        if actual_gdal_dataset_spatial_grid_definition
            .geo_transform()
            .y_axis_is_neg()
            == self.original_dataset_grid.geo_transform().y_axis_is_neg()
        {
            (*actual_gdal_dataset_spatial_grid_definition, false)
        } else {
            (
                actual_gdal_dataset_spatial_grid_definition
                    .flip_axis_y() // first: reverse the coordinate system to match the one used by tiling
                    .shift_bounds_relative_by_pixel_offset(GridIdx2D::new_y_x(
                        // second: move the origin to the other end of the y-axis
                        actual_gdal_dataset_spatial_grid_definition
                            .grid_bounds()
                            .axis_size_y() as isize,
                        0,
                    )),
                true,
            )
        }
    }
}

/// How many times coarser than `native` the pixels of `file` are, if that is an integer.
/// Both axes must have the same factor.
fn coarser_file_factor(
    native: &SpatialGridDefinition,
    file: &SpatialGridDefinition,
) -> Option<NonZeroUsize> {
    let factor = file
        .geo_transform()
        .spatial_resolution()
        .integer_multiple_of(native.geo_transform().spatial_resolution())?;
    NonZeroUsize::new(factor.get() as usize)
}

/// The grid that covers the same area as `grid` with `factor` times finer pixels.
fn refine_grid(
    grid: &SpatialGridDefinition,
    factor: NonZeroUsize,
) -> Option<SpatialGridDefinition> {
    let factor_f64 = factor.get() as f64;
    let factor = factor.get() as isize;
    let geo_transform = grid.geo_transform();
    let refined_geo_transform = GeoTransform::new(
        geo_transform.origin_coordinate(),
        geo_transform.x_pixel_size().get() / factor_f64,
        geo_transform.y_pixel_size().get() / factor_f64,
    )
    .ok()?;
    let bounds = grid.grid_bounds();
    let refined_bounds = GridBoundingBox2D::new(
        [bounds.y_min() * factor, bounds.x_min() * factor],
        [
            (bounds.y_max() + 1) * factor - 1,
            (bounds.x_max() + 1) * factor - 1,
        ],
    )
    .ok()?;
    Some(SpatialGridDefinition::new(
        refined_geo_transform,
        refined_bounds,
    ))
}

/// Turns a read window in pixels `factor` times finer than the file into the file's pixels.
/// The window grows to whole file pixels, so its edges move by less than one file pixel.
fn coarsen_read_window(window: GdalReadWindow, factor: NonZeroUsize) -> GdalReadWindow {
    if factor.get() == 1 {
        return window;
    }
    let factor_isize = factor.get() as isize;
    let start_x = window.start_x.div_euclid(factor_isize);
    let start_y = window.start_y.div_euclid(factor_isize);
    let end_x =
        (window.start_x + window.size_x as isize + factor_isize - 1).div_euclid(factor_isize);
    let end_y =
        (window.start_y + window.size_y as isize + factor_isize - 1).div_euclid(factor_isize);
    GdalReadWindow {
        start_x,
        start_y,
        size_x: (end_x - start_x) as usize,
        size_y: (end_y - start_y) as usize,
    }
}

/// The read advise for one axis of a file that is read from an overview level.
///
/// GDAL builds overview pixel `j` of a file from the original pixels `[phase + j * level, phase + (j + 1) * level)`
/// (clipped to the file). These pixels are not necessarily aligned with the overview pixels of the tile grid,
/// because the file's offset from the tile grid's origin does not have to be a multiple of the level.
/// We therefore assign every overview pixel of the file to exactly one tile pixel, namely the one that contains its start.
/// This keeps the mapping a constant integer shift, so adjacent files leave no gaps between them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OverviewAxisReadAdvise {
    /// start of the read window in original resolution pixels of the file
    window_start: isize,
    /// size of the read window in original resolution pixels of the file
    window_size: usize,
    /// inclusive bounds of the filled tile pixels in tile grid pixels
    target_bounds: [isize; 2],
}

impl OverviewAxisReadAdvise {
    /// - `file_offset`: index of the file's first pixel, in original resolution pixels relative to the tile grid's origin
    /// - `file_size`: number of original resolution pixels of the file
    /// - `readable`: inclusive range of the file's pixels that belong to the dataset
    /// - `phase`: original resolution pixel where the file's overview pixel edges start
    /// - `tile_bounds`: inclusive bounds of the tile in tile grid pixels
    fn new(
        file_offset: isize,
        file_size: isize,
        readable: [isize; 2],
        phase: isize,
        overview_level: isize,
        tile_bounds: [isize; 2],
    ) -> Option<Self> {
        // tile pixel = file overview pixel + shift: each tile pixel takes the overview pixel under
        // its center, as GDAL's nearest resampling does, so a file shifted against the tile grid
        // is off by at most half an overview pixel. `ceil((offset - level / 2) / level)`
        let shift = (file_offset + phase + overview_level / 2 - 1).div_euclid(overview_level);

        let first_overview_pixel = (readable[0] - phase)
            .div_euclid(overview_level)
            .max(tile_bounds[0] - shift);
        let last_overview_pixel = (readable[1] - phase)
            .div_euclid(overview_level)
            .min(tile_bounds[1] - shift);

        if first_overview_pixel > last_overview_pixel {
            return None;
        }

        let window_start = (phase + first_overview_pixel * overview_level).max(0);
        let window_end = (phase + (last_overview_pixel + 1) * overview_level).min(file_size);

        Some(Self {
            window_start,
            window_size: (window_end - window_start) as usize,
            target_bounds: [first_overview_pixel + shift, last_overview_pixel + shift],
        })
    }
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::{
        primitives::Coordinate2D,
        raster::{
            BoundedGrid, GeoTransform, GridBoundingBox2D, GridIdx2D, GridShape2D,
            SpatialGridDefinition,
        },
    };

    use super::*;

    #[test]
    fn reader_state_dataset_geo_transform() {
        let reader_state = ReaderState {
            dataset_spatial_grid: SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
                GridShape2D::new([1024, 1024]).bounding_box(),
            ),
        };

        assert_eq!(
            reader_state.dataset_spatial_grid.geo_transform(),
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.)
        );
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_no_change() {
        let spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([1024, 1024]).bounding_box(),
        );

        let reader_state = ReaderState {
            dataset_spatial_grid: spatial_grid,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 0,
                start_y: 0,
                size_x: 512,
                size_y: 512,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_shifted() {
        let spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(-180., 90.), 1., -1.),
            GridShape2D::new([180, 360]).bounding_box(),
        );

        let reader_state = ReaderState {
            dataset_spatial_grid: spatial_grid,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 180,
                start_y: 90,
                size_x: 180,
                size_y: 90,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_shifted_and_clipped() {
        let spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(-180., 90.), 1., -1.),
            GridShape2D::new([180, 360]).bounding_box(),
        );

        let reader_state = ReaderState {
            dataset_spatial_grid: spatial_grid,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new(GridIdx2D::new([10, 10]), GridIdx2D::new([99, 189]))
                    .unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 190,
                start_y: 100,
                size_x: 170,
                size_y: 80,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([10, 10]), GridIdx2D::new([89, 179])).unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([10, 10]), GridIdx2D::new([99, 189])).unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_shifted_flipy() {
        let spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(-180., 90.), 1., -1.),
            GridShape2D::new([180, 360]).bounding_box(),
        );

        let spatial_grid_flipy = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(-180., -90.), 1., 1.),
            GridShape2D::new([180, 360]).bounding_box(),
        );

        let reader_state = ReaderState {
            dataset_spatial_grid: spatial_grid,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &spatial_grid_flipy,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 180,
                start_y: 0,
                size_x: 180,
                size_y: 90,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap()
        );

        assert!(tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_overview_2() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([1024, 1024]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: std::num::NonZeroUsize::new(2).unwrap(),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 2., -2.),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 0,
                start_y: 0,
                size_x: 1024,
                size_y: 1024,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_overview_4() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([2048, 2048]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: std::num::NonZeroUsize::new(4).unwrap(),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 4., -4.),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 0,
                start_y: 0,
                size_x: 2048,
                size_y: 2048,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn it_reads_a_coarser_file_from_an_overview_level() {
        // a dataset of 10 m pixels, a file of 60 m pixels, tiles of overview level 8 (80 m)
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 10., -10.),
            GridShape2D::new([10980, 10980]).bounding_box(),
        );
        let coarser_file = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 60., -60.),
            GridShape2D::new([1830, 1830]).bounding_box(),
        );
        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: std::num::NonZeroUsize::new(8).unwrap(),
        };
        let tile = |x_tile: isize| {
            SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 80., -80.),
                GridBoundingBox2D::new(
                    GridIdx2D::new([0, x_tile * 512]),
                    GridIdx2D::new([511, x_tile * 512 + 511]),
                )
                .unwrap(),
            )
        };

        assert_eq!(
            GdalReaderMode::OverviewLevel(reader_state)
                .coarser_file_factor(&coarser_file)
                .map(NonZeroUsize::get),
            Some(6)
        );

        let first = reader_state
            .tiling_to_dataset_read_advise(&coarser_file, &tile(0))
            .unwrap();
        // 512 tile pixels are 4096 dataset pixels, i.e. 682 2/3 file pixels, rounded up
        assert_eq!(
            first.gdal_read_widow,
            GdalReadWindow {
                start_x: 0,
                start_y: 0,
                size_x: 683,
                size_y: 683,
            },
        );
        assert_eq!(
            first.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        // the second tile starts inside a file pixel: the window grows to whole file pixels
        let second = reader_state
            .tiling_to_dataset_read_advise(&coarser_file, &tile(1))
            .unwrap();
        assert_eq!(
            second.gdal_read_widow,
            GdalReadWindow {
                start_x: 682,
                start_y: 0,
                size_x: 684,
                size_y: 683,
            },
        );
        assert_eq!(
            second.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([0, 512]), GridIdx2D::new([511, 1023])).unwrap()
        );
    }

    #[test]
    fn it_skips_files_that_are_no_integer_multiple_of_the_dataset() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 10., -10.),
            GridShape2D::new([100, 100]).bounding_box(),
        );
        let file = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 15., -15.),
            GridShape2D::new([66, 66]).bounding_box(),
        );
        let reader_mode = GdalReaderMode::OverviewLevel(OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: std::num::NonZeroUsize::new(2).unwrap(),
        });

        assert_eq!(reader_mode.coarser_file_factor(&file), None);
        assert_eq!(
            reader_mode.coarser_file_factor(&original_spatial_grid),
            None,
            "a file at the dataset's resolution is not coarser"
        );
        assert!(
            reader_mode
                .tiling_to_dataset_read_advise(
                    &file,
                    &SpatialGridDefinition::new(
                        GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 20., -20.),
                        GridShape2D::new([50, 50]).bounding_box(),
                    ),
                )
                .is_none()
        );
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_overview_4_tile_22() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([4096, 4096]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: std::num::NonZeroUsize::new(4).unwrap(),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 4., -4.),
                GridBoundingBox2D::new(GridIdx2D::new([512, 512]), GridIdx2D::new([1023, 1023]))
                    .unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 2048,
                start_y: 2048,
                size_x: 2048,
                size_y: 2048,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([512, 512]), GridIdx2D::new([1023, 1023]))
                .unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([512, 512]), GridIdx2D::new([1023, 1023]))
                .unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_overview_4_tile_22_lrcrop() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([4096 - 16, 4096 - 16]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: std::num::NonZeroUsize::new(4).unwrap(),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 4., -4.),
                GridBoundingBox2D::new(GridIdx2D::new([512, 512]), GridIdx2D::new([1023, 1023]))
                    .unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 2048,
                start_y: 2048,
                size_x: 2048 - 16,
                size_y: 2048 - 16,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(
                GridIdx2D::new([512, 512]),
                GridIdx2D::new([1023 - 4, 1023 - 4])
            )
            .unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([512, 512]), GridIdx2D::new([1023, 1023]))
                .unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_overview_4_tile_22_ulcrop() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(16., -16.), 1., -1.),
            GridShape2D::new([4096 - 16, 4096 - 16]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: std::num::NonZeroUsize::new(4).unwrap(),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 4., -4.),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 0,
                start_y: 0,
                size_x: 2048 - 16,
                size_y: 2048 - 16,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([4, 4]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_overview_4_tile_22_ulcrop_numbers() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new_unchecked(Coordinate2D::new(17.123_456, -17.123_456), 1., -1.),
            GridShape2D::new([4096 - 16, 4096 - 16]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: std::num::NonZeroUsize::new(4).unwrap(),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new_unchecked(Coordinate2D::new(1.123_456, -1.123_456), 4., -4.),
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap(),
            ),
        );

        assert!(tiling_to_dataset_read_advise.is_some());

        let tiling_to_dataset_read_advise = tiling_to_dataset_read_advise.unwrap();

        assert_eq!(
            tiling_to_dataset_read_advise.gdal_read_widow,
            GdalReadWindow {
                start_x: 0,
                start_y: 0,
                size_x: 2048 - 16,
                size_y: 2048 - 16,
            },
        );

        assert_eq!(
            tiling_to_dataset_read_advise.read_window_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([4, 4]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert_eq!(
            tiling_to_dataset_read_advise.bounds_of_target,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert!(!tiling_to_dataset_read_advise.flip_y);
    }

    #[test]
    fn it_snaps_overview_read_window_to_tile_edge() {
        let reader_state = OverviewReaderState {
            original_dataset_grid: SpatialGridDefinition {
                geo_transform: GeoTransform::new_unchecked(
                    Coordinate2D { x: -180.0, y: 90.0 },
                    0.2,
                    -0.2,
                ),
                grid_bounds: GridBoundingBox2D::new_unchecked([0, 0], [899, 1799]),
            },
            overview_level: std::num::NonZeroUsize::new(2).unwrap(),
        };

        let actual_gdal_dataset_spatial_grid_definition = SpatialGridDefinition {
            geo_transform: GeoTransform::new_unchecked(
                Coordinate2D {
                    x: -45.0,
                    y: 22.399_999_999_999_99,
                },
                0.2,
                -0.2,
            ),
            grid_bounds: GridBoundingBox2D::new_unchecked([0, 0], [561, 1124]),
        };

        let tile = SpatialGridDefinition {
            geo_transform: GeoTransform::new_unchecked(Coordinate2D { x: 0.0, y: 0.0 }, 0.4, -0.4),
            grid_bounds: GridBoundingBox2D::new_unchecked([-512, -512], [-1, -1]),
        };

        let read_advise = reader_state
            .tiling_to_dataset_read_advise(&actual_gdal_dataset_spatial_grid_definition, &tile)
            .unwrap();

        assert_eq!(
            read_advise.read_window_bounds,
            GridBoundingBox2D::new_unchecked([-56, -113], [-1, -1])
        );
    }

    /// Each tile pixel takes the overview pixel under its center, as GDAL's nearest resampling.
    #[test]
    fn it_maps_tile_pixels_to_the_overview_pixel_under_their_center() {
        // file_offset, expected target bounds of the 16 overview pixels of a 64 px file at level 4
        for (file_offset, target) in [
            (0, [0, 15]),
            // the overview pixel 0 covers [1, 5) and holds the center 2 of tile pixel 0
            (1, [0, 15]),
            // the edge case: the center 2 of tile pixel 0 is on the left edge of overview pixel 0
            (2, [0, 15]),
            // [3, 7) holds the center 6 of tile pixel 1, tile pixel 0 has its center outside the file
            (3, [1, 16]),
            // [-3, 1) holds the center -2 of tile pixel -1
            (-3, [-1, 14]),
        ] {
            let advise =
                OverviewAxisReadAdvise::new(file_offset, 64, [0, 63], 0, 4, [-100, 100]).unwrap();
            assert_eq!(advise.target_bounds, target, "file offset {file_offset}");
            assert_eq!((advise.window_start, advise.window_size), (0, 64));
        }
    }

    /// A mosaic of 2x2 files whose edges are not aligned to the overview grid must still fill every overview pixel
    /// that lies completely inside the mosaic, especially the ones at the seams between the files.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn it_fills_all_overview_pixels_of_unaligned_mosaic() {
        const FILE_SIZE: isize = 101;
        const TILE_SIZE: isize = 16;

        let mut failures = Vec::new();

        for overview_level in [2_isize, 4, 8] {
            // mosaic origins in all four quadrants, not aligned to the overview grid, with regular and flipped files
            for (origin_x, origin_y, flip_y) in [
                (3, 5, false),
                (-301, 5, false),
                (3, -207, false),
                (-301, -207, false),
                (3, 5, true),
                (-301, -207, true),
            ] {
                let mosaic_grid = SpatialGridDefinition::new(
                    GeoTransform::new_unchecked(
                        Coordinate2D::new(origin_x as f64, origin_y as f64),
                        1.,
                        -1.,
                    ),
                    GridBoundingBox2D::new_unchecked(
                        [0, 0],
                        [2 * FILE_SIZE - 1, 2 * FILE_SIZE - 1],
                    ),
                );

                let files = (0..2)
                    .flat_map(|file_y| (0..2).map(move |file_x| (file_y, file_x)))
                    .map(|(file_y, file_x)| {
                        let file_x = (origin_x + file_x * FILE_SIZE) as f64;
                        // a flipped file has its origin at the bottom and a positive y pixel size
                        let (file_y, y_pixel_size) = if flip_y {
                            ((origin_y - (file_y + 1) * FILE_SIZE) as f64, 1.)
                        } else {
                            ((origin_y - file_y * FILE_SIZE) as f64, -1.)
                        };
                        SpatialGridDefinition::new(
                            GeoTransform::new_unchecked(
                                Coordinate2D::new(file_x, file_y),
                                1.,
                                y_pixel_size,
                            ),
                            GridBoundingBox2D::new_unchecked(
                                [0, 0],
                                [FILE_SIZE - 1, FILE_SIZE - 1],
                            ),
                        )
                    })
                    .collect::<Vec<_>>();

                let reader_state = OverviewReaderState {
                    original_dataset_grid: mosaic_grid,
                    overview_level: std::num::NonZeroUsize::new(overview_level as usize).unwrap(),
                };

                // global overview pixel grid with the tiling origin at (0, 0)
                let overview_geo_transform = GeoTransform::new_unchecked(
                    Coordinate2D::new(0., 0.),
                    overview_level as f64,
                    -overview_level as f64,
                );

                // overview pixels that lie completely inside the mosaic
                let mosaic_x_min = origin_x;
                let mosaic_x_max = origin_x + 2 * FILE_SIZE; // exclusive
                let mosaic_y_max = origin_y;
                let mosaic_y_min = origin_y - 2 * FILE_SIZE; // exclusive
                let x_min = -(-mosaic_x_min).div_euclid(overview_level);
                let x_max = mosaic_x_max.div_euclid(overview_level) - 1;
                let y_min = -mosaic_y_max.div_euclid(overview_level);
                let y_max = (-mosaic_y_min).div_euclid(overview_level) - 1;

                let mut covered = std::collections::HashSet::new();

                for tile_y in y_min.div_euclid(TILE_SIZE)..=y_max.div_euclid(TILE_SIZE) {
                    for tile_x in x_min.div_euclid(TILE_SIZE)..=x_max.div_euclid(TILE_SIZE) {
                        let tile = SpatialGridDefinition::new(
                            overview_geo_transform,
                            GridBoundingBox2D::new_unchecked(
                                [tile_y * TILE_SIZE, tile_x * TILE_SIZE],
                                [
                                    tile_y * TILE_SIZE + TILE_SIZE - 1,
                                    tile_x * TILE_SIZE + TILE_SIZE - 1,
                                ],
                            ),
                        );

                        for file in &files {
                            let advise = std::panic::catch_unwind(|| {
                                reader_state.tiling_to_dataset_read_advise(file, &tile)
                            });

                            let advise = match advise {
                                Ok(Some(advise)) => advise,
                                Ok(None) => continue,
                                Err(_) => {
                                    failures.push(format!(
                                        "level {overview_level}, origin ({origin_x}, {origin_y}), flip {flip_y}: panic for tile {:?} and file origin {:?}",
                                        tile.grid_bounds(),
                                        file.geo_transform().origin_coordinate()
                                    ));
                                    continue;
                                }
                            };

                            let window = advise.gdal_read_widow;
                            let bounds = advise.read_window_bounds;
                            let level = overview_level as usize;
                            if advise.flip_y != flip_y
                                || window.start_x % overview_level != 0
                                || window.start_y % overview_level != 0
                                || window.start_x + window.size_x as isize > FILE_SIZE
                                || window.start_y + window.size_y as isize > FILE_SIZE
                                || window.size_x.div_ceil(level) != bounds.axis_size_x()
                                || window.size_y.div_ceil(level) != bounds.axis_size_y()
                            {
                                failures.push(format!(
                                    "level {overview_level}, origin ({origin_x}, {origin_y}), flip {flip_y}: inconsistent read advise {advise:?} for tile {:?}",
                                    tile.grid_bounds()
                                ));
                            }
                            for y in bounds.y_min()..=bounds.y_max() {
                                for x in bounds.x_min()..=bounds.x_max() {
                                    covered.insert((y, x));
                                }
                            }
                        }
                    }
                }

                let mut missing = Vec::new();
                for y in y_min..=y_max {
                    for x in x_min..=x_max {
                        if !covered.contains(&(y, x)) {
                            missing.push((y, x));
                        }
                    }
                }

                if !missing.is_empty() {
                    failures.push(format!(
                        "level {overview_level}, origin ({origin_x}, {origin_y}), flip {flip_y}: {} of {} overview pixels not filled, e.g. (y, x) {:?}",
                        missing.len(),
                        (y_max - y_min + 1) * (x_max - x_min + 1),
                        &missing[..missing.len().min(5)]
                    ));
                }
            }
        }

        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
