use geoengine_datatypes::{
    primitives::AxisAlignedRectangle,
    raster::{
        GridBoundingBox2D, GridBounds, GridContains, GridIdx2D, GridOrEmpty, GridShape2D,
        GridShapeAccess, GridSize, RasterProperties, SpatialGridDefinition,
    },
};
use tracing::{trace, warn};

/// This struct is used to advise the GDAL reader how to read the data from the dataset.
/// The Workflow is as follows:
/// 1. The `gdal_read_window` is the window in the pixel space of the dataset that should be read.
/// 2. The `read_window_bounds` is the area in the target pixel space where the data should be placed.
///    2.1 The data read in step one is read to the width and height of the `read_window_bounds`.
///    2.2 if `flip_y` is true the data is flipped in the y direction. And should be unflipped after reading.
/// 3. The `bounds_of_target` is the area in the target pixel space where the data should be placed.
///    3.1 The `read_window_bounds` might be offset from the `bounds_of_target` or might have a different size.
///    Then, the data needs to be placed in the target pixel space accordingly. Other parts of the target pixel space should be filled with nodata.
#[allow(dead_code)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct GdalReadAdvise {
    pub gdal_read_widow: GdalReadWindow,
    pub read_window_bounds: GridBoundingBox2D,
    pub bounds_of_target: GridBoundingBox2D,
    pub flip_y: bool,
}

// TODO: re-implement direct read(?)
// impl GdalReadAdvise {
//     pub fn direct_read(&self) -> bool {
//         self.read_window_bounds == self.bounds_of_target
//     }
// }

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum GdalReaderMode {
    // read the original resolution
    OriginalResolution(ReaderState),
    // read an overview level of the dataset
    OverviewLevel(OverviewReaderState),
}

impl GdalReaderMode {
    /// Returns the read advise for the tiling based bounds
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
}

#[derive(Copy, Clone, Debug)]
pub struct ReaderState {
    pub dataset_spatial_grid: SpatialGridDefinition,
}

impl ReaderState {
    pub fn tiling_to_dataset_read_advise(
        &self,
        actual_gdal_dataset_spatial_grid_definition: &SpatialGridDefinition,
        tile: &SpatialGridDefinition,
    ) -> Option<GdalReadAdvise> {
        // Check if the y_axis is fliped.
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
        ); // TODO: raise error if this fails!

        let Some(tile_in_gdal_dataset_bounds) = tile_in_gdal_dataset_bounds else {
            warn!("BUG: Tile is not in the dataset bounds, skipping read advise generation");
            return None;
        };

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
            flip_y: false,
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct OverviewReaderState {
    pub original_dataset_grid: SpatialGridDefinition,
    pub overview_level: u32,
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
        // Check if the y_axis is fliped.
        let (actual_gdal_dataset_spatial_grid_definition, flip_y) =
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
            };

        // This is the intersection of grid of the gdal file and the global grid we use. Usually the dataset is inside the global dataset grid.
        // IF the intersection is empty the we return early and load nothing
        // The intersection uses the geo_transform of the gdal dataset which enables us to adress gdal pixels starting at 0,0
        let actual_bounds_to_use_original_resolution = actual_gdal_dataset_spatial_grid_definition
            .intersection(&self.original_dataset_grid)?;

        // now we map the tile we want to fill to the original grid. First, we set the tile to use the same origin coordinate as the gdal file/dataset
        let tile_with_overview_resolution_in_actual_space = if let Some(tile) = tile
            .with_moved_origin_exact_grid(
                actual_gdal_dataset_spatial_grid_definition
                    .geo_transform()
                    .origin_coordinate(),
            ) {
            tile
        } else {
            // special case where the original resolution tile's origin is not a valid pixel edge in the overview
            // in this case we need to snap to the nearest pixel edge in the original resolution
            trace!(
                "overview tile grid {:?} does not align with actual datasets grid {:?}, use nearest original edge instead",
                tile, actual_gdal_dataset_spatial_grid_definition
            );

            // move to pixel edge nearest to actual grid edge, the result is within one pixel distance in overview space
            let nearest_in_overview_space = tile.with_moved_origin_to_nearest_grid_edge(
                actual_gdal_dataset_spatial_grid_definition
                    .geo_transform()
                    .origin_coordinate(),
            );

            // replace the origin with the desired one. this forces the overview grid on the actual grid
            nearest_in_overview_space.replace_origin(
                actual_gdal_dataset_spatial_grid_definition
                    .geo_transform()
                    .origin_coordinate(),
            )
        };

        // then we change the resolution to the original resolution of the gdal dataset
        let tile_with_original_resolution_in_actual_space =
            tile_with_overview_resolution_in_actual_space.with_changed_resolution(
                actual_gdal_dataset_spatial_grid_definition
                    .geo_transform()
                    .spatial_resolution(),
            );

        // Now we need to intersect the tile and the actual bounds to use to identify what we can really read
        let tile_intersection_original_resolution_actual_space =
            &tile_with_original_resolution_in_actual_space
                .intersection(&actual_bounds_to_use_original_resolution)?;

        // if we need to unflip the dataset grid now is the time to do this.
        let tile_intersection_for_read_window = if flip_y {
            tile_intersection_original_resolution_actual_space
                .flip_axis_y() // first: reverse the coordinate system to match the one used by tiling
                .shift_bounds_relative_by_pixel_offset(GridIdx2D::new_y_x(
                    // second: move the origin to the other end of the y-axis
                    actual_gdal_dataset_spatial_grid_definition
                        .grid_bounds()
                        .axis_size_y() as isize,
                    0,
                ))
        } else {
            *tile_intersection_original_resolution_actual_space
        };

        // The input dataset and the output tile may not be aligned with respect to the overview level, i.e. the origin of the dataset is not a multiple of `overview_level` pixels away from the tile origin
        // Thus, we snap the computed intersection to the overview level relative to the current GDAL dataset (file) origin to ensure that GDAL actually uses overviews for reading
        let tile_intersection_for_read_window_snapped_to_overview_level =
            tile_intersection_original_resolution_actual_space.snap_to_dataset_overview_level(
                actual_gdal_dataset_spatial_grid_definition,
                self.overview_level,
            )?;

        // generate the read window for GDAL --> This is what we can read in any case.
        // TODO: does this include everything we need even if we snap the output to geo engine overview level?
        let read_window = GdalReadWindow::new(
            tile_intersection_for_read_window_snapped_to_overview_level.min_index(),
            tile_intersection_for_read_window_snapped_to_overview_level
                .grid_bounds()
                .grid_shape(),
        );

        // ensure that start and size of the read window are aligned with the overview level
        // must start from overview level aligned pixel
        debug_assert_eq!(read_window.start_x % self.overview_level as isize, 0);
        debug_assert_eq!(read_window.start_y % self.overview_level as isize, 0);

        // must be multiple of overview level in size, unless we are at the edge of the dataset
        debug_assert!(
            read_window.size_x % self.overview_level as usize == 0
                || read_window.start_x + read_window.size_x as isize
                    == actual_gdal_dataset_spatial_grid_definition
                        .grid_bounds()
                        .axis_size_x() as isize
        );
        debug_assert!(
            read_window.size_y % self.overview_level as usize == 0
                || read_window.start_y + read_window.size_y as isize
                    == actual_gdal_dataset_spatial_grid_definition
                        .grid_bounds()
                        .axis_size_y() as isize
        );

        let can_fill_whole_tile = tile_intersection_for_read_window.grid_bounds()
            == tile_with_original_resolution_in_actual_space.grid_bounds();

        if can_fill_whole_tile {
            // ensure that the read window is `overview_level` times larger than the tile
            debug_assert_eq!(
                read_window.size_x / tile.grid_bounds().grid_shape().x(),
                self.overview_level as usize
            );
            debug_assert_eq!(
                read_window.size_y / tile.grid_bounds().grid_shape().y(),
                self.overview_level as usize
            );

            let advise = GdalReadAdvise {
                gdal_read_widow: read_window,
                read_window_bounds: tile.grid_bounds,
                bounds_of_target: tile.grid_bounds,
                flip_y,
            };
            trace!("Tile is fully contained: {advise:?}");
            return Some(advise);
        }

        // IF we can't fill the whole tile, we have to find out which area of the tile we can fill.
        let readable_area_in_overview_res = tile_intersection_original_resolution_actual_space
            .with_changed_resolution(tile.geo_transform().spatial_resolution());

        let readable_area_in_overview_res_and_tile_space =
            if tile.is_compatible_grid_generic(&readable_area_in_overview_res) {
                readable_area_in_overview_res
            } else {
                // we need to make the readable area grid compatible to the tile in overview space, as the actual origin does not exist in overview space
                let tile_edge_idx = tile.geo_transform.coordinate_to_grid_idx_2d(
                    readable_area_in_overview_res
                        .geo_transform()
                        .origin_coordinate(),
                );
                let tile_edge_coord = tile
                    .geo_transform
                    .grid_idx_to_pixel_upper_left_coordinate_2d(tile_edge_idx);

                let readable_area_in_overview_res_and_tile_space =
                    readable_area_in_overview_res.replace_origin(tile_edge_coord);

                // if the snapping of the origin causes us to lose a pixel, we extend the bounds by one pixel in the respective direction
                let mut extend = [0, 0];
                if tile.geo_transform().origin_coordinate().y - tile_edge_coord.y
                    > tile.geo_transform().y_pixel_size()
                {
                    let max_index = readable_area_in_overview_res_and_tile_space
                        .grid_bounds()
                        .max_index()
                        .y();

                    let new_border_y = readable_area_in_overview_res_and_tile_space
                        .geo_transform()
                        .origin_coordinate()
                        .y
                        + (readable_area_in_overview_res_and_tile_space
                            .geo_transform()
                            .y_pixel_size()
                            * (max_index + 1) as f64);

                    // TODO: there must be an easier way to check if we are still within the dataset bounds
                    if new_border_y
                        < actual_gdal_dataset_spatial_grid_definition // TODO: approx equal?
                            .spatial_partition()
                            .lower_right()
                            .y
                    {
                        extend[0] = 1;
                    }
                }

                if tile.geo_transform().origin_coordinate().x - tile_edge_coord.x
                    > tile.geo_transform().x_pixel_size()
                {
                    let max_index = readable_area_in_overview_res_and_tile_space
                        .grid_bounds()
                        .max_index()
                        .x();

                    let new_border_x = readable_area_in_overview_res_and_tile_space
                        .geo_transform()
                        .origin_coordinate()
                        .x
                        + (readable_area_in_overview_res_and_tile_space
                            .geo_transform()
                            .x_pixel_size()
                            * (max_index + 1) as f64);

                    // TODO: there must be an easier way to check if we are still within the dataset bounds
                    if new_border_x
                        < actual_gdal_dataset_spatial_grid_definition // TODO: approx equal?
                            .spatial_partition()
                            .lower_right()
                            .x
                    {
                        extend[1] = 1;
                    }
                }

                let bounds = GridBoundingBox2D::new_unchecked(
                    readable_area_in_overview_res_and_tile_space
                        .grid_bounds()
                        .min_index(),
                    readable_area_in_overview_res_and_tile_space
                        .grid_bounds()
                        .max_index()
                        + GridIdx2D::new_y_x(extend[0], extend[1]),
                );

                SpatialGridDefinition::new(
                    readable_area_in_overview_res_and_tile_space.geo_transform(),
                    bounds,
                )
            };

        // Calculate the intersection of the readable area and the tile, result is in geotransform of the tile!
        let readable_tile_area = tile
            .intersection(&readable_area_in_overview_res_and_tile_space)
            .expect(
                "Since there was an intersection earlyer, there must be a part of data to read.",
            );

        // we need to crop the window to the intersection of the tiling based bounds and the dataset bounds
        let crop_tl = readable_tile_area.min_index() - tile.min_index();
        let crop_lr = readable_tile_area.max_index() - tile.max_index();

        let shifted_tl = tile.grid_bounds.min_index() + crop_tl;
        let shifted_lr = tile.grid_bounds.max_index() + crop_lr;

        // now we need to adapt the target pixel space read window to the clipped dataset intersection area
        let shifted_readable_bounds = GridBoundingBox2D::new_unchecked(shifted_tl, shifted_lr);
        debug_assert!(
            tile.grid_bounds().contains(&shifted_readable_bounds),
            "readable bounds must be contained in tile bounds"
        );

        Some(GdalReadAdvise {
            gdal_read_widow: read_window,
            read_window_bounds: shifted_readable_bounds,
            bounds_of_target: tile.grid_bounds,
            flip_y,
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct GdalReadWindow {
    start_x: isize, // pixelspace origin
    start_y: isize,
    size_x: usize, // pixelspace size
    size_y: usize,
}

impl GdalReadWindow {
    pub fn new(start: GridIdx2D, size: GridShape2D) -> Self {
        Self {
            start_x: start.x(),
            start_y: start.y(),
            size_x: size.axis_size_x(),
            size_y: size.axis_size_y(),
        }
    }

    pub fn gdal_window_start(&self) -> (isize, isize) {
        (self.start_x, self.start_y)
    }

    pub fn gdal_window_size(&self) -> (usize, usize) {
        (self.size_x, self.size_y)
    }
}

pub struct GridAndProperties<T> {
    pub grid: GridOrEmpty<GridBoundingBox2D, T>,
    pub properties: RasterProperties,
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

    use crate::source::multi_band_gdal_source::reader::{
        GdalReadWindow, OverviewReaderState, ReaderState,
    };

    #[test]
    fn reader_state_dataset_geo_transform() {
        let reader_state = ReaderState {
            dataset_spatial_grid: SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
                GridShape2D::new([1024, 1024]).bounding_box(),
            ),
        };

        assert_eq!(
            reader_state.dataset_spatial_grid.geo_transform(),
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.)
        );
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_no_change() {
        let spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([1024, 1024]).bounding_box(),
        );

        let reader_state = ReaderState {
            dataset_spatial_grid: spatial_grid,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
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
            GeoTransform::new(Coordinate2D::new(-180., 90.), 1., -1.),
            GridShape2D::new([180, 360]).bounding_box(),
        );

        let reader_state = ReaderState {
            dataset_spatial_grid: spatial_grid,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
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
            GeoTransform::new(Coordinate2D::new(-180., 90.), 1., -1.),
            GridShape2D::new([180, 360]).bounding_box(),
        );

        let reader_state = ReaderState {
            dataset_spatial_grid: spatial_grid,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
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
            GeoTransform::new(Coordinate2D::new(-180., 90.), 1., -1.),
            GridShape2D::new([180, 360]).bounding_box(),
        );

        let spatial_grid_flipy = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(-180., -90.), 1., 1.),
            GridShape2D::new([180, 360]).bounding_box(),
        );

        let reader_state = ReaderState {
            dataset_spatial_grid: spatial_grid,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &spatial_grid_flipy,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
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
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([1024, 1024]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: 2,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 2., -2.),
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
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([2048, 2048]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: 4,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 4., -4.),
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
    fn reader_state_tiling_to_dataset_read_advise_overview_4_tile_22() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([4096, 4096]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: 4,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 4., -4.),
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
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([4096 - 16, 4096 - 16]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: 4,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 4., -4.),
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
            GeoTransform::new(Coordinate2D::new(16., -16.), 1., -1.),
            GridShape2D::new([4096 - 16, 4096 - 16]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: 4,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(0., 0.), 4., -4.),
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
            GeoTransform::new(Coordinate2D::new(17.123_456, -17.123_456), 1., -1.),
            GridShape2D::new([4096 - 16, 4096 - 16]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
            overview_level: 4,
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            &original_spatial_grid,
            &SpatialGridDefinition::new(
                GeoTransform::new(Coordinate2D::new(1.123_456, -1.123_456), 4., -4.),
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
                geo_transform: GeoTransform::new(Coordinate2D { x: -180.0, y: 90.0 }, 0.2, -0.2),
                grid_bounds: GridBoundingBox2D::new_unchecked([0, 0], [899, 1799]),
            },
            overview_level: 2,
        };

        let actual_gdal_dataset_spatial_grid_definition = SpatialGridDefinition {
            geo_transform: GeoTransform::new(
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
            geo_transform: GeoTransform::new(Coordinate2D { x: 0.0, y: 0.0 }, 0.4, -0.4),
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
}
