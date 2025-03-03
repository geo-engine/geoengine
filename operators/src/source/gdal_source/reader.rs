use geoengine_datatypes::raster::{
    GridBoundingBox2D, GridBounds, GridContains, GridIdx2D, GridOrEmpty2D, GridShape2D,
    GridShapeAccess, GridSize, RasterProperties, SpatialGridDefinition,
};

/// This struct is used to advise the GDAL reader how to read the data from the dataset.
/// The Workflow is as follows:
/// 1. The `gdal_read_window` is the window in the pixel space of the dataset that should be read.
/// 2. The `read_window_bounds` is the area in the target pixel space where the data should be placed.
///     2.1 The data read in step one is read to the width and height of the `read_window_bounds`.
///     2.2 if `flip_y` is true the data is flipped in the y direction. And should be unflipped after reading.
/// 3. The `bounds_of_target` is the area in the target pixel space where the data should be placed.
///     3.1 The `read_window_bounds` might be offset from the `bounds_of_target` or might have a different size.
///         Then, the data needs to be placed in the target pixel space accordingly. Other parts of the target pixel space should be filled with nodata.
#[allow(dead_code)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct GdalReadAdvise {
    pub gdal_read_widow: GdalReadWindow,
    pub read_window_bounds: GridBoundingBox2D,
    pub bounds_of_target: GridBoundingBox2D,
    pub flip_y: bool,
}

impl GdalReadAdvise {
    pub fn direct_read(&self) -> bool {
        self.read_window_bounds == self.bounds_of_target
    }
}

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
        };

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
}

impl OverviewReaderState {
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
        let tile_with_overview_resolution_in_actual_space = tile
            .with_moved_origin_exact_grid(
                actual_gdal_dataset_spatial_grid_definition
                    .geo_transform()
                    .origin_coordinate(),
            )
            .expect("The overview level grid must map to pixel coordinates in the original grid"); // TODO: maybe relax this?
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

        // generate the read window for GDAL --> This is what we can read in any case.
        let read_window = GdalReadWindow::new(
            tile_intersection_for_read_window.min_index(),
            tile_intersection_for_read_window.grid_bounds().grid_shape(),
        );

        let is_tile_contained = tile_intersection_for_read_window.grid_bounds()
            == tile_with_original_resolution_in_actual_space.grid_bounds();

        if is_tile_contained {
            return Some(GdalReadAdvise {
                gdal_read_widow: read_window,
                read_window_bounds: tile.grid_bounds,
                bounds_of_target: tile.grid_bounds,
                flip_y,
            });
        }

        // IF we can't read the whole tile, we have to find out which area of the tile we can fill.
        let readble_area_in_overview_res = tile_intersection_original_resolution_actual_space
            .with_changed_resolution(tile.geo_transform().spatial_resolution());

        // Calculate the intersection of the readable area and the tile, result is in geotransform of the tile!
        let readable_tile_area = tile.intersection(&readble_area_in_overview_res).expect(
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
    pub grid: GridOrEmpty2D<T>,
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

    use crate::source::gdal_source::reader::{GdalReadWindow, OverviewReaderState, ReaderState};

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

    /*
     #[test]
    fn gdal_geotransform_to_read_bounds() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);

        let ti: TileInformation = TileInformation::new(
            GridIdx([1, 1]),
            GridShape2D::new([512, 512]),
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
        );

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 512,
                size_y: 512,
                start_x: 512,
                start_y: 512,
            }
        );

        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([512, 512]), GridIdx([1023, 1023])).unwrap()
        );
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_half_res() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);

        let ti: TileInformation = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::new([512, 512]),
            GeoTransform::new(Coordinate2D::new(0., 0.), 2., -2.),
        );

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 1024,
                size_y: 1024,
                start_x: 0,
                start_y: 0,
            }
        );

        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([0, 0]), GridIdx([511, 511])).unwrap()
        );
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_2x_res() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(0., 0.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);

        let ti: TileInformation = TileInformation::new(
            GridIdx([0, 0]),
            GridShape2D::new([512, 512]),
            GeoTransform::new(Coordinate2D::new(0., 0.), 0.5, -0.5),
        );

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 256,
                size_y: 256,
                start_x: 0,
                start_y: 0,
            }
        );

        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([0, 0]), GridIdx([511, 511])).unwrap()
        );
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_ul_out() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(-3., 3.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);
        let tile_grid_shape = GridShape2D::new([512, 512]);
        let tiling_global_geo_transfom = GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.);

        let ti: TileInformation =
            TileInformation::new(GridIdx([0, 0]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        // since the origin of the tile is at -3,3 and the "coordinate nearest to zero" is 0,0 the tile at tile position 0,0 maps to the read window starting at 3,3 with 512x512 pixels
        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 512,
                size_y: 512,
                start_x: 3,
                start_y: 3,
            }
        );

        // the data maps to the complete tile
        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([0, 0]), GridIdx([511, 511])).unwrap()
        );

        let ti: TileInformation =
            TileInformation::new(GridIdx([1, 1]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        // since the origin of the tile is at -3,3 and the "coordinate nearest to zero" is 0,0 the tile at tile position 1,1 maps to the read window starting at 515,515 (512+3, 512+3) with 512x512 pixels
        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 509,
                size_y: 509,
                start_x: 515,
                start_y: 515,
            }
        );

        // the data maps only to a part of the tile since the data is only 1024x1024 pixels in size. So the tile at tile position 1,1 maps to the data starting at 515,515 (512+3, 512+3) with 509x509 pixels left.
        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([512, 512]), GridIdx([1020, 1020])).unwrap()
        );
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_ul_in() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(3., -3.),
            x_pixel_size: 1.,
            y_pixel_size: -1.,
        };

        let gdal_data_size = GridShape2D::new([1024, 1024]);
        let tile_grid_shape = GridShape2D::new([512, 512]);
        let tiling_global_geo_transfom = GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.);

        let ti: TileInformation =
            TileInformation::new(GridIdx([0, 0]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        // in this case the data origin is at 3,-3 which is inside the tile at tile position 0,0. Since the tile starts at the "coordinate nearest to zero, which is 0.0,0.0" we need to read the data starting at data 0,0 with 509x509 pixels (512-3, 512-3).
        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 509,
                size_y: 509,
                start_x: 0,
                start_y: 0,
            }
        );

        // in this case, the data only maps to the last 509x509 pixels of the tile. So the data we read does not fill a whole tile.
        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([3, 3]), GridIdx([511, 511])).unwrap()
        );

        let ti: TileInformation =
            TileInformation::new(GridIdx([1, 1]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 512,
                size_y: 512,
                start_x: 509,
                start_y: 509,
            }
        );

        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([512, 512]), GridIdx([1023, 1023])).unwrap()
        );

        let ti: TileInformation =
            TileInformation::new(GridIdx([2, 2]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 3,
                size_y: 3,
                start_x: 1021,
                start_y: 1021,
            }
        );

        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([1024, 1024]), GridIdx([1026, 1026])).unwrap()
        );
    }

    #[test]
    fn gdal_geotransform_to_read_bounds_ul_out_frac_res() {
        let gdal_geo_transform: GdalDatasetGeoTransform = GdalDatasetGeoTransform {
            origin_coordinate: Coordinate2D::new(-9., 9.),
            x_pixel_size: 9.,
            y_pixel_size: -9.,
        };
        let gdal_data_size = GridShape2D::new([1024, 1024]);
        let tile_grid_shape = GridShape2D::new([512, 512]);
        let tiling_global_geo_transfom = GeoTransform::new(Coordinate2D::new(-0., 0.), 3., -3.);

        let ti: TileInformation =
            TileInformation::new(GridIdx([0, 0]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 170, //
                size_y: 170,
                start_x: 1,
                start_y: 1,
            }
        );

        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([0, 0]), GridIdx([512, 512])).unwrap()
        ); // we need to read 683 pixels but we only want 682.6666666666666 pixels.

        let ti: TileInformation =
            TileInformation::new(GridIdx([1, 1]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 171,
                size_y: 171,
                start_x: 171,
                start_y: 171,
            }
        );

        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([510, 510]), GridIdx([1025, 1025])).unwrap()
        );

        let ti: TileInformation =
            TileInformation::new(GridIdx([2, 2]), tile_grid_shape, tiling_global_geo_transfom);

        let (read_window, target_bounds) = gdal_geo_transform
            .grid_bounds_resolution_to_read_window_and_target_grid(gdal_data_size, &ti)
            .unwrap();

        assert_eq!(
            read_window,
            GdalReadWindow {
                size_x: 171,
                size_y: 171,
                start_x: 342,
                start_y: 342,
            }
        );

        assert_eq!(
            target_bounds,
            GridBoundingBox2D::new(GridIdx([1023, 1023]), GridIdx([1535, 1535])).unwrap()
        );
    }
     */

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_overview_2() {
        let original_spatial_grid = SpatialGridDefinition::new(
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
            GridShape2D::new([1024, 1024]).bounding_box(),
        );

        let reader_state = OverviewReaderState {
            original_dataset_grid: original_spatial_grid,
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
}
