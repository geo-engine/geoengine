use geoengine_datatypes::raster::{
    BoundedGrid, GeoTransform, GridBoundingBox2D, GridBoundingBoxExt, GridBounds, GridIdx2D,
    GridIntersection, GridOrEmpty2D, GridShape2D, GridShapeAccess, GridSize, RasterProperties,
};

/// This struct is used to advise the GDAL reader how to read the data from the dataset.
/// The Workflow is as follows:
/// 1. The gdal_read_window is the window in the pixel space of the dataset that should be read.
/// 2. The read_window_bounds is the area in the target pixel space where the data should be placed.
/// 2.1 The data read in step one is read to the width and height of the read_window_bounds.
/// 2.2 if flip_y is true the data is flipped in the y direction. And should be unflipped after reading.
/// 3. The bounds_of_target is the area in the target pixel space where the data should be placed.
/// 3.1 The read_window_bounds might be offset from the bounds_of_target or might have a different size.
/// Then, the data needs to be placed in the target pixel space accordingly. Other parts of the target pixel space should be filled with nodata.
#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
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
    /// check if the dataset intersects the given bounds
    pub fn dataset_intersects_bounds(&self, bounds: &GridBoundingBox2D) -> bool {
        match self {
            GdalReaderMode::OriginalResolution(reader_state) => {
                dbg!(reader_state.intersection_tiling_bounds(bounds)).is_some()
            }
            GdalReaderMode::OverviewLevel(_overview_reader_state) => {
                unimplemented!()
            }
        }
    }

    /// Returns the read advise for the tiling based bounds
    pub fn tiling_to_dataset_read_advise(
        &self,
        tiling_based_bounds: GridBoundingBox2D,
    ) -> Option<GdalReadAdvise> {
        match self {
            GdalReaderMode::OriginalResolution(reader_state) => {
                reader_state.tiling_to_dataset_read_advise(tiling_based_bounds)
            }
            GdalReaderMode::OverviewLevel(_overview_reader_state) => {
                unimplemented!()
            }
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ReaderState {
    pub dataset_shape: GridShape2D,
    pub dataset_geo_transform: GeoTransform,
}

impl ReaderState {
    #[inline]
    fn dataset_bounds(&self) -> GridBoundingBox2D {
        self.dataset_shape.bounding_box()
    }

    #[inline]
    fn dataset_geo_transform(&self) -> GeoTransform {
        // todo: only allow if the origin is in the upper left corner?
        self.dataset_geo_transform
    }

    #[inline]
    fn tiling_to_dataset_bounds(
        &self,
        tiling_based_bounds: GridBoundingBox2D,
    ) -> (GridBoundingBox2D, GridIdx2D) {
        let dataset_geo_transform = self.dataset_geo_transform();
        let offset = dataset_geo_transform.nearest_pixel_to_zero();
        let tiling_to_dataset = tiling_based_bounds.shift_by_offset(offset);

        (tiling_to_dataset, offset)
    }

    #[inline]
    fn intersection_tiling_bounds(&self, other: &GridBoundingBox2D) -> Option<GridBoundingBox2D> {
        let dataset_bounds = self.dataset_bounds();
        let (tiling_to_dataset_bounds, offset) = self.tiling_to_dataset_bounds(*other);

        dataset_bounds
            .intersection(&tiling_to_dataset_bounds)
            .map(|i| i.shift_by_offset(offset * -1))
    }

    #[inline]
    fn tiling_to_dataset_read_advise(
        &self,
        tiling_based_bounds: GridBoundingBox2D,
    ) -> Option<GdalReadAdvise> {
        // we need to shift the tiling based bounds to the dataset bounds
        let dataset_geo_transform = self.dataset_geo_transform();
        let offset = dataset_geo_transform.nearest_pixel_to_zero();
        let tiling_to_dataset_bounds = tiling_based_bounds.shift_by_offset(offset);

        //we can only read the intersection of the tiling based bounds and the dataset bounds from GDAL. If the intersection is empty we can't read anything.
        let tiling_dataset_intersection =
            tiling_to_dataset_bounds.intersection(&self.dataset_bounds())?;

        // generate the read window for GDAL
        let read_window = GdalReadWindow::new(
            tiling_dataset_intersection.min_index(),
            tiling_dataset_intersection.grid_shape(),
        );

        // if the read window has the same shape as the tiling based bounds we can fill that completely
        if tiling_dataset_intersection.grid_shape() == tiling_based_bounds.grid_shape() {
            return Some(GdalReadAdvise {
                gdal_read_widow: read_window,
                read_window_bounds: tiling_based_bounds,
                bounds_of_target: tiling_based_bounds,
                flip_y: false,
            });
        };

        // now we need to adapt the target pixel space read window to the clipped dataset intersection area
        let shifted_readable_bounds = tiling_dataset_intersection.shift_by_offset(offset * -1);

        Some(GdalReadAdvise {
            gdal_read_widow: read_window,
            read_window_bounds: shifted_readable_bounds,
            bounds_of_target: tiling_based_bounds,
            flip_y: false,
        })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct OverviewReaderState {
    /*
        dataset_shape: GridShape2D,
        dataset_geo_transform: GdalDatasetGeoTransform,
        target_pixel_bounds: GridBoundingBox2D,
        tiling_strategy: TilingStrategy,
    }

    impl OverviewReaderState {
        #[inline]
        pub fn dataset_bounds(&self) -> GridBoundingBox2D {
            self.dataset_shape.bounding_box()
        }

        #[inline]
        pub fn target_bounds(&self) -> GridBoundingBox2D {
            self.target_pixel_bounds
        }

        #[inline]
        pub fn is_original_resolution(&self) -> bool {
            debug_assert!(approx_eq!(
                f64,
                self.dataset_geo_transform.x_pixel_size,
                self.tiling_strategy.geo_transform.x_pixel_size()
            ));
            debug_assert!(approx_eq!(
                f64,
                self.dataset_geo_transform.y_pixel_size,
                self.tiling_strategy.geo_transform.y_pixel_size()
            ));
            self.dataset_shape == self.target_pixel_bounds.grid_shape()
        }
    */
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
        raster::{GeoTransform, GridBoundingBox2D, GridIdx2D, GridShape2D},
    };

    use crate::source::gdal_source::reader::{GdalReadWindow, ReaderState};

    #[test]
    fn reader_state_dataset_bounds() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([1024, 1024]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
        };

        assert_eq!(
            reader_state.dataset_bounds(),
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([1023, 1023])).unwrap()
        );
    }

    #[test]
    fn reader_state_dataset_geo_transform() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([1024, 1024]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
        };

        assert_eq!(
            reader_state.dataset_geo_transform(),
            GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.)
        );
    }

    #[test]
    fn reader_state_tiling_to_dataset_bounds_no_change() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([1024, 1024]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
        };

        let (tiling_to_dataset_bounds, offset) = reader_state.tiling_to_dataset_bounds(
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap(),
        );

        assert_eq!(
            tiling_to_dataset_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap()
        );

        assert_eq!(offset, GridIdx2D::new([0, 0]));
    }

    #[test]
    fn reader_state_tiling_to_dataset_bounds_shifted() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([180, 360]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(-180., 90.), 1., -1.),
        };

        let (tiling_to_dataset_bounds, offset) = reader_state.tiling_to_dataset_bounds(
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap(),
        );

        assert_eq!(
            tiling_to_dataset_bounds,
            GridBoundingBox2D::new(GridIdx2D::new([90, 180]), GridIdx2D::new([179, 359])).unwrap()
        );

        assert_eq!(offset, GridIdx2D::new([90, 180]));
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_no_change() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([1024, 1024]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(0., 0.), 1., -1.),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([511, 511])).unwrap(),
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

        assert_eq!(tiling_to_dataset_read_advise.flip_y, false);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_shifted() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([180, 360]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(-180., 90.), 1., -1.),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap(),
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

        assert_eq!(tiling_to_dataset_read_advise.flip_y, false);
    }

    #[test]
    fn reader_state_tiling_to_dataset_read_advise_shifted_and_clipped() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([180, 360]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(-180., 90.), 1., -1.),
        };

        let tiling_to_dataset_read_advise = reader_state.tiling_to_dataset_read_advise(
            GridBoundingBox2D::new(GridIdx2D::new([10, 10]), GridIdx2D::new([99, 189])).unwrap(),
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

        assert_eq!(tiling_to_dataset_read_advise.flip_y, false);
    }

    #[test]
    fn intersection_tiling_bounds() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([180, 360]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(-180., 90.), 1., -1.),
        };

        let intersection = reader_state.intersection_tiling_bounds(
            &GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap(),
        );

        assert_eq!(
            intersection,
            Some(
                GridBoundingBox2D::new(GridIdx2D::new([0, 0]), GridIdx2D::new([89, 179])).unwrap()
            )
        );
    }

    #[test]
    fn intersection_tiling_bounds_clipped() {
        let reader_state = ReaderState {
            dataset_shape: GridShape2D::new([180, 360]),
            dataset_geo_transform: GeoTransform::new(Coordinate2D::new(-180., 90.), 1., -1.),
        };

        let intersection = reader_state.intersection_tiling_bounds(
            &GridBoundingBox2D::new(GridIdx2D::new([10, 10]), GridIdx2D::new([99, 189])).unwrap(),
        );

        assert_eq!(
            intersection,
            Some(
                GridBoundingBox2D::new(GridIdx2D::new([10, 10]), GridIdx2D::new([89, 179]))
                    .unwrap()
            )
        );
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
}