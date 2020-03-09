use crate::primitives::Coordinate2D;

/// This is a typedef for the GDAL GeoTransform. It represents an affine transformation matrix.
pub type GdalGeoTransform = [f64; 6];

/// The GeoTransform is a more user friendly representation of the GDAL GeoTransform affine transformation matrix.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct GeoTransform {
    upper_left_x_coordinate: f64,
    x_pixel_size: f64,
    //  x_rotation: f64,
    upper_left_y_coordinate: f64,
    //  y_rotation: f64,
    y_pixel_size: f64,
}

impl GeoTransform {
    /// Generates a new GeoTransform
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    ///
    /// let geo_trnsform = GeoTransform::new(0.0, 1.0, 0.0, -1.0);
    /// ```
    ///
    pub fn new(
        upper_left_x_coordinate: f64,
        x_pixel_size: f64,
        upper_left_y_coordinate: f64,
        y_pixel_size: f64,
    ) -> Self {
        Self {
            upper_left_x_coordinate,
            x_pixel_size,
            upper_left_y_coordinate,
            y_pixel_size,
        }
    }

    /// Transforms a grid coordinate (row, column) ~ (y, x) into a SRS coordinate (x,y)
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    /// use geoengine_datatypes::primitives::{Coordinate2D};
    ///
    /// let geo_transform = GeoTransform::new(0.0, 1.0, 0.0, -1.0);
    /// assert_eq!(geo_transform.grid_2d_to_coordinate_2d((0, 0)), (0.0, 0.0).into())
    /// ```
    ///
    pub fn grid_2d_to_coordinate_2d(&self, grid_index: (usize, usize)) -> Coordinate2D {
        let (grid_index_y, grid_index_x) = grid_index;
        let coord_x = self.upper_left_x_coordinate + (grid_index_x as f64) * self.x_pixel_size; //  + (grid_index_y as f64) * self.x_rotation;
        let coord_y = self.upper_left_y_coordinate + (grid_index_y as f64) * self.y_pixel_size; //  + (grid_index_x as f64) * self.y_rotation
        Coordinate2D::new(coord_x, coord_y)
    }

    /// Transforms an SRS coordinate (x,y) into a grid coordinate (row, column) ~ (y, x)
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    /// use geoengine_datatypes::primitives::{Coordinate2D};
    ///
    /// let geo_transform = GeoTransform::new(0.0, 1.0, 0.0, -1.0);
    /// assert_eq!(geo_transform.coordinate_2d_to_grid_2d(&(0.0, 0.0).into()), (0, 0))
    /// ```
    ///
    pub fn coordinate_2d_to_grid_2d(&self, coord: &Coordinate2D) -> (usize, usize) {
        let grid_x_index = ((coord.x - self.upper_left_x_coordinate) / self.x_pixel_size) as usize;
        let grid_y_index = ((coord.y - self.upper_left_y_coordinate) / self.y_pixel_size) as usize;
        (grid_y_index, grid_x_index)
    }
}

impl Default for GeoTransform {
    fn default() -> Self {
        GeoTransform::new(0.0, 1.0, 0.0, -1.0)
    }
}

impl From<GdalGeoTransform> for GeoTransform {
    fn from(gdal_geo_transform: GdalGeoTransform) -> Self {
        Self::new(
            gdal_geo_transform[0],
            gdal_geo_transform[1],
            // gdal_geo_transform[2],
            gdal_geo_transform[3],
            // gdal_geo_transform[4],
            gdal_geo_transform[5],
        )
    }
}

impl Into<GdalGeoTransform> for GeoTransform {
    fn into(self) -> GdalGeoTransform {
        [
            self.upper_left_x_coordinate,
            self.x_pixel_size,
            0.0, // self.x_rotation,
            self.upper_left_y_coordinate,
            0.0, // self.y_rotation,
            self.y_pixel_size,
        ]
    }
}
