use crate::primitives::Coordinate2D;
use serde::{Deserialize, Serialize};

use super::{Dim, GridIdx2D, GridIndex, SignedGridIdx2D, SignedGridIndex};

/// This is a typedef for the `GDAL GeoTransform`. It represents an affine transformation matrix.
pub type GdalGeoTransform = [f64; 6];

/// The `GeoTransform` is a more user friendly representation of the `GDAL GeoTransform` affine transformation matrix.
#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct GeoTransform {
    pub origin_coordinate: Coordinate2D,
    pub x_pixel_size: f64,
    pub y_pixel_size: f64,
}

impl GeoTransform {
    /// Generates a new `GeoTransform`
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    ///
    /// let geo_transform = GeoTransform::new((0.0, 0.0).into(), 1.0, -1.0);
    /// ```
    ///
    pub fn new(origin_coordinate: Coordinate2D, x_pixel_size: f64, y_pixel_size: f64) -> Self {
        Self {
            origin_coordinate,
            x_pixel_size,
            y_pixel_size,
        }
    }

    /// Generates a new `GeoTransform` with explicit x, y values of the upper left edge
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    ///
    /// let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0);
    /// ```
    ///
    pub fn new_with_coordinate_x_y(
        origin_coordinate_x: f64,
        x_pixel_size: f64,
        origin_coordinate_y: f64,
        y_pixel_size: f64,
    ) -> Self {
        Self {
            origin_coordinate: (origin_coordinate_x, origin_coordinate_y).into(),
            x_pixel_size,
            y_pixel_size,
        }
    }

    /// Transforms a grid coordinate (row, column) ~ (y, x) into a SRS coordinate (x,y)
    /// See GDAL documentation for more details (including the two ignored parameters): <https://gdal.org/user/raster_data_model.html>
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    /// use geoengine_datatypes::primitives::{Coordinate2D};
    ///
    /// let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0);
    /// assert_eq!(geo_transform.grid_idx_to_coordinate_2d([0, 0].into()), (0.0, 0.0).into())
    /// ```
    ///
    pub fn grid_idx_to_coordinate_2d(&self, grid_index: GridIdx2D) -> Coordinate2D {
        let [.., grid_index_y, grid_index_x] = grid_index.as_index_array();
        let coord_x = self.origin_coordinate.x + (grid_index_x as f64) * self.x_pixel_size;
        let coord_y = self.origin_coordinate.y + (grid_index_y as f64) * self.y_pixel_size;
        Coordinate2D::new(coord_x, coord_y)
    }

    /// Transforms a grid coordinate (row, column) ~ (y, x) into a SRS coordinate (x,y)
    /// This method allows the index to have negative values.
    pub fn signed_grid_idx_to_coordinate_2d(&self, grid_index: SignedGridIdx2D) -> Coordinate2D {
        let [.., grid_index_y, grid_index_x] = grid_index.as_index_array();
        let coord_x = self.origin_coordinate.x + (grid_index_x as f64) * self.x_pixel_size;
        let coord_y = self.origin_coordinate.y + (grid_index_y as f64) * self.y_pixel_size;
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
    /// let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0);
    /// assert_eq!(geo_transform.coordinate_2d_to_grid_2d((0.0, 0.0).into()), [0, 0].into())
    /// ```
    ///
    pub fn coordinate_2d_to_grid_2d(&self, coord: Coordinate2D) -> GridIdx2D {
        let grid_x_index = ((coord.x - self.origin_coordinate.x) / self.x_pixel_size) as usize;
        let grid_y_index = ((coord.y - self.origin_coordinate.y) / self.y_pixel_size) as usize;
        [grid_y_index, grid_x_index].into()
    }

    /// Transforms an SRS coordinate (x,y) into a signed grid coordinate (row, column) ~ (y, x). This allows negative values.
    pub fn coordinate_2d_to_signed_grid_2d(&self, coord: Coordinate2D) -> SignedGridIdx2D {
        let grid_x_index = ((coord.x - self.origin_coordinate.x) / self.x_pixel_size) as isize;
        let grid_y_index = ((coord.y - self.origin_coordinate.y) / self.y_pixel_size) as isize;
        Dim::new([grid_y_index, grid_x_index])
    }
}

impl Default for GeoTransform {
    fn default() -> Self {
        GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0)
    }
}

impl From<GdalGeoTransform> for GeoTransform {
    fn from(gdal_geo_transform: GdalGeoTransform) -> Self {
        Self::new_with_coordinate_x_y(
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
            self.origin_coordinate.x,
            self.x_pixel_size,
            0.0, // self.x_rotation,
            self.origin_coordinate.y,
            0.0, // self.y_rotation,
            self.y_pixel_size,
        ]
    }
}

#[cfg(test)]
mod tests {

    use crate::raster::{Dim, GeoTransform};

    #[test]
    #[allow(clippy::float_cmp)]
    fn geo_transform_new() {
        let geo_transform = GeoTransform::new((0.0, 1.0).into(), 2.0, -3.0);
        assert_eq!(geo_transform.origin_coordinate.x, 0.0);
        assert_eq!(geo_transform.origin_coordinate.y, 1.0);
        assert_eq!(geo_transform.x_pixel_size, 2.0);
        assert_eq!(geo_transform.y_pixel_size, -3.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn geo_transform_new_with_coordinate_x_y() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 2.0, -3.0);
        assert_eq!(geo_transform.origin_coordinate.x, 0.0);
        assert_eq!(geo_transform.x_pixel_size, 1.0);
        assert_eq!(geo_transform.origin_coordinate.y, 2.0);
        assert_eq!(geo_transform.y_pixel_size, -3.0);
    }

    #[test]
    fn geo_transform_grid_2d_to_coordinate_2d() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(5.0, 1.0, 5.0, -1.0);
        assert_eq!(
            geo_transform.grid_idx_to_coordinate_2d(Dim::new([0, 0])),
            (5.0, 5.0).into()
        );
        assert_eq!(
            geo_transform.grid_idx_to_coordinate_2d(Dim::new([1, 1])),
            (6.0, 4.0).into()
        );
        assert_eq!(
            geo_transform.grid_idx_to_coordinate_2d(Dim::new([2, 2])),
            (7.0, 3.0).into()
        );
    }

    #[test]
    fn geo_transform_coordinate_2d_to_grid_2d() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(5.0, 1.0, 5.0, -1.0);
        assert_eq!(
            geo_transform.coordinate_2d_to_grid_2d((5.0, 5.0).into()),
            Dim::new([0, 0])
        );
        assert_eq!(
            geo_transform.coordinate_2d_to_grid_2d((6.0, 4.0).into()),
            Dim::new([1, 1])
        );
        assert_eq!(
            geo_transform.coordinate_2d_to_grid_2d((7.0, 3.0).into()),
            Dim::new([2, 2])
        );
    }
}
