use crate::{
    primitives::{AxisAlignedRectangle, Coordinate2D, SpatialPartition2D, SpatialResolution},
    util::test::TestDefault,
};
use serde::{de, Deserialize, Deserializer, Serialize};

use super::{GridBoundingBox2D, GridBounds, GridIdx, GridIdx2D};

/// This is a typedef for the `GDAL GeoTransform`. It represents an affine transformation matrix.
pub type GdalGeoTransform = [f64; 6];

/// The `GeoTransform` specifies the relation between pixel coordinates and geographic coordinates.
/// In Geo Engine x pixel size is always postive and y pixel size is always negative. For raster tiles
/// the origin is always the upper left corner. In the global grid for the `TilingStrategy` the origin
/// is always located at (0, 0).
#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GeoTransform {
    pub origin_coordinate: Coordinate2D,
    x_pixel_size: f64,
    y_pixel_size: f64,
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
    #[inline]
    pub fn new(origin_coordinate: Coordinate2D, x_pixel_size: f64, y_pixel_size: f64) -> Self {
        debug_assert!(x_pixel_size > 0.0);
        debug_assert!(y_pixel_size < 0.0);

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
    #[inline]
    pub fn new_with_coordinate_x_y(
        origin_coordinate_x: f64,
        x_pixel_size: f64,
        origin_coordinate_y: f64,
        y_pixel_size: f64,
    ) -> Self {
        debug_assert!(x_pixel_size > 0.0);
        debug_assert!(y_pixel_size < 0.0);

        Self {
            origin_coordinate: (origin_coordinate_x, origin_coordinate_y).into(),
            x_pixel_size,
            y_pixel_size,
        }
    }

    pub fn x_pixel_size(&self) -> f64 {
        self.x_pixel_size
    }

    pub fn y_pixel_size(&self) -> f64 {
        self.y_pixel_size
    }

    /// Transforms a grid coordinate (row, column) ~ (y, x) into a SRS coordinate (x,y)
    /// The resulting coordinate is the upper left coordinate of the pixel
    /// See GDAL documentation for more details (including the two ignored parameters): <https://gdal.org/user/raster_data_model.html>
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    /// use geoengine_datatypes::primitives::{Coordinate2D};
    ///
    /// let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0);
    /// assert_eq!(geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d([0, 0].into()), (0.0, 0.0).into())
    /// ```
    ///
    #[inline]
    pub fn grid_idx_to_pixel_upper_left_coordinate_2d(
        &self,
        grid_index: GridIdx2D,
    ) -> Coordinate2D {
        let GridIdx([.., grid_index_y, grid_index_x]) = grid_index;
        let coord_x = self.origin_coordinate.x + (grid_index_x as f64) * self.x_pixel_size;
        let coord_y = self.origin_coordinate.y + (grid_index_y as f64) * self.y_pixel_size;
        Coordinate2D::new(coord_x, coord_y)
    }

    /// Transforms a grid coordinate (row, column) ~ (y, x) into a SRS coordinate (x,y)
    /// The resulting coordinate is the coordinate of the center of the pixel
    #[inline]
    pub fn grid_idx_to_pixel_center_coordinate_2d(&self, grid_index: GridIdx2D) -> Coordinate2D {
        let GridIdx([.., grid_index_y, grid_index_x]) = grid_index;
        let coord_x = self.origin_coordinate.x + (grid_index_x as f64 + 0.5) * self.x_pixel_size;
        let coord_y = self.origin_coordinate.y + (grid_index_y as f64 + 0.5) * self.y_pixel_size;
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
    /// assert_eq!(geo_transform.coordinate_to_grid_idx_2d((0.0, 0.0).into()), [0, 0].into())
    /// ```
    ///
    #[inline]
    pub fn coordinate_to_grid_idx_2d(&self, coord: Coordinate2D) -> GridIdx2D {
        let grid_x_index =
            ((coord.x - self.origin_coordinate.x) / self.x_pixel_size).floor() as isize;
        let grid_y_index =
            ((coord.y - self.origin_coordinate.y) / self.y_pixel_size).floor() as isize;
        [grid_y_index, grid_x_index].into()
    }

    /// create the worldfile content for this `GeoTransform`
    pub fn worldfile_string(&self) -> String {
        format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            self.x_pixel_size,
            0.,
            0.,
            self.y_pixel_size,
            self.origin_coordinate.x + (self.x_pixel_size / 2.),
            self.origin_coordinate.y - (self.y_pixel_size / 2.)
        )
    }

    pub fn spatial_resolution(&self) -> SpatialResolution {
        SpatialResolution {
            x: self.x_pixel_size.abs(),
            y: self.y_pixel_size.abs(),
        }
    }

    /// compute the index of the upper left pixel that is contained in the `partition`
    pub fn upper_left_pixel_idx(&self, partition: &SpatialPartition2D) -> GridIdx2D {
        // choose the epsilon relative to the pixel size
        const EPSILON: f64 = 0.000_001;
        let epsilon: Coordinate2D =
            (self.x_pixel_size() * EPSILON, self.y_pixel_size() * EPSILON).into();

        let upper_left_coordinate = partition.upper_left() + epsilon;

        self.coordinate_to_grid_idx_2d(upper_left_coordinate)
    }

    /// compute the index of the lower right pixel that is contained in the `partition`
    pub fn lower_right_pixel_idx(&self, partition: &SpatialPartition2D) -> GridIdx2D {
        // as the lower right coordinate is not included in the partition we subtract an epsilon
        // in order to not include the next pixel if the lower right coordinate is exactly on the
        // edge of the next pixel

        // choose the epsilon relative to the pixel size
        const EPSILON: f64 = 0.000_001;
        let epsilon: Coordinate2D =
            (self.x_pixel_size() * EPSILON, self.y_pixel_size() * EPSILON).into();

        // shift lower right by epsilon
        let lower_right = partition.lower_right() - epsilon;

        // ensure we don't accidentally go beyond the upper left pixel
        let lower_right = (
            lower_right.x.max(partition.upper_left().x),
            lower_right.y.min(partition.upper_left().y),
        )
            .into();

        self.coordinate_to_grid_idx_2d(lower_right)
    }

    /// Transform a `SpatialPartition2D` into a `GridBoundingBox`
    #[inline]
    pub fn spatial_to_grid_bounds(
        &self,
        spatial_partition: &SpatialPartition2D,
    ) -> GridBoundingBox2D {
        let GridIdx([ul_y, ul_x]) = self.upper_left_pixel_idx(spatial_partition);
        let GridIdx([lr_y, lr_x]) = self.lower_right_pixel_idx(spatial_partition); // this is the pixel inside the spatial partition

        debug_assert!(ul_x <= lr_x);
        debug_assert!(ul_y <= lr_y);

        let start: GridIdx2D = [ul_y, ul_x].into();
        let end: GridIdx2D = [lr_y, lr_x].into();

        GridBoundingBox2D::new_unchecked(start, end)
    }

    pub fn deserialize_with_check<'de, D>(deserializer: D) -> Result<GeoTransform, D::Error>
    where
        D: Deserializer<'de>,
    {
        let unchecked = GeoTransform::deserialize(deserializer)?;
        if unchecked.x_pixel_size.is_sign_negative() {
            return Err(de::Error::custom("x_pixel_size must be positive"));
        }
        if unchecked.y_pixel_size.is_sign_positive() {
            return Err(de::Error::custom("y_pixel_size must be negative"));
        }

        Ok(unchecked)
    }

    pub fn grid_to_spatial_bounds(&self, grid_bounds: &GridBoundingBox2D) -> SpatialPartition2D {
        let ul = self.grid_idx_to_pixel_upper_left_coordinate_2d(grid_bounds.min_index());
        let lr = self.grid_idx_to_pixel_upper_left_coordinate_2d(grid_bounds.max_index() + 1);

        SpatialPartition2D::new_unchecked(ul, lr)
    }

    pub fn origin_coordinate(&self) -> Coordinate2D {
        self.origin_coordinate
    }
}

impl TestDefault for GeoTransform {
    fn test_default() -> Self {
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

impl From<GeoTransform> for GdalGeoTransform {
    fn from(geo_transform: GeoTransform) -> GdalGeoTransform {
        [
            geo_transform.origin_coordinate.x,
            geo_transform.x_pixel_size,
            0.0, // self.x_rotation,
            geo_transform.origin_coordinate.y,
            0.0, // self.y_rotation,
            geo_transform.y_pixel_size,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        primitives::SpatialPartition2D,
        raster::{GeoTransform, GridIdx2D},
    };

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
            geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(GridIdx2D::new([0, 0])),
            (5.0, 5.0).into()
        );
        assert_eq!(
            geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(GridIdx2D::new([1, 1])),
            (6.0, 4.0).into()
        );
        assert_eq!(
            geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(GridIdx2D::new([2, 2])),
            (7.0, 3.0).into()
        );
        assert_eq!(
            geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d(GridIdx2D::new([-1, -1])),
            (4.0, 6.0).into()
        );
    }

    #[test]
    fn geo_transform_coordinate_2d_to_grid_2d() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(5.0, 1.0, 5.0, -1.0);
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((5.0, 5.0).into()),
            GridIdx2D::new([0, 0])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((6.0, 4.0).into()),
            GridIdx2D::new([1, 1])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((7.0, 3.0).into()),
            GridIdx2D::new([2, 2])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((4.0, 6.0).into()),
            GridIdx2D::new([-1, -1])
        );
    }

    #[test]
    fn geo_transform_coordinate_2d_to_global_grid_2d() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0);
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((0.0, 0.0).into()),
            GridIdx2D::new([0, 0])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((0.5, 0.0).into()),
            GridIdx2D::new([0, 0])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((0.5, 0.5).into()),
            GridIdx2D::new([-1, 0])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((0.0, 0.5).into()),
            GridIdx2D::new([-1, 0])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((0.5, -0.5).into()),
            GridIdx2D::new([0, 0])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((-0.5, 0.5).into()),
            GridIdx2D::new([-1, -1])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((-0.5, -0.5).into()),
            GridIdx2D::new([0, -1])
        );

        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((1.5, -0.5).into()),
            GridIdx2D::new([0, 1])
        );
        assert_eq!(
            geo_transform.coordinate_to_grid_idx_2d((-1.5, 1.5).into()),
            GridIdx2D::new([-2, -2])
        );
    }

    #[test]
    fn pixel_center() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(5.0, 1.0, 5.0, -1.0);
        assert_eq!(
            geo_transform.grid_idx_to_pixel_center_coordinate_2d(GridIdx2D::new([0, 0])),
            (5.5, 4.5).into()
        );
    }

    #[test]
    fn pixel_box_three_pixels() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0);

        assert_eq!(
            geo_transform.spatial_to_grid_bounds(
                &SpatialPartition2D::new((6.0, 4.0).into(), (9.0, 1.0).into()).unwrap()
            ),
            GridBoundingBox2D::new(GridIdx2D::new([-4, 6]), GridIdx2D::new([-2, 8])).unwrap()
        );
    }

    #[test]
    fn pixel_box_one_pixel() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0);

        assert_eq!(
            geo_transform.spatial_to_grid_bounds(
                &SpatialPartition2D::new((6.0, 4.0).into(), (7.0, 3.0).into()).unwrap()
            ),
            GridBoundingBox2D::new(GridIdx2D::new([-4, 6]), GridIdx2D::new([-4, 6])).unwrap()
        );
    }

    #[test]
    fn pixel_box_mini() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0);

        assert_eq!(
            geo_transform.spatial_to_grid_bounds(
                &SpatialPartition2D::new((6.0, 4.0).into(), (6.1, 3.9).into()).unwrap()
            ),
            GridBoundingBox2D::new(GridIdx2D::new([-4, 6]), GridIdx2D::new([-4, 6])).unwrap()
        );
    }

    #[test]
    fn lower_right_pixel_index_edge() {
        let geo_transform = GeoTransform::test_default();

        let partition = SpatialPartition2D::new((1., 1.).into(), (8., -8.).into()).unwrap();
        assert_eq!(
            geo_transform.lower_right_pixel_idx(&partition),
            [7, 7].into()
        );

        let partition = SpatialPartition2D::new((1., 1.).into(), (8.5, -8.).into()).unwrap();
        assert_eq!(
            geo_transform.lower_right_pixel_idx(&partition),
            [7, 8].into()
        );

        let partition = SpatialPartition2D::new((1., 1.).into(), (8., -8.5).into()).unwrap();
        assert_eq!(
            geo_transform.lower_right_pixel_idx(&partition),
            [8, 7].into()
        );
    }

    #[test]
    fn lower_right_pixel_index_inside() {
        let geo_transform = GeoTransform::test_default();

        let partition = SpatialPartition2D::new((1., 1.).into(), (7.5, -7.5).into()).unwrap();
        assert_eq!(
            geo_transform.lower_right_pixel_idx(&partition),
            [7, 7].into()
        );
    }

    #[test]
    fn deserialze() {
        let gt = GeoTransform::new_with_coordinate_x_y(-180.0, 1., 90.0, -1.);

        let test: GeoTransform = serde_json::from_str(
            r#"{
            "originCoordinate": {
              "x": -180.0,
              "y": 90.0
            },
            "xPixelSize": 1.0,
            "yPixelSize": -1.0
          }"#,
        )
        .unwrap();

        assert_eq!(gt, test);
    }

    #[test]
    fn deserialze_with_check_ok() {
        let gt = GeoTransform::new_with_coordinate_x_y(-180.0, 1., 90.0, -1.);

        let mut de = serde_json::Deserializer::from_str(
            r#"{
            "originCoordinate": {
              "x": -180.0,
              "y": 90.0
            },
            "xPixelSize": 1.0,
            "yPixelSize": -1.0
          }"#,
        );

        let test = GeoTransform::deserialize_with_check(&mut de).unwrap();

        assert_eq!(gt, test);
    }

    #[test]
    fn deserialze_with_check_fail() {
        let mut de = serde_json::Deserializer::from_str(
            r#"{
            "originCoordinate": {
              "x": -180.0,
              "y": 90.0
            },
            "xPixelSize": -1.0,
            "yPixelSize": -1.0
          }"#,
        );

        let test = GeoTransform::deserialize_with_check(&mut de);

        assert!(test.is_err());

        let mut de = serde_json::Deserializer::from_str(
            r#"{
            "originCoordinate": {
              "x": -180.0,
              "y": 90.0
            },
            "xPixelSize": 1.0,
            "yPixelSize": 1.0
          }"#,
        );

        let test = GeoTransform::deserialize_with_check(&mut de);

        assert!(test.is_err());
    }
}
