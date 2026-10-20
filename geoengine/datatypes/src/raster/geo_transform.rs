use super::{GeoTransformAccess, GridBoundingBox2D, GridBounds, GridIdx, GridIdx2D};
use crate::{
    error,
    primitives::{
        AxisAlignedRectangle, BoundingBox2D, Coordinate2D, SpatialPartition2D, SpatialResolution,
    },
    util::{NonZeroFiniteF64, Result, test::TestDefault},
};
use float_cmp::{ApproxEq, F64Margin, approx_eq};
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use typed_floats::tf64;

/// This is a typedef for the `GDAL GeoTransform`. It represents an affine transformation matrix.
pub type GdalGeoTransform = [f64; 6];

/// The `GeoTransform` specifies the relation between pixel coordinates and geographic coordinates.
/// The pixel sizes are always finite and non-zero, which is enforced by their type.
/// Usually, the x pixel size is positive and the y pixel size is negative, but upside-down datasets
/// have a positive y pixel size. For raster tiles the origin is always the upper left corner.
/// In the global grid for the `TilingStrategy` the origin is always located at (0, 0).
#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize, ToSql, FromSql)]
#[serde(rename_all = "camelCase")]
pub struct GeoTransform {
    pub origin_coordinate: Coordinate2D,
    x_pixel_size: NonZeroFiniteF64,
    y_pixel_size: NonZeroFiniteF64,
}

impl GeoTransform {
    /// Generates a new `GeoTransform`, if the origin is finite and the pixel sizes are finite and non-zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    ///
    /// let geo_transform = GeoTransform::new((0.0, 0.0).into(), 1.0, -1.0).unwrap();
    ///
    /// assert!(GeoTransform::new((0.0, 0.0).into(), 0.0, -1.0).is_err());
    /// ```
    ///
    #[inline]
    pub fn new(
        origin_coordinate: Coordinate2D,
        x_pixel_size: f64,
        y_pixel_size: f64,
    ) -> Result<Self> {
        let invalid = || error::InvalidGeoTransform {
            origin_coordinate,
            x_pixel_size,
            y_pixel_size,
        };

        ensure!(
            origin_coordinate.x.is_finite() && origin_coordinate.y.is_finite(),
            invalid()
        );
        let (Ok(x_pixel_size_typed), Ok(y_pixel_size_typed)) = (
            NonZeroFiniteF64::new(x_pixel_size),
            NonZeroFiniteF64::new(y_pixel_size),
        ) else {
            return invalid().fail();
        };

        Ok(Self::new_typed(
            origin_coordinate,
            x_pixel_size_typed,
            y_pixel_size_typed,
        ))
    }

    /// Generates a new `GeoTransform` from known-valid values, e.g. constants.
    ///
    /// # Panics
    ///
    /// If the origin is not finite or a pixel size is zero or not finite.
    #[inline]
    pub fn new_unchecked(
        origin_coordinate: Coordinate2D,
        x_pixel_size: f64,
        y_pixel_size: f64,
    ) -> Self {
        Self::new(origin_coordinate, x_pixel_size, y_pixel_size)
            .expect("the geo transform must be valid")
    }

    /// Generates a new `GeoTransform` from pixel sizes that are already validated.
    #[inline]
    pub fn new_typed(
        origin_coordinate: Coordinate2D,
        x_pixel_size: impl Into<NonZeroFiniteF64>,
        y_pixel_size: impl Into<NonZeroFiniteF64>,
    ) -> Self {
        Self {
            origin_coordinate,
            x_pixel_size: x_pixel_size.into(),
            y_pixel_size: y_pixel_size.into(),
        }
    }

    /// Generates a new `GeoTransform` with explicit x, y values of the upper left edge
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    ///
    /// let geo_transform = GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, -1.0).unwrap();
    /// ```
    #[inline]
    pub fn new_with_coordinate_x_y(
        origin_coordinate_x: f64,
        x_pixel_size: f64,
        origin_coordinate_y: f64,
        y_pixel_size: f64,
    ) -> Result<Self> {
        Self::new(
            (origin_coordinate_x, origin_coordinate_y).into(),
            x_pixel_size,
            y_pixel_size,
        )
    }

    /// Generates a new `GeoTransform` with explicit x, y values of the upper left edge from known-valid values, e.g. constants.
    ///
    /// # Panics
    ///
    /// If the origin is not finite or a pixel size is zero or not finite.
    #[inline]
    pub fn new_with_coordinate_x_y_unchecked(
        origin_coordinate_x: f64,
        x_pixel_size: f64,
        origin_coordinate_y: f64,
        y_pixel_size: f64,
    ) -> Self {
        Self::new_unchecked(
            (origin_coordinate_x, origin_coordinate_y).into(),
            x_pixel_size,
            y_pixel_size,
        )
    }

    pub fn x_pixel_size(&self) -> tf64::NonZeroNonNaNFinite {
        self.x_pixel_size.into()
    }

    pub fn y_pixel_size(&self) -> tf64::NonZeroNonNaNFinite {
        self.y_pixel_size.into()
    }

    pub fn y_axis_is_neg(&self) -> bool {
        self.y_pixel_size().is_sign_negative()
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
    /// let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 0.0, -1.0);
    /// assert_eq!(geo_transform.grid_idx_to_pixel_upper_left_coordinate_2d([0, 0].into()), (0.0, 0.0).into())
    /// ```
    ///
    #[inline]
    pub fn grid_idx_to_pixel_upper_left_coordinate_2d(
        &self,
        grid_index: GridIdx2D,
    ) -> Coordinate2D {
        let GridIdx([.., grid_index_y, grid_index_x]) = grid_index;
        let coord_x = self.origin_coordinate.x + (grid_index_x as f64) * self.x_pixel_size().get();
        let coord_y = self.origin_coordinate.y + (grid_index_y as f64) * self.y_pixel_size().get();
        Coordinate2D::new(coord_x, coord_y)
    }

    /// Transforms a grid coordinate (row, column) ~ (y, x) into a SRS coordinate (x,y)
    /// The resulting coordinate is the coordinate of the center of the pixel
    #[inline]
    pub fn grid_idx_to_pixel_center_coordinate_2d(&self, grid_index: GridIdx2D) -> Coordinate2D {
        let GridIdx([.., grid_index_y, grid_index_x]) = grid_index;
        let coord_x =
            self.origin_coordinate.x + (grid_index_x as f64 + 0.5) * self.x_pixel_size().get();
        let coord_y =
            self.origin_coordinate.y + (grid_index_y as f64 + 0.5) * self.y_pixel_size().get();
        Coordinate2D::new(coord_x, coord_y)
    }

    /// Transforms an SRS coordinate (x,y) into a grid coordinate (row, column) ~ (y, x)
    /// This method selects the grid index with the center nearest to the coordinate.
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::raster::{GeoTransform};
    /// use geoengine_datatypes::primitives::{Coordinate2D};
    ///
    /// let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 0.0, -1.0);
    /// assert_eq!(geo_transform.coordinate_to_grid_idx_2d((0.0, 0.0).into()), [0, 0].into())
    /// ```
    ///
    #[inline]
    pub fn coordinate_to_grid_idx_2d(&self, coord: Coordinate2D) -> GridIdx2D {
        let grid_x_index =
            ((coord.x - self.origin_coordinate.x) / self.x_pixel_size().get()).floor() as isize;
        let grid_y_index =
            ((coord.y - self.origin_coordinate.y) / self.y_pixel_size().get()).floor() as isize;
        [grid_y_index, grid_x_index].into()
    }

    /// create the worldfile content for this `GeoTransform`
    pub fn worldfile_string(&self) -> String {
        format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            self.x_pixel_size().get(),
            0.,
            0.,
            self.y_pixel_size().get(),
            self.origin_coordinate.x + (self.x_pixel_size().get() / 2.),
            self.origin_coordinate.y - (self.y_pixel_size().get() / 2.)
        )
    }

    pub fn spatial_resolution(&self) -> SpatialResolution {
        // TODO: should honor negative y size
        SpatialResolution::new_typed(self.x_pixel_size.abs(), self.y_pixel_size.abs())
    }

    /// compute the index of the upper left pixel that is contained in the `partition`
    pub fn upper_left_pixel_idx(&self, partition: &SpatialPartition2D) -> GridIdx2D {
        let upper_left_coordinate = partition.upper_left();
        self.coordinate_to_grid_idx_2d(upper_left_coordinate)
    }

    /// compute the index of the lower right pixel that is contained in the `partition`
    pub fn lower_right_pixel_idx(&self, partition: &SpatialPartition2D) -> GridIdx2D {
        // as the lower right coordinate is not included in the partition we subtract an epsilon
        // in order to not include the next pixel if the lower right coordinate is exactly on the
        // edge of the next pixel

        // choose the epsilon relative to the pixel size
        const EPSILON: f64 = 0.000_001;
        let epsilon: Coordinate2D = (
            self.x_pixel_size().get() * EPSILON,
            self.y_pixel_size().get() * EPSILON,
        )
            .into();

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

    /// Transform a `BoundingBox2D` into a `GridBoundingBox2D`.
    #[inline]
    pub fn bounding_box_2d_to_intersecting_grid_bounds(
        &self,
        bounding_box: &BoundingBox2D,
    ) -> GridBoundingBox2D {
        let upper_left = self.coordinate_to_grid_idx_2d(bounding_box.upper_left());
        let lower_right = self.coordinate_to_grid_idx_2d(bounding_box.lower_right());

        GridBoundingBox2D::new_unchecked(upper_left, lower_right)
    }

    /// Transform a `SpatialPartition2D` into a `GridBoundingBox`
    #[inline]
    pub fn spatial_to_grid_bounds(
        &self,
        spatial_partition: &SpatialPartition2D,
    ) -> GridBoundingBox2D {
        let GridIdx([ul_y, ul_x]) = self.coordinate_to_grid_idx_2d(spatial_partition.upper_left());
        let GridIdx([lr_y, lr_x]) = self.lower_right_pixel_idx(spatial_partition); // this is the pixel inside the spatial partition

        debug_assert!(ul_x <= lr_x);
        debug_assert!(ul_y <= lr_y);

        let start: GridIdx2D = [ul_y, ul_x].into();
        let end: GridIdx2D = [lr_y, lr_x].into();

        GridBoundingBox2D::new_unchecked(start, end)
    }

    pub fn grid_to_spatial_bounds<S: GridBounds<IndexArray = [isize; 2]>>(
        &self,
        grid_bounds: &S,
    ) -> SpatialPartition2D {
        let ul = self.grid_idx_to_pixel_upper_left_coordinate_2d(grid_bounds.min_index());
        let lr = self.grid_idx_to_pixel_upper_left_coordinate_2d(grid_bounds.max_index() + 1);

        SpatialPartition2D::new_unchecked(ul, lr)
    }

    pub fn origin_coordinate(&self) -> Coordinate2D {
        self.origin_coordinate
    }

    #[must_use]
    pub fn shift_by_pixel_offset(&self, offset: GridIdx2D) -> Self {
        GeoTransform::new_typed(
            self.grid_idx_to_pixel_upper_left_coordinate_2d(offset),
            self.x_pixel_size,
            self.y_pixel_size,
        )
    }

    pub fn nearest_pixel_edge(&self, coordinate: Coordinate2D) -> GridIdx2D {
        self.coordinate_to_grid_idx_2d(
            coordinate
                + Coordinate2D::new(
                    self.x_pixel_size().get() * 0.5,
                    self.y_pixel_size().get() * 0.5,
                ), // by adding a half pixel, we can find flips between edges
        )
    }

    pub fn nearest_pixel_edge_coordinate(&self, coordinate: Coordinate2D) -> Coordinate2D {
        self.grid_idx_to_pixel_upper_left_coordinate_2d(self.nearest_pixel_edge(coordinate))
    }

    pub fn distance_to_nearest_pixel_edge(&self, coordinate: Coordinate2D) -> Coordinate2D {
        let pixel_edge = self.nearest_pixel_edge_coordinate(coordinate);
        let dist = coordinate - pixel_edge;
        debug_assert!(dist.x.abs() <= self.x_pixel_size().get().abs() * 0.5);
        debug_assert!(dist.y.abs() <= self.y_pixel_size().get().abs() * 0.5);
        dist
    }

    /// `coordinate` is a valid pixel edge if it is approximately equal to the nearest pixel edge, within floating point precision
    pub fn is_valid_pixel_edge(&self, coordinate: Coordinate2D) -> bool {
        let edge = self.nearest_pixel_edge(coordinate);
        let edge_coord = self.grid_idx_to_pixel_upper_left_coordinate_2d(edge);
        // compare x and y separately to account for difference in magnitude

        approx_eq!(f64, coordinate.x, edge_coord.x) && approx_eq!(f64, coordinate.y, edge_coord.y)
    }

    pub fn is_compatible_grid(&self, other: GeoTransform) -> bool {
        self.is_valid_pixel_edge(other.origin_coordinate)
            && approx_eq!(f64, self.x_pixel_size().get(), other.x_pixel_size().get())
            && approx_eq!(f64, self.y_pixel_size().get(), other.y_pixel_size().get())
    }

    pub fn is_compatible_grid_generic<G: GeoTransformAccess>(&self, g: &G) -> bool {
        self.is_compatible_grid(g.geo_transform())
    }
}

impl TestDefault for GeoTransform {
    fn test_default() -> Self {
        GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 0.0, -1.0)
    }
}

impl TryFrom<GdalGeoTransform> for GeoTransform {
    type Error = crate::error::Error;

    fn try_from(gdal_geo_transform: GdalGeoTransform) -> Result<Self> {
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
            geo_transform.x_pixel_size().get(),
            0.0, // self.x_rotation,
            geo_transform.origin_coordinate.y,
            0.0, // self.y_rotation,
            geo_transform.y_pixel_size().get(),
        ]
    }
}

impl ApproxEq for GeoTransform {
    type Margin = F64Margin;

    fn approx_eq<M: Into<Self::Margin>>(self, other: Self, margin: M) -> bool {
        let m: F64Margin = margin.into();

        self.origin_coordinate.approx_eq(other.origin_coordinate, m)
            && self.x_pixel_size.approx_eq(other.x_pixel_size, m)
            && self.y_pixel_size.approx_eq(other.y_pixel_size, m)
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
        let geo_transform = GeoTransform::new_unchecked((0.0, 1.0).into(), 2.0, -3.0);
        assert_eq!(geo_transform.origin_coordinate.x, 0.0);
        assert_eq!(geo_transform.origin_coordinate.y, 1.0);
        assert_eq!(geo_transform.x_pixel_size().get(), 2.0);
        assert_eq!(geo_transform.y_pixel_size().get(), -3.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn geo_transform_new_with_coordinate_x_y() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 2.0, -3.0);
        assert_eq!(geo_transform.origin_coordinate.x, 0.0);
        assert_eq!(geo_transform.x_pixel_size().get(), 1.0);
        assert_eq!(geo_transform.origin_coordinate.y, 2.0);
        assert_eq!(geo_transform.y_pixel_size().get(), -3.0);
    }

    #[test]
    fn geo_transform_grid_2d_to_coordinate_2d() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(5.0, 1.0, 5.0, -1.0);
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
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(5.0, 1.0, 5.0, -1.0);
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
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 0.0, -1.0);
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
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(5.0, 1.0, 5.0, -1.0);
        assert_eq!(
            geo_transform.grid_idx_to_pixel_center_coordinate_2d(GridIdx2D::new([0, 0])),
            (5.5, 4.5).into()
        );
    }

    #[test]
    fn pixel_box_three_pixels() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 0.0, -1.0);

        assert_eq!(
            geo_transform.spatial_to_grid_bounds(
                &SpatialPartition2D::new((6.0, 4.0).into(), (9.0, 1.0).into()).unwrap()
            ),
            GridBoundingBox2D::new(GridIdx2D::new([-4, 6]), GridIdx2D::new([-2, 8])).unwrap()
        );
    }

    #[test]
    fn pixel_box_one_pixel() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 0.0, -1.0);

        assert_eq!(
            geo_transform.spatial_to_grid_bounds(
                &SpatialPartition2D::new((6.0, 4.0).into(), (7.0, 3.0).into()).unwrap()
            ),
            GridBoundingBox2D::new(GridIdx2D::new([-4, 6]), GridIdx2D::new([-4, 6])).unwrap()
        );
    }

    #[test]
    fn pixel_box_mini() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 0.0, -1.0);

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
        let gt = GeoTransform::new_with_coordinate_x_y_unchecked(-180.0, 1., 90.0, -1.);

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
    fn it_rejects_invalid_pixel_sizes_on_deserialize() {
        let json = |x: &str, y: &str| {
            format!(
                r#"{{"originCoordinate": {{"x": -180.0, "y": 90.0}}, "xPixelSize": {x}, "yPixelSize": {y}}}"#
            )
        };

        // upside-down datasets have a positive y pixel size
        assert!(serde_json::from_str::<GeoTransform>(&json("1.0", "1.0")).is_ok());
        assert!(serde_json::from_str::<GeoTransform>(&json("-1.0", "-1.0")).is_ok());

        assert!(serde_json::from_str::<GeoTransform>(&json("0.0", "-1.0")).is_err());
        assert!(serde_json::from_str::<GeoTransform>(&json("1.0", "-0.0")).is_err());
    }

    #[test]
    fn it_rejects_invalid_geo_transforms() {
        assert!(GeoTransform::new((0.0, 0.0).into(), 1.0, -1.0).is_ok());
        assert!(GeoTransform::new((0.0, 0.0).into(), 1.0, 1.0).is_ok());

        for invalid in [0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert!(GeoTransform::new((0.0, 0.0).into(), invalid, -1.0).is_err());
            assert!(GeoTransform::new((0.0, 0.0).into(), 1.0, invalid).is_err());
            assert!(GeoTransform::new_with_coordinate_x_y(0.0, invalid, 0.0, -1.0).is_err());
            assert!(GeoTransform::new_with_coordinate_x_y(0.0, 1.0, 0.0, invalid).is_err());
        }

        for invalid in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert!(GeoTransform::new((invalid, 0.0).into(), 1.0, -1.0).is_err());
            assert!(GeoTransform::new((0.0, invalid).into(), 1.0, -1.0).is_err());
        }

        assert!(GeoTransform::try_from([0.0, 0.0, 0.0, 0.0, 0.0, -1.0]).is_err());
        assert!(GeoTransform::try_from([0.0, 1.0, 0.0, 0.0, 0.0, -1.0]).is_ok());
    }

    #[test]
    fn shift_by_pixel_offset() {
        let geo_transform = GeoTransform::new_with_coordinate_x_y_unchecked(0.0, 1.0, 0.0, -1.0);
        let shifted = geo_transform.shift_by_pixel_offset([1, 1].into());
        assert_eq!(shifted.origin_coordinate, (1.0, -1.0).into());
    }

    #[test]
    fn coordinate_to_nearest_grid_center_idx_2d() {
        let geo_transform = GeoTransform::new_unchecked(Coordinate2D::new(0., 0.), 1., -1.);

        let coord = Coordinate2D::new(0.1, -0.1);
        let center_coord_idx = geo_transform.coordinate_to_grid_idx_2d(coord);
        assert_eq!(center_coord_idx, [0, 0].into());

        let coord = Coordinate2D::new(0.5, -0.5);
        let center_coord_idx = geo_transform.coordinate_to_grid_idx_2d(coord);
        assert_eq!(center_coord_idx, [0, 0].into());

        let coord = Coordinate2D::new(0.9, -0.9);
        let center_coord_idx = geo_transform.coordinate_to_grid_idx_2d(coord);
        assert_eq!(center_coord_idx, [0, 0].into());

        let coord = Coordinate2D::new(1.0, -1.0);
        let center_coord_idx = geo_transform.coordinate_to_grid_idx_2d(coord);
        assert_eq!(center_coord_idx, [1, 1].into());
    }

    #[test]
    fn it_detects_valid_pixel_edge_robustly() {
        let geo_transform = GeoTransform::new_unchecked((-180.0, 90.).into(), 0.2, -0.2);
        assert!(geo_transform.is_valid_pixel_edge(Coordinate2D::new(-45.0, 22.4)));
        assert!(geo_transform.is_valid_pixel_edge(Coordinate2D::new(-45.0, 22.399_999_999_999_99)));
        assert!(geo_transform.is_valid_pixel_edge(Coordinate2D::new(-45.0, 22.4_f64.next_up())));
        assert!(geo_transform.is_valid_pixel_edge(Coordinate2D::new(-45.0, 22.4_f64.next_down())));
        assert!(!geo_transform.is_valid_pixel_edge(Coordinate2D::new(-45.0, 22.41)));

        let geo_transform = GeoTransform::new_unchecked(
            (10_000_000_000_000_000_f64, -10_000_000_000_000_000_f64).into(),
            10_000_000_000_000_000_f64,
            -10_000_000_000_000_000_f64,
        );
        assert!(geo_transform.is_valid_pixel_edge(Coordinate2D::new(
            10_000_000_000_000_000_f64,
            -10_000_000_000_000_000_f64
        )));
        assert!(geo_transform.is_valid_pixel_edge(Coordinate2D::new(
            10_000_000_000_000_001_f64,
            -10_000_000_000_000_000_f64
        )));
        assert!(geo_transform.is_valid_pixel_edge(Coordinate2D::new(
            10_000_000_000_000_000_f64.next_up(),
            -10_000_000_000_000_000_f64.next_up()
        )));
    }
}
