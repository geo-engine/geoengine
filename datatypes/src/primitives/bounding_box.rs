use super::Coordinate2D;
use crate::error;
use crate::util::Result;
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug)]
#[repr(C)]
/// The bounding box of a geometry.
/// Note: may degenerate to a point!
pub struct BoundingBox2D {
    lower_left_coordinate: Coordinate2D,
    upper_right_coordinate: Coordinate2D,
}

impl BoundingBox2D {
    /// Creates a new bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(2.0, 2.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// This constructor fails if the coordinate's values are not in order
    ///
    pub fn new(
        lower_left_coordinate: Coordinate2D,
        upper_right_coordinate: Coordinate2D,
    ) -> Result<Self> {
        ensure!(
            lower_left_coordinate.x <= upper_right_coordinate.x
                && lower_left_coordinate.y <= upper_right_coordinate.x,
            error::InvalidBoundingBox {
                lower_left_coordinate,
                upper_right_coordinate
            }
        );
        Ok(Self {
            lower_left_coordinate,
            upper_right_coordinate,
        })
    }

    /// Creates a new bounding box unchecked
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(2.0, 2.0);
    /// let bbox = BoundingBox2D::new_unchecked(ll, ur);
    /// ```
    ///
    pub fn new_unchecked(
        lower_left_coordinate: Coordinate2D,
        upper_right_coordinate: Coordinate2D,
    ) -> Self {
        Self {
            lower_left_coordinate,
            upper_right_coordinate,
        }
    }

    /// Creates a new bounding box with `upper_left` and `lower_right` coordinates
    /// This is usually used with raster data and matches with the gdal geotransform
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ul = Coordinate2D::new(1.0, 2.0);
    /// let lr = Coordinate2D::new(2.0, 1.0);
    /// let bbox = BoundingBox2D::new_upper_left_lower_right(ul, lr).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// This constructor fails if the order of coordinates is not correct
    ///
    pub fn new_upper_left_lower_right(
        upper_left_coordinate: Coordinate2D,
        lower_right_coordinate: Coordinate2D,
    ) -> Result<Self> {
        let lower_left_coordinate = (upper_left_coordinate.x, lower_right_coordinate.y).into();
        let upper_right_coordinate = (lower_right_coordinate.x, upper_left_coordinate.y).into();
        BoundingBox2D::new(lower_left_coordinate, upper_right_coordinate)
    }

    /// Creates a new bounding box with `upper_left` and `lower_right` coordinates unchecked
    /// This is usually used with raster data and matches with the gdal geotransform
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ul = Coordinate2D::new(1.0, 2.0);
    /// let lr = Coordinate2D::new(2.0, 1.0);
    /// let bbox = BoundingBox2D::new_upper_left_lower_right_unchecked(ul, lr);
    /// ```
    ///
    pub fn new_upper_left_lower_right_unchecked(
        upper_left_coordinate: Coordinate2D,
        lower_right_coordinate: Coordinate2D,
    ) -> Self {
        let lower_left_coordinate = (upper_left_coordinate.x, lower_right_coordinate.y).into();
        let upper_right_coordinate = (lower_right_coordinate.x, upper_left_coordinate.y).into();
        BoundingBox2D::new_unchecked(lower_left_coordinate, upper_right_coordinate)
    }

    /// Returns the `Coordnate2D` representing the lower left edge of the bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(2.0, 2.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// assert_eq!(bbox.lower_left(), (1.0, 1.0).into());
    /// ```
    ///
    pub fn lower_left(&self) -> Coordinate2D {
        self.lower_left_coordinate
    }

    /// Returns the `Coordnate2D` representing the upper right edge of the bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(2.0, 2.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// assert_eq!(bbox.upper_right(), (2.0, 2.0).into());
    /// ```
    ///
    pub fn upper_right(&self) -> Coordinate2D {
        self.upper_right_coordinate
    }

    /// Returns the `Coordnate2D` representing the upper left edge of the bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(2.0, 2.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// assert_eq!(bbox.upper_left(), (1.0, 2.0).into());
    /// ```
    ///
    pub fn upper_left(&self) -> Coordinate2D {
        (self.lower_left_coordinate.x, self.upper_right_coordinate.y).into()
    }

    /// Returns the `Coordnate2D` representing the upper right edge of the bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(2.0, 2.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// assert_eq!(bbox.upper_left(), (1.0, 2.0).into());
    /// ```
    ///
    pub fn lower_right(&self) -> Coordinate2D {
        (self.upper_right_coordinate.x, self.lower_left_coordinate.y).into()
    }

    /// Checks if a coordinate is located inside the bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(2.0, 2.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// assert!(bbox.contains_coordinate(&(1.5, 1.5).into()));
    /// ```
    ///
    pub fn contains_coordinate(&self, coordinate: &Coordinate2D) -> bool {
        coordinate.x >= self.lower_left_coordinate.x
            && coordinate.y >= self.lower_left_coordinate.y
            && coordinate.x <= self.upper_right_coordinate.x
            && coordinate.y <= self.upper_right_coordinate.y
    }

    /// Checks if the bounding box contains another bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(4.0, 4.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// let ll_in = Coordinate2D::new(2.0, 2.0);
    /// let ur_in = Coordinate2D::new(3.0, 3.0);
    /// let bbox_in = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// assert!(bbox.contains_bbox(&bbox_in));
    /// ```
    ///
    pub fn contains_bbox(&self, other_bbox: &Self) -> bool {
        other_bbox.lower_left_coordinate.x >= self.lower_left_coordinate.x
            && other_bbox.lower_left_coordinate.y >= self.lower_left_coordinate.y
            && other_bbox.upper_right_coordinate.x <= self.upper_right_coordinate.x
            && other_bbox.upper_right_coordinate.y <= self.upper_right_coordinate.y
    }

    /// Checks if the bounding box contains another bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(4.0, 4.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// let ll_inside = Coordinate2D::new(2.0, 2.0);
    /// let ur_inside = Coordinate2D::new(3.0, 3.0);
    /// let bbox_inside = BoundingBox2D::new(ll_inside, ur_inside).unwrap();
    /// assert!(bbox.overlaps_bbox(&bbox_inside));
    /// ```
    ///
    pub fn overlaps_bbox(&self, other_bbox: &Self) -> bool {
        let no_overlap = self.lower_left_coordinate.x > other_bbox.upper_right_coordinate.x
            || other_bbox.lower_left_coordinate.x > self.upper_right_coordinate.x
            || self.lower_left_coordinate.y > other_bbox.upper_right_coordinate.y
            || other_bbox.lower_left_coordinate.y > self.upper_right_coordinate.y;
        !no_overlap
    }

    /// Returns the `Coordnate2D` representing the upper right edge of the bounding box
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let ll = Coordinate2D::new(1.0, 1.0);
    /// let ur = Coordinate2D::new(4.0, 4.0);
    /// let bbox = BoundingBox2D::new(ll, ur).unwrap();
    ///
    /// let ll_intersect = Coordinate2D::new(2.0, 2.0);
    /// let ur_intersect = Coordinate2D::new(5.0, 5.0);
    /// let bbox_intersect = BoundingBox2D::new(ll_intersect, ur_intersect).unwrap();
    /// assert!(bbox.intersects_bbox(&bbox_intersect));
    /// ```
    ///
    pub fn intersects_bbox(&self, other_bbox: &Self) -> bool {
        self.overlaps_bbox(other_bbox)
            && !(self.contains_bbox(other_bbox) || other_bbox.contains_bbox(self))
    }
}

#[cfg(test)]
mod tests {
    use crate::primitives::{BoundingBox2D, Coordinate2D};
    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_new() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        assert_eq!(bbox.lower_left_coordinate.x, 1.0);
        assert_eq!(bbox.lower_left_coordinate.y, 1.0);
        assert_eq!(bbox.upper_right_coordinate.x, 2.0);
        assert_eq!(bbox.upper_right_coordinate.y, 2.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_new_unchecked() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new_unchecked(ll, ur);
        assert_eq!(bbox.lower_left_coordinate.x, 1.0);
        assert_eq!(bbox.lower_left_coordinate.y, 1.0);
        assert_eq!(bbox.upper_right_coordinate.x, 2.0);
        assert_eq!(bbox.upper_right_coordinate.y, 2.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_new_ul_lr() {
        let ul = Coordinate2D::new(1.0, 2.0);
        let lr = Coordinate2D::new(2.0, 1.0);
        let bbox = BoundingBox2D::new_upper_left_lower_right(ul, lr).unwrap();
        assert_eq!(bbox.lower_left_coordinate.x, 1.0);
        assert_eq!(bbox.lower_left_coordinate.y, 1.0);
        assert_eq!(bbox.upper_right_coordinate.x, 2.0);
        assert_eq!(bbox.upper_right_coordinate.y, 2.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_new_ul_lr_unchecked() {
        let ul = Coordinate2D::new(1.0, 2.0);
        let lr = Coordinate2D::new(2.0, 1.0);
        let bbox = BoundingBox2D::new_upper_left_lower_right_unchecked(ul, lr);

        assert_eq!(bbox.lower_left_coordinate.x, 1.0);
        assert_eq!(bbox.lower_left_coordinate.y, 1.0);
        assert_eq!(bbox.upper_right_coordinate.x, 2.0);
        assert_eq!(bbox.upper_right_coordinate.y, 2.0);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_lower_left() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        assert_eq!(bbox.lower_left(), (1.0, 1.0).into());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_lower_right() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        assert_eq!(bbox.lower_right(), (2.0, 1.0).into());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_upper_right() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        assert_eq!(bbox.upper_right(), (2.0, 2.0).into());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_upper_left() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        assert_eq!(bbox.upper_left(), (1.0, 2.0).into());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn bounding_box_contains_coordinate() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        assert!(bbox.contains_coordinate(&(1.5, 1.5).into()));
    }

    #[test]
    fn bounding_box_contains_bbox() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(4.0, 4.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_in = Coordinate2D::new(2.0, 2.0);
        let ur_in = Coordinate2D::new(3.0, 3.0);
        let bbox_in = BoundingBox2D::new(ll_in, ur_in).unwrap();

        assert!(bbox.contains_bbox(&bbox_in));
    }

    #[test]
    fn bounding_box_contains_bbox_overlap() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(4.0, 4.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_in = Coordinate2D::new(2.0, 2.0);
        let ur_in = Coordinate2D::new(5.0, 5.0);
        let bbox_in = BoundingBox2D::new(ll_in, ur_in).unwrap();

        assert!(!bbox.contains_bbox(&bbox_in));
    }

    #[test]
    fn bounding_box_contains_bbox_seperate() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_in = Coordinate2D::new(3.0, 3.0);
        let ur_in = Coordinate2D::new(5.0, 5.0);
        let bbox_in = BoundingBox2D::new(ll_in, ur_in).unwrap();

        assert!(!bbox.contains_bbox(&bbox_in));
    }

    #[test]
    fn bounding_box_overlaps_bbox() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(4.0, 4.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_inside = Coordinate2D::new(2.0, 2.0);
        let ur_inside = Coordinate2D::new(3.0, 3.0);
        let bbox_inside = BoundingBox2D::new(ll_inside, ur_inside).unwrap();
        assert!(bbox.overlaps_bbox(&bbox_inside));
    }

    #[test]
    fn bounding_box_overlaps_bbox_intersect() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(4.0, 4.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_intersect = Coordinate2D::new(2.0, 2.0);
        let ur_intersect = Coordinate2D::new(3.0, 3.0);
        let bbox_intersect = BoundingBox2D::new(ll_intersect, ur_intersect).unwrap();
        assert!(bbox.overlaps_bbox(&bbox_intersect));
    }

    #[test]
    fn bounding_box_overlaps_bbox_seperate() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_seperate = Coordinate2D::new(3.0, 3.0);
        let ur_seperate = Coordinate2D::new(4.0, 4.0);
        let bbox_seperate = BoundingBox2D::new(ll_seperate, ur_seperate).unwrap();
        assert!(!bbox.overlaps_bbox(&bbox_seperate));
    }

    #[test]
    fn bounding_box_intersects_bbox() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(4.0, 4.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_intersect = Coordinate2D::new(2.0, 2.0);
        let ur_intersect = Coordinate2D::new(5.0, 5.0);
        let bbox_intersect = BoundingBox2D::new(ll_intersect, ur_intersect).unwrap();
        assert!(bbox.intersects_bbox(&bbox_intersect));
    }

    #[test]
    fn bounding_box_intersects_bbox_inside() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(4.0, 4.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_inside = Coordinate2D::new(2.0, 2.0);
        let ur_inside = Coordinate2D::new(3.0, 3.0);
        let bbox_inside = BoundingBox2D::new(ll_inside, ur_inside).unwrap();
        assert!(!bbox.intersects_bbox(&bbox_inside));
    }

    #[test]
    fn bounding_box_intersects_bbox_separate() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_separate = Coordinate2D::new(3.0, 3.0);
        let ur_separate = Coordinate2D::new(5.0, 5.0);
        let bbox_separate = BoundingBox2D::new(ll_separate, ur_separate).unwrap();
        assert!(!bbox.intersects_bbox(&bbox_separate));
    }
}
