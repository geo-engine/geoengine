use super::{Coordinate2D, SpatialBounded};
use crate::error;
use crate::util::Result;
#[cfg(feature = "postgres")]
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use snafu::ensure;

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug)]
#[cfg_attr(feature = "postgres", derive(ToSql, FromSql))]
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
                && lower_left_coordinate.y <= upper_right_coordinate.y,
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

    /// Returns the width of the bounding box
    pub fn size_x(&self) -> f64 {
        self.upper_right_coordinate.x - self.lower_left_coordinate.x
    }

    /// Returns the height of the bounding box
    pub fn size_y(&self) -> f64 {
        self.upper_right_coordinate.y - self.lower_left_coordinate.y
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
        self.contains_x(other_bbox) && self.contains_y(other_bbox)
    }

    fn contains_x(&self, other_bbox: &Self) -> bool {
        crate::util::ranges::value_in_range_inclusive(
            other_bbox.lower_left().x,
            self.lower_left().x,
            self.upper_right().x,
        ) && crate::util::ranges::value_in_range_inclusive(
            other_bbox.upper_right().x,
            self.lower_left().x,
            self.upper_right().x,
        )
    }

    fn contains_y(&self, other_bbox: &Self) -> bool {
        crate::util::ranges::value_in_range_inclusive(
            other_bbox.lower_left().y,
            self.lower_left().y,
            self.upper_right().y,
        ) && crate::util::ranges::value_in_range_inclusive(
            other_bbox.upper_right().y,
            self.lower_left().y,
            self.upper_right().y,
        )
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
    /// let ur_inside = Coordinate2D::new(5.0, 5.0);
    /// let bbox_inside = BoundingBox2D::new(ll_inside, ur_inside).unwrap();
    /// assert!(bbox.overlaps_bbox(&bbox_inside));
    /// ```
    ///
    pub fn overlaps_bbox(&self, other_bbox: &Self) -> bool {
        (self.overlap_x(other_bbox) && self.overlap_y(other_bbox))
            && (!self.contains_x(other_bbox) || !self.contains_y(other_bbox))
    }

    fn overlap_x(&self, other_bbox: &Self) -> bool {
        crate::util::ranges::value_in_range_inclusive(
            self.lower_left().x,
            other_bbox.lower_left().x,
            other_bbox.upper_right().x,
        ) || crate::util::ranges::value_in_range_inclusive(
            other_bbox.lower_left().x,
            self.lower_left().x,
            self.upper_right().x,
        )
    }

    fn overlap_y(&self, other_bbox: &Self) -> bool {
        crate::util::ranges::value_in_range_inclusive(
            self.lower_left().y,
            other_bbox.lower_left().y,
            other_bbox.upper_right().y,
        ) || crate::util::ranges::value_in_range_inclusive(
            other_bbox.lower_left().y,
            self.lower_left().y,
            self.upper_right().y,
        )
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
        self == other_bbox || (self.overlap_x(other_bbox) && self.overlap_y(other_bbox))

        // this should be (more or less) equiv
        //self == other_bbox
        //    || self.overlaps_bbox(other_bbox)
        //    || self.contains_bbox(other_bbox)
        //    || other_bbox.contains_bbox(self)
        //    || self.touches_bbox(other_bbox)
    }

    /// Returns `Some(intersection)` with `other_bbox` or `None` if they do not intersect
    ///
    /// # Examples
    ///
    /// ```
    /// use geoengine_datatypes::primitives::{Coordinate2D, BoundingBox2D};
    ///
    /// let bbox = BoundingBox2D::new((0.0, 0.0).into(), (10.0, 10.0).into()).unwrap();
    /// let bbox2 = BoundingBox2D::new((5.0, 5.0).into(), (15.0, 15.0).into()).unwrap();
    ///
    /// let intersection = BoundingBox2D::new((5.0, 5.0).into(), (10.0, 10.0).into()).unwrap();
    ///
    /// assert_eq!(bbox.intersection(&bbox2), Some(intersection));
    /// ```
    ///
    pub fn intersection(&self, other_bbox: &Self) -> Option<Self> {
        if self.intersects_bbox(other_bbox) {
            let ll_x = f64::max(
                self.lower_left_coordinate.x,
                other_bbox.lower_left_coordinate.x,
            );
            let ll_y = f64::max(
                self.lower_left_coordinate.y,
                other_bbox.lower_left_coordinate.y,
            );
            let ur_x = f64::min(
                self.upper_right_coordinate.x,
                other_bbox.upper_right_coordinate.x,
            );
            let ur_y = f64::min(
                self.upper_right_coordinate.y,
                other_bbox.upper_right_coordinate.y,
            );

            Some(BoundingBox2D::new_unchecked(
                (ll_x, ll_y).into(),
                (ur_x, ur_y).into(),
            ))
        } else {
            None
        }
    }

    pub fn extend_with_coord(&mut self, coord: Coordinate2D) {
        self.lower_left_coordinate = self.lower_left_coordinate.min_elements(coord);
        self.upper_right_coordinate = self.upper_right_coordinate.max_elements(coord);
    }

    pub fn from_coord_iter<I: IntoIterator<Item = Coordinate2D>>(iter: I) -> Option<Self> {
        let mut iterator = iter.into_iter();

        let first = iterator.next().map(|c| BoundingBox2D::new_unchecked(c, c));

        first.map(|mut f| {
            for c in iterator {
                f.extend_with_coord(c);
            }
            f
        })
    }

    pub fn from_coord_ref_iter<'l, I: IntoIterator<Item = &'l Coordinate2D>>(
        iter: I,
    ) -> Option<Self> {
        let mut iterator = iter.into_iter();

        let first = iterator.next().map(|&c| BoundingBox2D::new_unchecked(c, c));

        first.map(|mut f| {
            for &c in iterator {
                f.extend_with_coord(c);
            }
            f
        })
    }
}

impl From<BoundingBox2D> for geo::Rect<f64> {
    fn from(bbox: BoundingBox2D) -> geo::Rect<f64> {
        Self::from(&bbox)
    }
}

impl From<&BoundingBox2D> for geo::Rect<f64> {
    fn from(bbox: &BoundingBox2D) -> geo::Rect<f64> {
        geo::Rect::new(bbox.lower_left_coordinate, bbox.upper_right_coordinate)
    }
}

impl SpatialBounded for BoundingBox2D {
    fn spatial_bounds(&self) -> BoundingBox2D {
        *self
    }
}

#[cfg(test)]
mod tests {

    use crate::primitives::{BoundingBox2D, Coordinate2D, SpatialBounded};
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
        assert!(bbox.contains_coordinate(&bbox.lower_left()));
        assert!(bbox.contains_coordinate(&bbox.upper_left()));
        assert!(bbox.contains_coordinate(&bbox.upper_right()));
        assert!(bbox.contains_coordinate(&bbox.lower_right()));
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
    fn bounding_box_x_overlap() {
        let x0_y0 = Coordinate2D::new(0.0, 0.0);
        let x1_y1 = Coordinate2D::new(1.0, 1.0);
        let x2_y1 = Coordinate2D::new(2.0, 1.0);
        // let x3_y1 = Coordinate2D::new(3.0, 1.0);
        // let x4_y1 = Coordinate2D::new(4.0, 1.0);
        let x1_y2 = Coordinate2D::new(1.0, 2.0);
        let x2_y2 = Coordinate2D::new(2.0, 2.0);
        // let x3_y2 = Coordinate2D::new(3.0, 2.0);
        // let x4_y2 = Coordinate2D::new(4.0, 2.0);
        // let x1_y3 = Coordinate2D::new(1.0, 3.0);
        // let x2_y3 = Coordinate2D::new(2.0, 3.0);
        let x3_y3 = Coordinate2D::new(3.0, 3.0);
        let x4_y3 = Coordinate2D::new(4.0, 3.0);
        // let x1_y4 = Coordinate2D::new(1.0, 4.0);
        // let x2_y4 = Coordinate2D::new(2.0, 4.0);
        let x3_y4 = Coordinate2D::new(3.0, 4.0);
        let x4_y4 = Coordinate2D::new(4.0, 4.0);

        let outer_box_up_left = BoundingBox2D::new(x0_y0, x1_y1).unwrap();
        let box_up_left = BoundingBox2D::new(x1_y1, x3_y3).unwrap();
        let box_up_right = BoundingBox2D::new(x2_y1, x4_y3).unwrap();
        let box_down_left = BoundingBox2D::new(x1_y2, x3_y4).unwrap();
        let box_down_right = BoundingBox2D::new(x2_y2, x4_y4).unwrap();
        let box_center = BoundingBox2D::new(x2_y2, x3_y3).unwrap();

        assert!(outer_box_up_left.overlap_x(&box_up_left));
        assert!(!outer_box_up_left.overlap_x(&box_up_right));
        assert!(outer_box_up_left.overlap_x(&box_down_left));
        assert!(!outer_box_up_left.overlap_x(&box_down_right));
        assert!(!outer_box_up_left.overlap_x(&box_center));

        assert!(!box_center.overlap_x(&outer_box_up_left));
        assert!(box_center.overlap_x(&box_up_left));
        assert!(box_center.overlap_x(&box_up_right));
        assert!(box_center.overlap_x(&box_down_left));
        assert!(box_center.overlap_x(&box_down_right));
        assert!(box_center.overlap_x(&box_center));

        assert!(box_up_left.overlap_x(&outer_box_up_left));
        assert!(box_up_left.overlap_x(&box_up_left));
        assert!(box_up_left.overlap_x(&box_up_right));
        assert!(box_up_left.overlap_x(&box_down_left));
        assert!(box_up_left.overlap_x(&box_down_right));
        assert!(box_up_left.overlap_x(&outer_box_up_left));

        assert!(!box_up_right.overlap_x(&outer_box_up_left));
        assert!(box_up_right.overlap_x(&box_up_left));
        assert!(box_up_right.overlap_x(&box_up_right));
        assert!(box_up_right.overlap_x(&box_down_left));
        assert!(box_up_right.overlap_x(&box_down_right));
        assert!(box_up_right.overlap_x(&box_center));

        assert!(box_down_left.overlap_x(&outer_box_up_left));
        assert!(box_down_left.overlap_x(&box_up_left));
        assert!(box_down_left.overlap_x(&box_up_right));
        assert!(box_down_left.overlap_x(&box_down_left));
        assert!(box_down_left.overlap_x(&box_down_right));
        assert!(box_down_left.overlap_x(&box_center));

        assert!(!box_down_right.overlap_x(&outer_box_up_left));
        assert!(box_down_right.overlap_x(&box_up_left));
        assert!(box_down_right.overlap_x(&box_up_right));
        assert!(box_down_right.overlap_x(&box_down_left));
        assert!(box_down_right.overlap_x(&box_down_right));
        assert!(box_down_right.overlap_x(&box_center));
    }

    #[test]
    fn bounding_box_y_overlap() {
        let x0_y0 = Coordinate2D::new(0.0, 0.0);
        let x1_y1 = Coordinate2D::new(1.0, 1.0);
        let x2_y1 = Coordinate2D::new(2.0, 1.0);
        // let x3_y1 = Coordinate2D::new(3.0, 1.0);
        // let x4_y1 = Coordinate2D::new(4.0, 1.0);
        let x1_y2 = Coordinate2D::new(1.0, 2.0);
        let x2_y2 = Coordinate2D::new(2.0, 2.0);
        // let x3_y2 = Coordinate2D::new(3.0, 2.0);
        // let x4_y2 = Coordinate2D::new(4.0, 2.0);
        // let x1_y3 = Coordinate2D::new(1.0, 3.0);
        // let x2_y3 = Coordinate2D::new(2.0, 3.0);
        let x3_y3 = Coordinate2D::new(3.0, 3.0);
        let x4_y3 = Coordinate2D::new(4.0, 3.0);
        // let x1_y4 = Coordinate2D::new(1.0, 4.0);
        // let x2_y4 = Coordinate2D::new(2.0, 4.0);
        let x3_y4 = Coordinate2D::new(3.0, 4.0);
        let x4_y4 = Coordinate2D::new(4.0, 4.0);

        let outer_box_up_left = BoundingBox2D::new(x0_y0, x1_y1).unwrap();
        let box_up_left = BoundingBox2D::new(x1_y1, x3_y3).unwrap();
        let box_up_right = BoundingBox2D::new(x2_y1, x4_y3).unwrap();
        let box_down_left = BoundingBox2D::new(x1_y2, x3_y4).unwrap();
        let box_down_right = BoundingBox2D::new(x2_y2, x4_y4).unwrap();
        let box_center = BoundingBox2D::new(x2_y2, x3_y3).unwrap();

        assert!(outer_box_up_left.overlap_y(&box_up_left));
        assert!(outer_box_up_left.overlap_y(&box_up_right));
        assert!(!outer_box_up_left.overlap_y(&box_down_left));
        assert!(!outer_box_up_left.overlap_y(&box_down_right));
        assert!(!outer_box_up_left.overlap_y(&box_center));

        assert!(box_center.overlap_y(&box_up_left));
        assert!(box_center.overlap_y(&box_up_right));
        assert!(box_center.overlap_y(&box_down_left));
        assert!(box_center.overlap_y(&box_down_right));

        assert!(box_up_left.overlap_y(&box_up_right));
        assert!(box_up_left.overlap_y(&box_down_left));
        assert!(box_up_left.overlap_y(&box_down_right));

        assert!(box_up_right.overlap_y(&box_down_left));
        assert!(box_up_right.overlap_y(&box_down_right));

        assert!(box_down_left.overlap_y(&box_down_right));
    }

    #[test]
    fn bounding_box_overlaps() {
        let x0_y0 = Coordinate2D::new(0.0, 0.0);
        let x1_y1 = Coordinate2D::new(1.0, 1.0);
        let x2_y1 = Coordinate2D::new(2.0, 1.0);
        // let x3_y1 = Coordinate2D::new(3.0, 1.0);
        // let x4_y1 = Coordinate2D::new(4.0, 1.0);
        let x1_y2 = Coordinate2D::new(1.0, 2.0);
        let x2_y2 = Coordinate2D::new(2.0, 2.0);
        // let x3_y2 = Coordinate2D::new(3.0, 2.0);
        // let x4_y2 = Coordinate2D::new(4.0, 2.0);
        // let x1_y3 = Coordinate2D::new(1.0, 3.0);
        // let x2_y3 = Coordinate2D::new(2.0, 3.0);
        let x3_y3 = Coordinate2D::new(3.0, 3.0);
        let x4_y3 = Coordinate2D::new(4.0, 3.0);
        // let x1_y4 = Coordinate2D::new(1.0, 4.0);
        // let x2_y4 = Coordinate2D::new(2.0, 4.0);
        let x3_y4 = Coordinate2D::new(3.0, 4.0);
        let x4_y4 = Coordinate2D::new(4.0, 4.0);

        let outer_box_up_left = BoundingBox2D::new(x0_y0, x1_y1).unwrap();
        let box_up_left = BoundingBox2D::new(x1_y1, x3_y3).unwrap();
        let box_up_right = BoundingBox2D::new(x2_y1, x4_y3).unwrap();
        let box_down_left = BoundingBox2D::new(x1_y2, x3_y4).unwrap();
        let box_down_right = BoundingBox2D::new(x2_y2, x4_y4).unwrap();
        let box_center = BoundingBox2D::new(x2_y2, x3_y3).unwrap();

        assert!(outer_box_up_left.overlaps_bbox(&box_up_left));
        assert!(!outer_box_up_left.overlaps_bbox(&box_up_right));
        assert!(!outer_box_up_left.overlaps_bbox(&box_down_left));
        assert!(!outer_box_up_left.overlaps_bbox(&box_down_right));
        assert!(!outer_box_up_left.overlaps_bbox(&box_center));

        assert!(box_center.overlaps_bbox(&box_up_left));
        assert!(box_center.overlaps_bbox(&box_up_right));
        assert!(box_center.overlaps_bbox(&box_down_left));
        assert!(box_center.overlaps_bbox(&box_down_right));

        assert!(box_up_left.overlaps_bbox(&box_up_right));
        assert!(box_up_left.overlaps_bbox(&box_down_left));
        assert!(box_up_left.overlaps_bbox(&box_down_right));

        assert!(box_up_right.overlaps_bbox(&box_down_left));
        assert!(box_up_right.overlaps_bbox(&box_down_right));

        assert!(box_down_left.overlaps_bbox(&box_down_right));
    }

    #[test]
    fn bounding_box_no_overlaps() {
        let x0_y0 = Coordinate2D::new(0.0, 0.0);
        let x1_y1 = Coordinate2D::new(1.0, 1.0);

        let x0_y5 = Coordinate2D::new(0.0, 5.0);
        let x1_y6 = Coordinate2D::new(1.0, 6.0);

        let x3_y3 = Coordinate2D::new(3.0, 3.0);
        let x4_y4 = Coordinate2D::new(4.0, 4.0);

        let x5_y0 = Coordinate2D::new(5.0, 0.0);
        let x6_y1 = Coordinate2D::new(6.0, 1.0);

        let x5_y5 = Coordinate2D::new(5.0, 5.0);
        let x6_y6 = Coordinate2D::new(6.0, 6.0);

        let box_up_left = BoundingBox2D::new(x0_y0, x1_y1).unwrap();
        let box_up_right = BoundingBox2D::new(x0_y5, x1_y6).unwrap();
        let box_down_left = BoundingBox2D::new(x5_y0, x6_y1).unwrap();
        let box_down_right = BoundingBox2D::new(x5_y5, x6_y6).unwrap();
        let box_center = BoundingBox2D::new(x3_y3, x4_y4).unwrap();

        assert!(!box_center.overlaps_bbox(&box_up_left));
        assert!(!box_center.overlaps_bbox(&box_up_right));
        assert!(!box_center.overlaps_bbox(&box_down_left));
        assert!(!box_center.overlaps_bbox(&box_down_right));

        assert!(!box_up_left.overlaps_bbox(&box_up_right));
        assert!(!box_up_left.overlaps_bbox(&box_down_left));
        assert!(!box_up_left.overlaps_bbox(&box_down_right));

        assert!(!box_down_right.overlaps_bbox(&box_down_left));
        assert!(!box_down_right.overlaps_bbox(&box_down_right));

        assert!(!box_down_left.overlaps_bbox(&box_up_right));
    }

    #[test]
    fn bounding_box_intersect_within() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(4.0, 4.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_intersect = Coordinate2D::new(2.0, 2.0);
        let ur_intersect = Coordinate2D::new(3.0, 3.0);
        let bbox_intersect = BoundingBox2D::new(ll_intersect, ur_intersect).unwrap();
        assert!(bbox.intersects_bbox(&bbox_intersect));
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
        assert!(bbox.intersects_bbox(&bbox_inside));
    }

    #[test]
    fn bounding_box_intersects_bbox_separate() {
        let ll = Coordinate2D::new(1.0, 1.0);
        let ur = Coordinate2D::new(2.0, 2.0);
        let bbox = BoundingBox2D::new(ll, ur).unwrap();

        let ll_separate = Coordinate2D::new(3.0, 3.0);
        let ur_separate = Coordinate2D::new(5.0, 5.0);
        let bbox_separate = BoundingBox2D::new(ll_separate, ur_separate).unwrap();

        assert!(!bbox.contains_bbox(&bbox_separate));
        assert!(!bbox_separate.contains_bbox(&bbox));

        assert!(!bbox.overlaps_bbox(&bbox_separate));
        assert!(!bbox_separate.overlaps_bbox(&bbox));

        assert!(!bbox.intersects_bbox(&bbox_separate));
    }
    #[test]
    fn bounding_box_intersect_real_tiles() {
        let query = BoundingBox2D {
            lower_left_coordinate: Coordinate2D { x: 10.0, y: 20.0 },
            upper_right_coordinate: Coordinate2D { x: 70.0, y: 80.0 },
        };
        let tile_0_0 = BoundingBox2D {
            lower_left_coordinate: Coordinate2D { x: -180.0, y: 30.0 },
            upper_right_coordinate: Coordinate2D { x: -120.0, y: 90.0 },
        };
        assert!(!query.intersects_bbox(&tile_0_0));

        let tile_0_1 = BoundingBox2D {
            lower_left_coordinate: Coordinate2D { x: -120.0, y: 30.0 },
            upper_right_coordinate: Coordinate2D { x: -60.0, y: 90.0 },
        };
        assert!(!query.intersects_bbox(&tile_0_1));

        let tile_0_2 = BoundingBox2D {
            lower_left_coordinate: Coordinate2D { x: -60.0, y: 30.0 },
            upper_right_coordinate: Coordinate2D { x: 0.0, y: 90.0 },
        };
        assert!(!query.intersects_bbox(&tile_0_2));

        let tile_0_3 = BoundingBox2D {
            lower_left_coordinate: Coordinate2D { x: 0.0, y: 30.0 },
            upper_right_coordinate: Coordinate2D { x: 60.0, y: 90.0 },
        };
        assert!(query.intersects_bbox(&tile_0_3));

        let tile_0_4 = BoundingBox2D {
            lower_left_coordinate: Coordinate2D { x: 60.0, y: 30.0 },
            upper_right_coordinate: Coordinate2D { x: 120.0, y: 90.0 },
        };
        assert!(query.intersects_bbox(&tile_0_4));

        let tile_0_5 = BoundingBox2D {
            lower_left_coordinate: Coordinate2D { x: 120.0, y: 30.0 },
            upper_right_coordinate: Coordinate2D { x: 180.0, y: 90.0 },
        };
        assert!(!query.intersects_bbox(&tile_0_5));
    }

    #[test]
    fn intersects_inner() {
        let bbox = BoundingBox2D::new((0.0, 0.0).into(), (15.0, 15.0).into()).unwrap();
        let bbox2 = BoundingBox2D::new((5.0, 5.0).into(), (10.0, 10.0).into()).unwrap();

        assert_eq!(bbox.intersection(&bbox2), Some(bbox2));
    }

    #[test]
    fn spatial_bounds() {
        let bbox = BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into());
        assert_eq!(bbox, bbox.spatial_bounds())
    }

    #[test]
    fn add_coord_outside() {
        let mut bbox = BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into());
        bbox.extend_with_coord(Coordinate2D::new(-1., 1.5));
        let expect = BoundingBox2D::new_unchecked((-1., 0.).into(), (1., 1.5).into());
        assert_eq!(bbox, expect)
    }

    #[test]
    fn from_coord_iter() {
        let expected = BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into());

        let coordinates: Vec<Coordinate2D> = Vec::from([
            (1., 0.4).into(),
            (0.8, 0.0).into(),
            (0.3, 0.1).into(),
            (0.0, 1.0).into(),
        ]);
        let bbox = BoundingBox2D::from_coord_iter(coordinates).unwrap();
        assert_eq!(bbox, expected)
    }

    #[test]
    fn from_coord_ref_iter() {
        let expected = BoundingBox2D::new_unchecked((0., 0.).into(), (1., 1.).into());

        let coordinates: Vec<Coordinate2D> = Vec::from([
            (1., 0.4).into(),
            (0.8, 0.0).into(),
            (0.3, 0.1).into(),
            (0.0, 1.0).into(),
        ]);
        let bbox = BoundingBox2D::from_coord_ref_iter(coordinates.iter()).unwrap();
        assert_eq!(bbox, expected)
    }
}
