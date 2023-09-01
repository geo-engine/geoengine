use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::error;
use crate::raster::GridShape2D;
use crate::util::helpers::snap_next;
use crate::util::helpers::snap_prev;
use crate::util::Result;

use super::BoundingBox2D;
use super::Coordinate2D;
use super::SpatialResolution;

/// Common trait for axis-parallel boxes
pub trait AxisAlignedRectangle: Copy {
    /// create a new instance defined by `min` (lower left) and `max` (upper right) coordinate
    fn from_min_max(min: Coordinate2D, max: Coordinate2D) -> Result<Self>;

    fn lower_left(&self) -> Coordinate2D;
    fn upper_left(&self) -> Coordinate2D;
    fn upper_right(&self) -> Coordinate2D;
    fn lower_right(&self) -> Coordinate2D;

    fn size_x(&self) -> f64;
    fn size_y(&self) -> f64;

    fn intersection(&self, other: &Self) -> Option<Self>;

    /// create a `BoundingBox2D` with `self.lower_left()` and `self.upper_right()`
    fn as_bbox(&self) -> BoundingBox2D;
}

/// A partition of space that include the upper left but excludes the lower right coordinate
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpatialPartition2D {
    upper_left_coordinate: Coordinate2D,
    lower_right_coordinate: Coordinate2D,
}

impl SpatialPartition2D {
    pub fn new(
        upper_left_coordinate: Coordinate2D,
        lower_right_coordinate: Coordinate2D,
    ) -> Result<Self> {
        ensure!(
            upper_left_coordinate.x < lower_right_coordinate.x
                && upper_left_coordinate.y > lower_right_coordinate.y,
            error::InvalidSpatialPartition {
                upper_left_coordinate,
                lower_right_coordinate
            }
        );
        Ok(Self {
            upper_left_coordinate,
            lower_right_coordinate,
        })
    }

    pub fn new_unchecked(
        upper_left_coordinate: Coordinate2D,
        lower_right_coordinate: Coordinate2D,
    ) -> Self {
        Self {
            upper_left_coordinate,
            lower_right_coordinate,
        }
    }

    /// Create a partition from a bbox by snapping to the next pixel
    /// The resulting partition is not equivalent to the bbox but contains it
    pub fn with_bbox_and_resolution(bbox: BoundingBox2D, resolution: SpatialResolution) -> Self {
        // TODO: replace with pixel snapping once raster bounds use pixels
        const EPSILON: f64 = 0.000_001;

        let upper_left = bbox.upper_left();

        let x_pixels = ((bbox.size_x() + EPSILON * resolution.x) / resolution.x).ceil();
        let y_pixels = ((bbox.size_y() + EPSILON * resolution.y) / resolution.y).ceil();

        let snapped_lower_right = Coordinate2D::new(
            upper_left.x + x_pixels * resolution.x,
            upper_left.y - y_pixels * resolution.y,
        );

        SpatialPartition2D::new_unchecked(upper_left, snapped_lower_right)
    }

    /// Checks if a coordinate is located inside spatial partition
    pub fn contains_coordinate(&self, coordinate: &Coordinate2D) -> bool {
        coordinate.x >= self.upper_left_coordinate.x
            && coordinate.y <= self.upper_left_coordinate.y
            && coordinate.x < self.lower_right_coordinate.x
            && coordinate.y > self.lower_right_coordinate.y
    }

    /// Return true if the `other` partition has any space in common with the partition
    pub fn intersects(&self, other: &SpatialPartition2D) -> bool {
        let overlap_x = crate::util::ranges::value_in_range(
            self.upper_left_coordinate.x,
            other.upper_left_coordinate.x,
            other.lower_right_coordinate.x,
        ) || crate::util::ranges::value_in_range(
            other.upper_left_coordinate.x,
            self.upper_left_coordinate.x,
            self.lower_right_coordinate.x,
        );

        let overlap_y = crate::util::ranges::value_in_range_inv(
            self.upper_left_coordinate.y,
            other.lower_right_coordinate.y,
            other.upper_left_coordinate.y,
        ) || crate::util::ranges::value_in_range_inv(
            other.upper_left_coordinate.y,
            self.lower_right_coordinate.y,
            self.upper_left_coordinate.y,
        );

        overlap_x && overlap_y
    }

    /// Returns true if the given bbox has any space in common with the partition
    pub fn intersects_bbox(&self, bbox: &BoundingBox2D) -> bool {
        let overlap_x = crate::util::ranges::value_in_range(
            self.upper_left_coordinate.x,
            bbox.lower_left().x,
            bbox.upper_right().x,
        ) || crate::util::ranges::value_in_range(
            bbox.lower_left().x,
            self.upper_left_coordinate.x,
            self.lower_right_coordinate.x,
        );

        let overlap_y = crate::util::ranges::value_in_range(
            self.upper_left_coordinate.y,
            bbox.lower_left().y,
            bbox.upper_right().y,
        ) || crate::util::ranges::value_in_range_inv(
            bbox.upper_right().y,
            self.lower_right_coordinate.y,
            self.upper_left_coordinate.y,
        );

        overlap_x && overlap_y
    }

    /// Returns the intersection (common area) of the partition with `other` if there is any
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        if self.intersects(other) {
            let ul_x = f64::max(self.upper_left_coordinate.x, other.upper_left_coordinate.x);
            let ul_y = f64::min(self.upper_left_coordinate.y, other.upper_left_coordinate.y);
            let lr_x = f64::min(
                self.lower_right_coordinate.x,
                other.lower_right_coordinate.x,
            );
            let lr_y = f64::max(
                self.lower_right_coordinate.y,
                other.lower_right_coordinate.y,
            );

            Some(Self::new_unchecked(
                (ul_x, ul_y).into(),
                (lr_x, lr_y).into(),
            ))
        } else {
            None
        }
    }

    /// Return true if the partition contains the `other`.
    /// A partition contains another partition if it contains all of its points
    /// OR the other partition is equal to it.
    pub fn contains(&self, other: &Self) -> bool {
        self.contains_x(other) && self.contains_y(other)
    }

    fn contains_x(&self, other: &Self) -> bool {
        crate::util::ranges::value_in_range(
            other.upper_left_coordinate.x,
            self.upper_left_coordinate.x,
            self.lower_right_coordinate.x,
        ) && crate::util::ranges::value_in_range_inv(
            other.lower_right_coordinate.x,
            self.upper_left_coordinate.x,
            self.lower_right_coordinate.x,
        )
    }

    fn contains_y(&self, other: &Self) -> bool {
        crate::util::ranges::value_in_range(
            other.lower_right_coordinate.y,
            self.lower_right_coordinate.y,
            self.upper_left_coordinate.y,
        ) && crate::util::ranges::value_in_range_inv(
            other.upper_left_coordinate.y,
            self.lower_right_coordinate.y,
            self.upper_left_coordinate.y,
        )
    }

    /// Align this partition by snapping bounds to the pixel borders defined by `origin` and `resolution`
    #[must_use]
    pub fn snap_to_grid(&self, origin: Coordinate2D, resolution: SpatialResolution) -> Self {
        Self {
            upper_left_coordinate: (
                snap_prev(origin.x, resolution.x, self.upper_left().x),
                snap_next(origin.y, resolution.y, self.upper_left().y),
            )
                .into(),
            lower_right_coordinate: (
                snap_next(origin.x, resolution.x, self.lower_right().x),
                snap_prev(origin.y, resolution.y, self.lower_right().y),
            )
                .into(),
        }
    }

    /// Return the number of pixel as a gridshape. Due to floating number imprecisions we snap to grid
    /// and round here
    pub fn grid_shape(&self, origin: Coordinate2D, resolution: SpatialResolution) -> GridShape2D {
        let snapped = self.snap_to_grid(origin, resolution);

        [
            (snapped.size_y() / resolution.y).round() as usize,
            (snapped.size_x() / resolution.x).round() as usize,
        ]
        .into()
    }

    #[must_use]
    pub fn extended(self, other: &Self) -> Self {
        let mut extended = self;
        extended.extend(other);
        extended
    }

    pub fn extend(&mut self, other: &Self) {
        self.upper_left_coordinate.x = self.upper_left().x.min(other.upper_left().x);
        self.upper_left_coordinate.y = self.upper_left().y.max(other.upper_left().y);
        self.lower_right_coordinate.x = self.lower_right().x.max(other.lower_right().x);
        self.lower_right_coordinate.y = self.lower_right().y.min(other.lower_right().y);
    }
}

pub trait SpatialPartitioned {
    fn spatial_partition(&self) -> SpatialPartition2D;
}

impl AxisAlignedRectangle for SpatialPartition2D {
    fn from_min_max(min: Coordinate2D, max: Coordinate2D) -> Result<Self> {
        SpatialPartition2D::new((min.x, max.y).into(), (max.x, min.y).into())
    }

    fn upper_left(&self) -> Coordinate2D {
        self.upper_left_coordinate
    }

    fn lower_right(&self) -> Coordinate2D {
        self.lower_right_coordinate
    }

    fn upper_right(&self) -> Coordinate2D {
        Coordinate2D {
            x: self.lower_right_coordinate.x,
            y: self.upper_left_coordinate.y,
        }
    }

    fn lower_left(&self) -> Coordinate2D {
        Coordinate2D {
            x: self.upper_left_coordinate.x,
            y: self.lower_right_coordinate.y,
        }
    }

    fn size_x(&self) -> f64 {
        self.lower_right_coordinate.x - self.upper_left_coordinate.x
    }

    fn size_y(&self) -> f64 {
        self.upper_left_coordinate.y - self.lower_right_coordinate.y
    }

    fn intersection(&self, other: &Self) -> Option<Self> {
        self.intersection(other)
    }

    fn as_bbox(&self) -> BoundingBox2D {
        BoundingBox2D::new_unchecked(self.lower_left(), self.upper_right())
    }
}

impl From<geo::Rect<f64>> for SpatialPartition2D {
    fn from(partition: geo::Rect<f64>) -> SpatialPartition2D {
        SpatialPartition2D::new_unchecked(
            (partition.min().x, partition.max().y).into(),
            (partition.max().x, partition.min().y).into(),
        )
    }
}

impl From<&SpatialPartition2D> for geo::Rect<f64> {
    fn from(partition: &SpatialPartition2D) -> geo::Rect<f64> {
        geo::Rect::new(partition.lower_left(), partition.upper_right())
    }
}

/// Compute the extent of all input partitions. If one partition is None, the output will also be None
pub fn partitions_extent<I: Iterator<Item = Option<SpatialPartition2D>>>(
    mut bboxes: I,
) -> Option<SpatialPartition2D> {
    let Some(Some(mut extent)) = bboxes.next() else {
        return None;
    };

    for bbox in bboxes {
        if let Some(bbox) = bbox {
            extent.extend(&bbox);
        } else {
            return None;
        }
    }

    Some(extent)
}

#[cfg(test)]
mod tests {
    use float_cmp::approx_eq;

    use super::*;

    #[test]
    fn bbox_to_partition() {
        let bbox = BoundingBox2D::new_unchecked((-180., -89.95).into(), (179.95, 90.).into());
        let res = SpatialResolution::new(0.1, 0.1).unwrap();
        assert_eq!(
            SpatialPartition2D::with_bbox_and_resolution(bbox, res),
            SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into())
        );
    }

    #[test]
    fn bbox_to_partition_border_line() {
        let bbox = BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into());
        let res = SpatialResolution::new(0.1, 0.1).unwrap();
        let partition = SpatialPartition2D::with_bbox_and_resolution(bbox, res);

        assert!(approx_eq!(
            Coordinate2D,
            partition.upper_left(),
            (-180., 90.).into()
        ));
        assert!(approx_eq!(
            Coordinate2D,
            partition.lower_right(),
            (180.1, -90.1).into()
        ));
    }

    #[test]
    fn it_contains() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let p2 = SpatialPartition2D::new_unchecked((0., 1.).into(), (0.5, 0.5).into());
        assert!(p1.contains(&p2));
        assert!(!p2.contains(&p1));
    }

    #[test]
    fn it_contains_coord() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());

        let c1 = Coordinate2D { x: 0.1, y: 0.1 };
        let c2 = Coordinate2D { x: 1.1, y: 1.1 };

        assert!(p1.contains_coordinate(&c1));
        assert!(!p1.contains_coordinate(&c2));
    }

    #[test]
    fn it_contains_self() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        assert!(p1.contains(&p1));
    }

    #[test]
    fn it_contains_not() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let p2 = SpatialPartition2D::new_unchecked((1., 1.).into(), (0., 2.).into());
        assert!(!p1.contains(&p2));
    }

    #[test]
    fn it_intersects() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let p2 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.5).into());
        assert!(p1.intersects(&p2));
        assert!(p2.intersects(&p1));

        assert_eq!(
            Some(SpatialPartition2D::new_unchecked(
                (0., 1.).into(),
                (1., 0.5).into()
            )),
            p1.intersection(&p2)
        );

        assert_eq!(
            Some(SpatialPartition2D::new_unchecked(
                (0., 1.).into(),
                (1., 0.5).into()
            )),
            p2.intersection(&p1)
        );
    }

    #[test]
    fn it_intersects2() {
        let p1 = SpatialPartition2D::new_unchecked((0., 5.0).into(), (5.0, 0.).into());
        let p2 = SpatialPartition2D::new_unchecked((0., 20.0).into(), (20.0, 0.).into());
        assert!(p1.intersects(&p2));
        assert!(p2.intersects(&p1));

        assert_eq!(
            Some(SpatialPartition2D::new_unchecked(
                (0., 5.).into(),
                (5., 0.).into()
            )),
            p1.intersection(&p2)
        );

        assert_eq!(
            Some(SpatialPartition2D::new_unchecked(
                (0., 5.).into(),
                (5., 0.).into()
            )),
            p2.intersection(&p1)
        );
    }

    #[test]
    fn it_intersects_not() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let p2 = SpatialPartition2D::new_unchecked((1., 1.).into(), (2., 0.).into());
        assert!(!p1.intersects(&p2));
        assert!(!p2.intersects(&p1));
        assert_eq!(None, p1.intersection(&p2));
        assert_eq!(None, p2.intersection(&p1));
    }

    #[test]
    fn it_intersects_bbox() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let bbox = BoundingBox2D::new_unchecked((0., 0.).into(), (0.5, 0.5).into());
        assert!(p1.intersects_bbox(&bbox));
    }

    #[test]
    fn it_intersects_bbox_not() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let bbox = BoundingBox2D::new_unchecked((1., 1.).into(), (2., 2.).into());
        assert!(!p1.intersects_bbox(&bbox));
    }

    #[test]
    fn it_snaps_to_grid() {
        let origin = Coordinate2D::new(1., 1.);
        let resolution = SpatialResolution { x: 3., y: 5. };
        let p = SpatialPartition2D::new_unchecked((2., 10.).into(), (6., 2.).into());

        assert_eq!(
            p.snap_to_grid(origin, resolution),
            SpatialPartition2D::new_unchecked((1., 11.).into(), (7., 1.).into())
        );
    }

    #[test]
    fn it_counts_pixels() {
        let p = SpatialPartition2D::new_unchecked(
            (137.229_987_293_519_68, -66.227_224_576_271_84).into(),
            (180., -90.).into(),
        );

        assert_eq!(
            p.grid_shape(
                (-180., -66.227_224_576_271_84).into(),
                SpatialResolution::new_unchecked(
                    0.228_716_645_489_199_48,
                    0.226_407_384_987_887_26
                )
            ),
            [105, 187].into()
        );
    }

    #[test]
    fn extent() {
        assert_eq!(partitions_extent([None].into_iter()), None);
        assert_eq!(
            partitions_extent(
                [
                    Some(SpatialPartition2D::new((-50., 50.).into(), (50., -50.).into()).unwrap()),
                    Some(SpatialPartition2D::new((0., 70.).into(), (70., 0.).into()).unwrap())
                ]
                .into_iter()
            ),
            Some(SpatialPartition2D::new((-50., 70.).into(), (70., -50.).into()).unwrap())
        );
        assert_eq!(
            partitions_extent(
                [
                    Some(SpatialPartition2D::new((-50., 50.).into(), (50., -50.).into()).unwrap()),
                    None
                ]
                .into_iter()
            ),
            None
        );
        assert_eq!(
            partitions_extent(
                [
                    None,
                    Some(SpatialPartition2D::new((-50., 50.).into(), (50., -50.).into()).unwrap())
                ]
                .into_iter()
            ),
            None
        );
    }

    #[test]
    fn extend() {
        let mut p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let p2 = SpatialPartition2D::new_unchecked((1., 2.).into(), (2., 0.).into());
        p1.extend(&p2);
        assert_eq!(
            p1,
            SpatialPartition2D::new_unchecked((0., 2.).into(), (2., 0.).into())
        );
    }

    #[test]
    fn extended() {
        let p1 = SpatialPartition2D::new_unchecked((0., 1.).into(), (1., 0.).into());
        let p2 = SpatialPartition2D::new_unchecked((1., 2.).into(), (2., 0.).into());
        assert_eq!(
            p1.extended(&p2),
            SpatialPartition2D::new_unchecked((0., 2.).into(), (2., 0.).into())
        );
    }
}
