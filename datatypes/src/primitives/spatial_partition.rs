use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error;
use crate::util::Result;

use super::BoundingBox2D;
use super::Coordinate2D;
use super::{AxisAlignedRectangle, SpatialResolution};

/// A partition of space that include the upper left but excludes the lower right coordinate
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpatialPartition {
    upper_left_coordinate: Coordinate2D,
    lower_right_coordinate: Coordinate2D,
}

impl SpatialPartition {
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
        let lr = bbox.lower_right();
        Self {
            upper_left_coordinate: bbox.upper_left(),
            lower_right_coordinate: Coordinate2D {
                x: (lr.x / resolution.x).ceil() * resolution.x,
                y: (lr.y / resolution.y).ceil() * resolution.y,
            },
        }
    }

    /// Checks if a coordinate is located inside spatial partition
    pub fn contains_coordinate(&self, coordinate: &Coordinate2D) -> bool {
        coordinate.x >= self.upper_left_coordinate.x
            && coordinate.y >= self.upper_left_coordinate.y
            && coordinate.x < self.lower_right_coordinate.x
            && coordinate.y < self.lower_right_coordinate.y
    }

    /// Return true if the `other` partition has any space in common with the partition
    pub fn intersects(&self, other: &SpatialPartition) -> bool {
        let overlap_x = crate::util::ranges::value_in_range(
            self.upper_left_coordinate.x,
            other.upper_left_coordinate.x,
            other.lower_right_coordinate.x,
        ) || crate::util::ranges::value_in_range(
            other.upper_left_coordinate.x,
            self.upper_left_coordinate.x,
            self.lower_right_coordinate.x,
        );

        let overlap_y = crate::util::ranges::value_in_range(
            self.lower_right_coordinate.y,
            other.lower_right_coordinate.y,
            other.upper_left_coordinate.y,
        ) || crate::util::ranges::value_in_range(
            other.lower_right_coordinate.y,
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
            self.lower_right_coordinate.y,
            bbox.lower_left().y,
            bbox.upper_right().y,
        ) || crate::util::ranges::value_in_range(
            bbox.lower_left().y,
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

    /// Return true if the partition contains the `other`
    pub fn contains(&self, other: &Self) -> bool {
        self.contains_x(other) && self.contains_y(other)
    }

    fn contains_x(&self, other: &Self) -> bool {
        crate::util::ranges::value_in_range(
            other.upper_left_coordinate.x,
            self.upper_left_coordinate.x,
            self.lower_right_coordinate.x,
        ) && crate::util::ranges::value_in_range(
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
        ) && crate::util::ranges::value_in_range(
            other.upper_left_coordinate.y,
            self.lower_right_coordinate.y,
            self.upper_left_coordinate.y,
        )
    }
}

pub trait SpatialPartitioned {
    fn spatial_partition(&self) -> SpatialPartition;
}

impl AxisAlignedRectangle for SpatialPartition {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbox_to_partition() {
        let bbox = BoundingBox2D::new_unchecked((-180., -90.).into(), (180., 90.).into());
        let res = SpatialResolution { x: 0.1, y: -0.1 };
        assert_eq!(
            SpatialPartition::with_bbox_and_resolution(bbox, res),
            SpatialPartition::new_unchecked((-180., 90.).into(), (180., -90.).into())
        );
    }
}
