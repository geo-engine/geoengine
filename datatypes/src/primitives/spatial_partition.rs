use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::error;
use crate::util::Result;

use super::BoundingBox2D;
use super::Coordinate2D;
use super::{BoxShaped, SpatialResolution};

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
                && upper_left_coordinate.y < lower_right_coordinate.y,
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

    /// Create a partition from a bbox by adding a pixel in each dimension
    /// The resulting partition is not equivalent to the bbox but contains it
    pub fn with_bbox_and_resolution(bbox: BoundingBox2D, resolution: SpatialResolution) -> Self {
        let lr = bbox.lower_right();
        Self {
            upper_left_coordinate: bbox.upper_left(),
            lower_right_coordinate: Coordinate2D {
                x: lr.x + resolution.x, // rather snap to next pixel if pixel is already partially conained
                y: lr.y + resolution.y,
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
    pub fn intersects(&self, other: SpatialPartition) -> bool {
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
    pub fn intersects_bbox(&self, bbox: BoundingBox2D) -> bool {
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

    pub fn intersection(&self, other: SpatialPartition) -> Option<Self> {
        if self.intersects(other) {
            // let ul_x = f64::max(
            //     self.lower_left_coordinate.x,
            //     other_bbox.lower_left_coordinate.x,
            // );
            // let ul_y = f64::max(
            //     self.lower_left_coordinate.y,
            //     other_bbox.lower_left_coordinate.y,
            // );
            // let lr_x = f64::min(
            //     self.upper_right_coordinate.x,
            //     other_bbox.upper_right_coordinate.x,
            // );
            // let lr_y = f64::min(
            //     self.upper_right_coordinate.y,
            //     other_bbox.upper_right_coordinate.y,
            // );

            // Some(BoundingBox2D::new_unchecked(
            //     (ll_x, ll_y).into(),
            //     (ur_x, ur_y).into(),
            // ))
            todo!()
        } else {
            None
        }
    }

    pub fn contains(&self, _other: &Self) -> bool {
        // self.contains_x(other) && self.contains_y(other)
        todo!()
    }

    // fn contains_x(&self, other_bbox: &Self) -> bool {
    //     crate::util::ranges::value_in_rangee(
    //         other_bbox.lower_left().x,
    //         self.lower_left().x,
    //         self.upper_right().x,
    //     ) && crate::util::ranges::value_in_range(
    //         other_bbox.upper_right().x,
    //         self.lower_left().x,
    //         self.upper_right().x,
    //     )
    // }

    // fn contains_y(&self, other_bbox: &Self) -> bool {
    //     crate::util::ranges::value_in_range(
    //         other_bbox.lower_left().y,
    //         self.lower_left().y,
    //         self.upper_right().y,
    //     ) && crate::util::ranges::value_in_range(
    //         other_bbox.upper_right().y,
    //         self.lower_left().y,
    //         self.upper_right().y,
    //     )
    // }
}

pub trait SpatialPartitioned {
    fn spatial_partition(&self) -> SpatialPartition;
}

impl BoxShaped for SpatialPartition {
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
        self.lower_right_coordinate.y - self.upper_left_coordinate.y
    }
}
