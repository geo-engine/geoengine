use crate::raster::{GeoTransform, GridBoundingBox2D, GridBounds};

use super::{
    AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D, SpatialPartitioned, SpatialResolution,
    TimeInterval,
};
use serde::{Deserialize, Serialize};

pub trait SpatialResolutionAccess {
    fn spatial_resolution(&self) -> SpatialResolution;
}

pub trait TimeIntervalAccess {
    fn time_interval(&self) -> TimeInterval;
}

pub trait SpatialBoundsAccess {
    type SpatialBounds: AxisAlignedRectangle;
    fn spatial_bounds(&self) -> Self::SpatialBounds;
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoundingBox2DWithResolution {
    pub spatial_bounds: BoundingBox2D,
    pub spatial_resolution: SpatialResolution,
}

/// A spatio-temporal rectangle with a specified resolution
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<SpatialBounds: AxisAlignedRectangle> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D>;
pub type RasterQueryRectangle = QueryRectangle<SpatialPartition2D>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D>;

impl<S> QueryRectangle<S>
where
    S: AxisAlignedRectangle,
{
    pub fn new(
        spatial_bounds: S,
        time_interval: TimeInterval,
        spatial_resolution: SpatialResolution,
    ) -> Self {
        Self {
            spatial_bounds,
            time_interval,
            spatial_resolution,
        }
    }
}

impl SpatialPartitioned for QueryRectangle<BoundingBox2D> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        SpatialPartition2D::with_bbox_and_resolution(self.spatial_bounds, self.spatial_resolution)
    }
}

impl SpatialPartitioned for QueryRectangle<SpatialPartition2D> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_bounds
    }
}

impl From<QueryRectangle<BoundingBox2D>> for QueryRectangle<SpatialPartition2D> {
    fn from(value: QueryRectangle<BoundingBox2D>) -> Self {
        Self {
            spatial_bounds: value.spatial_partition(),
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
        }
    }
}

impl<S> TimeIntervalAccess for QueryRectangle<S>
where
    S: AxisAlignedRectangle,
{
    fn time_interval(&self) -> TimeInterval {
        self.time_interval
    }
}

impl<S> SpatialResolutionAccess for QueryRectangle<S>
where
    S: AxisAlignedRectangle,
{
    fn spatial_resolution(&self) -> SpatialResolution {
        self.spatial_resolution
    }
}

impl<S> SpatialBoundsAccess for QueryRectangle<S>
where
    S: AxisAlignedRectangle,
{
    type SpatialBounds = S;

    fn spatial_bounds(&self) -> Self::SpatialBounds {
        self.spatial_bounds
    }
}

/// A spatio-temporal grid with a geotransform and a size in pixels
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterQueryRectangleImpl {
    pub geo_transform: GeoTransform,
    pub grid_bounds: GridBoundingBox2D,
    pub time_interval: TimeInterval,
}

impl SpatialPartitioned for RasterQueryRectangleImpl {
    fn spatial_partition(&self) -> SpatialPartition2D {
        let ul = self
            .geo_transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(self.grid_bounds.min_index());
        let lr = self
            .geo_transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(self.grid_bounds.max_index() + 1);

        SpatialPartition2D::new_unchecked(ul, lr)
    }
}

impl RasterQueryRectangleImpl {
    pub fn new(
        geo_transform: GeoTransform,
        grid_bounds: GridBoundingBox2D,
        time_interval: TimeInterval,
    ) -> Self {
        Self {
            geo_transform,
            grid_bounds,
            time_interval,
        }
    }
}
