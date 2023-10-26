use super::{
    AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D, SpatialPartitioned, SpatialResolution,
    TimeInterval,
};
use serde::{Deserialize, Serialize};

/// A spatio-temporal rectangle with a specified resolution and the selected bands
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<SpatialBounds: AxisAlignedRectangle> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
    // TODO: for vectors this would be attributes? Can we find a common name? Or do we have to split the QueryRectangle up?
    #[serde(default)] // TODO: remove once all clients send this?
    pub bands: BandSelection, // TODO: or do we want to allow individual band selection?
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BandSelection {
    Single(usize),
    Range(BandRange),
}

impl Default for BandSelection {
    fn default() -> Self {
        Self::Single(0)
    }
}

impl BandSelection {
    pub fn new_range(start: usize, end: usize) -> Self {
        Self::Range(BandRange { start, end })
    }

    pub fn bands(&self) -> Vec<usize> {
        match self {
            BandSelection::Single(b) => vec![*b],
            BandSelection::Range(r) => (r.start..r.end).collect(),
        }
    }

    pub fn count(&self) -> usize {
        match self {
            BandSelection::Single(_) => 1,
            BandSelection::Range(r) => r.end - r.start,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BandRange {
    pub start: usize, // inclusive
    pub end: usize,   // exclusive
}

pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D>;
pub type RasterQueryRectangle = QueryRectangle<SpatialPartition2D>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D>;

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
            bands: BandSelection::default(),
        }
    }
}
