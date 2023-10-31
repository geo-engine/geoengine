use super::{
    AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D, SpatialPartitioned, SpatialResolution,
    TimeInterval,
};
use serde::{Deserialize, Serialize};

/// A spatio-temporal rectangle with a specified resolution and the selected bands
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<SpatialBounds: AxisAlignedRectangle, Selection: QuerySelection> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
    #[serde(default)] // TODO: remove once all clients send this?
    pub selection: Selection,
}

pub trait QuerySelection: Default + Copy + Send + Sync {}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BandSelection {
    Single(usize),
    Range(BandRange),
    // TODO: support indiviual bands(?)
}

impl Default for BandSelection {
    fn default() -> Self {
        Self::Single(0) // TODO: default should maybe be all bands?
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

impl QuerySelection for BandSelection {}

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct ColumnSelection {}

impl QuerySelection for ColumnSelection {}
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BandRange {
    pub start: usize, // inclusive
    pub end: usize,   // exclusive
}

pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D, ColumnSelection>;
pub type RasterQueryRectangle = QueryRectangle<SpatialPartition2D, BandSelection>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D, ColumnSelection>;

impl SpatialPartitioned for QueryRectangle<BoundingBox2D, ColumnSelection> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        SpatialPartition2D::with_bbox_and_resolution(self.spatial_bounds, self.spatial_resolution)
    }
}

impl SpatialPartitioned for QueryRectangle<SpatialPartition2D, BandSelection> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_bounds
    }
}

impl From<QueryRectangle<BoundingBox2D, ColumnSelection>>
    for QueryRectangle<SpatialPartition2D, BandSelection>
{
    fn from(value: QueryRectangle<BoundingBox2D, ColumnSelection>) -> Self {
        Self {
            spatial_bounds: value.spatial_partition(),
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
            selection: Default::default(), // TODO: how to do this automatically? maybe we have to remove this From implementation
        }
    }
}
