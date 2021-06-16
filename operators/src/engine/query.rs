use geoengine_datatypes::primitives::{
    BoundingBox2D, SpatialPartition, SpatialPartitioned, SpatialResolution, TimeInterval,
};

/// A spatio-temporal rectangle for querying data
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct VectorQueryRectangle {
    pub bbox: BoundingBox2D,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

/// A spatio-temporal rectangle for querying data
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct RasterQueryRectangle {
    pub partition: SpatialPartition,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

impl SpatialPartitioned for VectorQueryRectangle {
    fn spatial_partition(&self) -> SpatialPartition {
        SpatialPartition::with_bbox_and_resolution(self.bbox, self.spatial_resolution)
    }
}

impl SpatialPartitioned for RasterQueryRectangle {
    fn spatial_partition(&self) -> SpatialPartition {
        self.partition
    }
}

impl From<VectorQueryRectangle> for RasterQueryRectangle {
    fn from(value: VectorQueryRectangle) -> Self {
        Self {
            partition: value.spatial_partition(),
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
        }
    }
}

pub trait QueryContext: Send + Sync {
    fn chunk_byte_size(&self) -> usize;
}

pub struct MockQueryContext {
    pub chunk_byte_size: usize,
}

impl Default for MockQueryContext {
    fn default() -> Self {
        Self {
            chunk_byte_size: 1024 * 1024,
        }
    }
}

impl MockQueryContext {
    pub fn new(chunk_byte_size: usize) -> Self {
        Self { chunk_byte_size }
    }
}

impl QueryContext for MockQueryContext {
    fn chunk_byte_size(&self) -> usize {
        self.chunk_byte_size
    }
}
