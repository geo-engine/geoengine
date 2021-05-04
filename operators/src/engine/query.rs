use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};

/// A spatio-temporal rectangle for querying data
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct QueryRectangle {
    pub bbox: BoundingBox2D,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
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
