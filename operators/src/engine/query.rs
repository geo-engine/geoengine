use geoengine_datatypes::primitives::{BoundingBox2D, SpatialResolution, TimeInterval};

/// A spatio-temporal rectangle for querying data
#[derive(Copy, Clone, Debug)]
pub struct QueryRectangle {
    pub bbox: BoundingBox2D,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

/// A collection of meta data for processing a query
#[derive(Copy, Clone, Debug)]
pub struct QueryContext {
    // TODO: resolution, profiler, user session, ...
    // TODO: determine chunk size globally or dynamically from workload? Or global Engine Manager instance that gives that info
    pub chunk_byte_size: usize,
}
