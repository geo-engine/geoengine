use geoengine_datatypes::primitives::{BoundingBox2D, TimeInterval};

#[derive(Copy, Clone, Debug)]
pub struct QueryRectangle {
    pub bbox: BoundingBox2D,
    pub time_interval: TimeInterval,
}

#[derive(Copy, Clone, Debug)]
pub struct QueryContext {
    // TODO: resolution, profiler, user session, ...
    // TODO: determine chunk size globally or dynamically from workload? Or global Engine Manager instance that gives that info
    pub chunk_byte_size: usize,
}
