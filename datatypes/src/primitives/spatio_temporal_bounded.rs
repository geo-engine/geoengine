use crate::primitives::{BoundingBox2D, TimeInterval};

pub trait SpatialBounded {
    fn spatial_bounds(&self) -> BoundingBox2D;
}

pub trait TemporalBounded {
    fn temporal_bounds(&self) -> TimeInterval;
}
