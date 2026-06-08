use crate::primitives::Coordinate2D;
use crate::spatial_reference::SpatialReference;
use crate::util::Result;

pub trait CoordinateProjection {
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self>
    where
        Self: Sized;
    /// project a single coord
    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D>;

    /// project a set of coords
    fn project_coordinates<A: AsRef<[Coordinate2D]>>(&self, coords: A)
    -> Result<Vec<Coordinate2D>>;

    fn source_srs(&self) -> SpatialReference;

    fn target_srs(&self) -> SpatialReference;
}
