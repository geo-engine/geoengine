use crate::primitives::Coordinate2D;
use crate::spatial_reference::SpatialReference;
use crate::util::Result;

/// A projection between two spatial reference systems.
///
/// This is the abstraction that makes the coordinate projector swappable:
/// concrete implementations exist for
/// - [`super::ProjCoordinateProjector`] (the PROJ library via the `proj` crate),
/// - [`crate::spatial_reference::GeodesyCoordinateProjector`] (the `geodesy` crate),
/// - the `MixedCoordinateProjector`, which combines both
///   (this is `DefaultCoordinateProjector`).
///
/// Implementations are created for a concrete pair of spatial references via
/// [`from_known_srs`](CoordinateProjection::from_known_srs) and can then project
/// coordinate(s) from their source into their target CRS.
pub trait CoordinateProjection {
    /// Construct a projector for the transformation from `from` to `to`.
    /// Fails if no projection is available for the pair of spatial references.
    fn from_known_srs(from: SpatialReference, to: SpatialReference) -> Result<Self>
    where
        Self: Sized;
    /// project a single coord
    fn project_coordinate(&self, c: Coordinate2D) -> Result<Coordinate2D>;

    /// project a set of coords
    fn project_coordinates<A: AsRef<[Coordinate2D]>>(&self, coords: A)
    -> Result<Vec<Coordinate2D>>;

    /// The spatial reference the projector projects from.
    fn source_srs(&self) -> SpatialReference;

    /// The spatial reference the projector projects into.
    fn target_srs(&self) -> SpatialReference;
}
