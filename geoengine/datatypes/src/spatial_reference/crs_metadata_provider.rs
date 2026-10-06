use crate::primitives::AxisAlignedRectangle;
use crate::spatial_reference::SpatialReference;
use crate::util::Result;

/// Static metadata about a coordinate reference system.
///
/// This is the swappable counterpart to the `CoordinateProjection` trait:
/// implementations look up CRS metadata for a given `SpatialReference`,
/// backed either by the static [`crate::spatial_reference::StaticEpsgMetadataProvider`]
/// (compile-time EPSG data from the `crs-constants` crate) or by the CRS
/// database of the underlying projection library.
pub trait CrsMetadataProvider {
    /// Construct a provider for the given well-known CRS.
    /// Fails if no metadata is available for the spatial reference.
    fn new_known_crs(def: SpatialReference) -> Result<Self>
    where
        Self: Sized;
    /// The area in which this CRS is defined to be used, in WGS 84
    /// longitude/latitude degrees.
    fn area_of_use<A: AxisAlignedRectangle>(&self) -> Result<A>;
    /// The area in which this CRS is defined to be used, in the CRS's own
    /// native units (e.g. meters for projected CRSs, degrees for geographic
    /// ones). Empty or absent for CRSs without a valid projected extent (e.g.
    /// EPSG 4326 itself).
    fn area_of_use_projected<A: AxisAlignedRectangle>(&self) -> Result<A>;
    /// Whether the CRS's native unit is meters.
    fn uses_meters(&self) -> Result<bool>;
    /// How many meters correspond to one unit of the CRS's native unit.
    fn meters_per_unit(&self) -> Result<f64>;
}
