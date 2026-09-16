use crate::primitives::AxisAlignedRectangle;
use crate::spatial_reference::SpatialReference;
use crate::util::Result;

pub trait CrsMetadataProvider {
    fn new_known_crs(def: SpatialReference) -> Result<Self>
    where
        Self: Sized;
    fn area_of_use<A: AxisAlignedRectangle>(&self) -> Result<A>;
    fn area_of_use_projected<A: AxisAlignedRectangle>(&self) -> Result<A>;
    fn uses_meters(&self) -> Result<bool>;
    fn meters_per_unit(&self) -> Result<f64>;
}
