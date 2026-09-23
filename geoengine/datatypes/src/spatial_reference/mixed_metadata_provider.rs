use super::{ProjMetadataProvider, StaticEpsgMetadataProvider};
use crate::spatial_reference::CrsMetadataProvider;
pub enum MixedMetadataProvider {
    Proj(ProjMetadataProvider),
    Static(StaticEpsgMetadataProvider),
}

impl CrsMetadataProvider for MixedMetadataProvider {
    fn new_known_crs(def: crate::spatial_reference::SpatialReference) -> super::Result<Self>
    where
        Self: Sized,
    {
        let static_provider_try = StaticEpsgMetadataProvider::new_known_crs(def);
        if static_provider_try.is_ok() {
            tracing::trace!("Using StaticEpsgMetadataProvider for {def}");
            return static_provider_try.map(MixedMetadataProvider::Static);
        }

        tracing::trace!("Using ProjMetadataProvider for {def}");
        ProjMetadataProvider::new_known_crs(def).map(MixedMetadataProvider::Proj)
    }

    fn area_of_use<A: crate::primitives::AxisAlignedRectangle>(&self) -> super::Result<A> {
        match self {
            MixedMetadataProvider::Proj(pro) => pro.area_of_use(),
            MixedMetadataProvider::Static(sta) => sta.area_of_use(),
        }
    }

    fn area_of_use_projected<A: crate::primitives::AxisAlignedRectangle>(
        &self,
    ) -> super::Result<A> {
        match self {
            MixedMetadataProvider::Proj(pro) => pro.area_of_use_projected(),
            MixedMetadataProvider::Static(sta) => sta.area_of_use_projected(),
        }
    }

    fn uses_meters(&self) -> super::Result<bool> {
        match self {
            MixedMetadataProvider::Proj(pro) => pro.uses_meters(),
            MixedMetadataProvider::Static(sta) => sta.uses_meters(),
        }
    }

    fn meters_per_unit(&self) -> super::Result<f64> {
        match self {
            MixedMetadataProvider::Proj(pro) => pro.meters_per_unit(),
            MixedMetadataProvider::Static(sta) => sta.meters_per_unit(),
        }
    }
}
