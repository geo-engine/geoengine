use super::{ProjAreaOfUseProvider, StaticEpsgAreaProvider};
use crate::spatial_reference::AreaOfUseProvider;
pub enum MixedAreaOfUseProvider {
    Proj(ProjAreaOfUseProvider),
    Static(StaticEpsgAreaProvider),
}

impl AreaOfUseProvider for MixedAreaOfUseProvider {
    fn new_known_crs(def: crate::spatial_reference::SpatialReference) -> super::Result<Self>
    where
        Self: Sized,
    {
        let static_provider_try = StaticEpsgAreaProvider::new_known_crs(def);
        if static_provider_try.is_ok() {
            tracing::trace!("Using StaticAreaofUseProvider for {def}");
            return static_provider_try.map(MixedAreaOfUseProvider::Static);
        }

        tracing::trace!("Using ProjAreaOfUseProvider for {def}");
        ProjAreaOfUseProvider::new_known_crs(def).map(MixedAreaOfUseProvider::Proj)
    }

    fn area_of_use<A: crate::primitives::AxisAlignedRectangle>(&self) -> super::Result<A> {
        match self {
            MixedAreaOfUseProvider::Proj(pro) => pro.area_of_use(),
            MixedAreaOfUseProvider::Static(sta) => sta.area_of_use(),
        }
    }

    fn area_of_use_projected<A: crate::primitives::AxisAlignedRectangle>(
        &self,
    ) -> super::Result<A> {
        match self {
            MixedAreaOfUseProvider::Proj(pro) => pro.area_of_use_projected(),
            MixedAreaOfUseProvider::Static(sta) => sta.area_of_use_projected(),
        }
    }
}
