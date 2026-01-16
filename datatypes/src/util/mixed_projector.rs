use crate::{
    operations::reproject::CoordinateProjection,
    spatial_reference::AreaOfUseProvider,
    util::{crs_definitions::StaticAreaofUseProvider, proj_projector::ProjAreaOfUseProvider},
};

pub enum MixedCoordinateProjector {
    ProjProjector(super::proj_projector::ProjCoordinateProjector),
    GeodesyProjector(super::geodesy_projector::Transformer),
}

impl MixedCoordinateProjector {
    pub fn from_known_srs(
        source: crate::spatial_reference::SpatialReference,
        target: crate::spatial_reference::SpatialReference,
    ) -> crate::util::Result<Self> {
        let try_geodesy = super::geodesy_projector::Transformer::from_known_srs(source, target);

        if try_geodesy.is_ok() {
            tracing::debug!(
                "Using geodesy for reprojection from {} to {}",
                source,
                target
            );
            try_geodesy.map(Self::GeodesyProjector)
        } else {
            tracing::debug!("Using proj for reprojection from {} to {}", source, target);
            let proj_projector =
                super::proj_projector::ProjCoordinateProjector::from_known_srs(source, target)?;
            Ok(Self::ProjProjector(proj_projector))
        }
    }
}

impl CoordinateProjection for MixedCoordinateProjector {
    fn from_known_srs(
        from: crate::spatial_reference::SpatialReference,
        to: crate::spatial_reference::SpatialReference,
    ) -> super::Result<Self>
    where
        Self: Sized,
    {
        MixedCoordinateProjector::from_known_srs(from, to)
    }

    fn project_coordinate(
        &self,
        c: crate::primitives::Coordinate2D,
    ) -> super::Result<crate::primitives::Coordinate2D> {
        match self {
            MixedCoordinateProjector::ProjProjector(proj) => proj.project_coordinate(c),
            MixedCoordinateProjector::GeodesyProjector(geodesy) => geodesy.project_coordinate(c),
        }
    }

    fn project_coordinates<A: AsRef<[crate::primitives::Coordinate2D]>>(
        &self,
        coords: A,
    ) -> super::Result<Vec<crate::primitives::Coordinate2D>> {
        match self {
            MixedCoordinateProjector::ProjProjector(proj) => proj.project_coordinates(coords),
            MixedCoordinateProjector::GeodesyProjector(geodesy) => {
                geodesy.project_coordinates(coords)
            }
        }
    }

    fn source_srs(&self) -> crate::spatial_reference::SpatialReference {
        match self {
            MixedCoordinateProjector::ProjProjector(proj) => proj.source_srs(),
            MixedCoordinateProjector::GeodesyProjector(geodesy) => geodesy.source_srs(),
        }
    }

    fn target_srs(&self) -> crate::spatial_reference::SpatialReference {
        match self {
            MixedCoordinateProjector::ProjProjector(proj) => proj.target_srs(),
            MixedCoordinateProjector::GeodesyProjector(geodesy) => geodesy.target_srs(),
        }
    }
}

pub enum MixedAreaOfUseProvider {
    Proj(ProjAreaOfUseProvider),
    Static(StaticAreaofUseProvider),
}

impl AreaOfUseProvider for MixedAreaOfUseProvider {
    fn new_known_crs(def: crate::spatial_reference::SpatialReference) -> super::Result<Self>
    where
        Self: Sized,
    {
        let static_provider_try = StaticAreaofUseProvider::new_known_crs(def);
        if static_provider_try.is_ok() {
            tracing::trace!("Using StaticAreaofUseProvider for {def}");
            return static_provider_try.map(MixedAreaOfUseProvider::Static);
        };

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
