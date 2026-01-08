use crate::operations::reproject::CoordinateProjection;

pub enum MixedCoordinateProjector {
    ProProjector(super::proj_projector::ProjCoordinateProjector),
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
            Ok(Self::ProProjector(proj_projector))
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
            MixedCoordinateProjector::ProProjector(proj) => proj.project_coordinate(c),
            MixedCoordinateProjector::GeodesyProjector(geodesy) => geodesy.project_coordinate(c),
        }
    }

    fn project_coordinates<A: AsRef<[crate::primitives::Coordinate2D]>>(
        &self,
        coords: A,
    ) -> super::Result<Vec<crate::primitives::Coordinate2D>> {
        match self {
            MixedCoordinateProjector::ProProjector(proj) => proj.project_coordinates(coords),
            MixedCoordinateProjector::GeodesyProjector(geodesy) => {
                geodesy.project_coordinates(coords)
            }
        }
    }

    fn source_srs(&self) -> crate::spatial_reference::SpatialReference {
        match self {
            MixedCoordinateProjector::ProProjector(proj) => proj.source_srs(),
            MixedCoordinateProjector::GeodesyProjector(geodesy) => geodesy.source_srs(),
        }
    }

    fn target_srs(&self) -> crate::spatial_reference::SpatialReference {
        match self {
            MixedCoordinateProjector::ProProjector(proj) => proj.target_srs(),
            MixedCoordinateProjector::GeodesyProjector(geodesy) => geodesy.target_srs(),
        }
    }
}
