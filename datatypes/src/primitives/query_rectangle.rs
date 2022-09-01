use super::{
    AxisAlignedRectangle, BoundingBox2D, SpatialPartition2D, SpatialPartitioned, SpatialResolution,
    TimeInterval,
};
use serde::{Deserialize, Serialize};

/// A spatio-temporal rectangle with a specified resolution
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<SpatialBounds: AxisAlignedRectangle> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D>;
pub type RasterQueryRectangle = QueryRectangle<SpatialPartition2D>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D>;

impl SpatialPartitioned for QueryRectangle<BoundingBox2D> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        SpatialPartition2D::with_bbox_and_resolution(self.spatial_bounds, self.spatial_resolution)
    }
}

impl SpatialPartitioned for QueryRectangle<SpatialPartition2D> {
    fn spatial_partition(&self) -> SpatialPartition2D {
        self.spatial_bounds
    }
}

impl From<QueryRectangle<BoundingBox2D>> for QueryRectangle<SpatialPartition2D> {
    fn from(value: QueryRectangle<BoundingBox2D>) -> Self {
        Self {
            spatial_bounds: value.spatial_partition(),
            time_interval: value.time_interval,
            spatial_resolution: value.spatial_resolution,
        }
    }
}

/// manual implementation, because derivation can't handle the `SpatialBounds` generic (yet)
impl utoipa::ToSchema for QueryRectangle<SpatialPartition2D> {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property("spatialBounds", Ref::from_schema_name("SpatialPartition2D"))
            .required("spatialBounds")
            .property("timeInterval", Ref::from_schema_name("TimeInterval"))
            .required("timeInterval")
            .property(
                "spatialResolution",
                Ref::from_schema_name("SpatialResolution"),
            )
            .required("spatialResolution")
            .description(Some(
                "A spatio-temporal rectangle with a specified resolution",
            ))
            .into()
    }
}

/// manual implementation, because derivation can't handle the `SpatialBounds` generic (yet)
impl utoipa::ToSchema for QueryRectangle<BoundingBox2D> {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property("spatialBounds", Ref::from_schema_name("BoundingBox2D"))
            .required("spatialBounds")
            .property("timeInterval", Ref::from_schema_name("TimeInterval"))
            .required("timeInterval")
            .property(
                "spatialResolution",
                Ref::from_schema_name("SpatialResolution"),
            )
            .required("spatialResolution")
            .description(Some(
                "A spatio-temporal rectangle with a specified resolution",
            ))
            .into()
    }
}
