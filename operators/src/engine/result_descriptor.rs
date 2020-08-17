use geoengine_datatypes::{
    collections::VectorDataType, projection::ProjectionOption, raster::RasterDataType,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub projection: ProjectionOption,
}

impl RasterResultDescriptor {
    pub fn map<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        f(self)
    }

    pub fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(RasterDataType) -> RasterDataType,
    {
        self.data_type = f(self.data_type);
        self
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub projection: ProjectionOption,
}

impl VectorResultDescriptor {
    pub fn map<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        f(self)
    }

    pub fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(VectorDataType) -> VectorDataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    pub fn map_projection<F>(mut self, f: F) -> Self
    where
        F: Fn(ProjectionOption) -> ProjectionOption,
    {
        self.projection = f(self.projection);
        self
    }
}
