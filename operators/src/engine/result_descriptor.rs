use geoengine_datatypes::{
    collections::VectorDataType, projection::ProjectionOption, raster::RasterDataType,
};
use serde::{Deserialize, Serialize};

/// A descriptor that contains information about the query result, for instance, the data type
/// and projection.
pub trait ResultDescriptor: Copy {
    type DataType;

    fn data_type(&self) -> Self::DataType;

    fn projection(&self) -> ProjectionOption;

    fn map<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        f(self)
    }

    fn map_projection<F>(self, f: F) -> Self
    where
        F: Fn(Self::DataType) -> Self::DataType;

    fn map_data_type<F>(self, f: F) -> Self
    where
        F: Fn(ProjectionOption) -> ProjectionOption;
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub projection: ProjectionOption,
}

impl ResultDescriptor for RasterResultDescriptor {
    type DataType = RasterDataType;

    fn data_type(&self) -> Self::DataType {
        self.data_type
    }

    fn projection(&self) -> ProjectionOption {
        self.projection
    }

    fn map_projection<F>(mut self, f: F) -> Self
    where
        F: Fn(Self::DataType) -> Self::DataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(ProjectionOption) -> ProjectionOption,
    {
        self.projection = f(self.projection);
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
}

impl ResultDescriptor for VectorResultDescriptor {
    type DataType = VectorDataType;

    fn data_type(&self) -> Self::DataType {
        self.data_type
    }

    fn projection(&self) -> ProjectionOption {
        self.projection
    }

    fn map_projection<F>(mut self, f: F) -> Self
    where
        F: Fn(Self::DataType) -> Self::DataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(ProjectionOption) -> ProjectionOption,
    {
        self.projection = f(self.projection);
        self
    }
}
