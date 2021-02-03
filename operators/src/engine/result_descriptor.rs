use geoengine_datatypes::{
    collections::VectorDataType, raster::RasterDataType, spatial_reference::SpatialReferenceOption,
};
use serde::{Deserialize, Serialize};

/// A descriptor that contains information about the query result, for instance, the data type
/// and spatial reference.
pub trait ResultDescriptor: Copy {
    type DataType;

    /// Return the type-specific result data type
    fn data_type(&self) -> Self::DataType;

    /// Return the spatial reference of the result
    fn spatial_reference(&self) -> SpatialReferenceOption;

    /// Map one descriptor to another one
    fn map<F>(self, f: F) -> Self
    where
        F: Fn(Self) -> Self,
    {
        f(self)
    }

    /// Map one descriptor to another one by modifying only the spatial reference
    fn map_data_type<F>(self, f: F) -> Self
    where
        F: Fn(Self::DataType) -> Self::DataType;

    /// Map one descriptor to another one by modifying only the data type
    fn map_spatial_reference<F>(self, f: F) -> Self
    where
        F: Fn(SpatialReferenceOption) -> SpatialReferenceOption;
}

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
}

impl ResultDescriptor for RasterResultDescriptor {
    type DataType = RasterDataType;

    fn data_type(&self) -> Self::DataType {
        self.data_type
    }

    fn spatial_reference(&self) -> SpatialReferenceOption {
        self.spatial_reference
    }

    fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(Self::DataType) -> Self::DataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    fn map_spatial_reference<F>(mut self, f: F) -> Self
    where
        F: Fn(SpatialReferenceOption) -> SpatialReferenceOption,
    {
        self.spatial_reference = f(self.spatial_reference);
        self
    }
}

/// A `ResultDescriptor` for vector queries
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub spatial_reference: SpatialReferenceOption,
}

impl ResultDescriptor for VectorResultDescriptor {
    type DataType = VectorDataType;

    fn data_type(&self) -> Self::DataType {
        self.data_type
    }

    fn spatial_reference(&self) -> SpatialReferenceOption {
        self.spatial_reference
    }

    fn map_data_type<F>(mut self, f: F) -> Self
    where
        F: Fn(Self::DataType) -> Self::DataType,
    {
        self.data_type = f(self.data_type);
        self
    }

    fn map_spatial_reference<F>(mut self, f: F) -> Self
    where
        F: Fn(SpatialReferenceOption) -> SpatialReferenceOption,
    {
        self.spatial_reference = f(self.spatial_reference);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::SpatialReference;

    #[test]
    fn map_vector_descriptor() {
        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
        };

        let descriptor = descriptor.map_data_type(|_d| VectorDataType::MultiPoint);
        let descriptor =
            descriptor.map_spatial_reference(|_sref| SpatialReference::epsg_4326().into());

        assert_eq!(
            descriptor,
            VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
            }
        );
    }
}
