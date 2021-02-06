use geoengine_datatypes::primitives::FeatureDataType;
use geoengine_datatypes::{
    collections::VectorDataType, raster::RasterDataType, spatial_reference::SpatialReferenceOption,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A descriptor that contains information about the query result, for instance, the data type
/// and spatial reference.
pub trait ResultDescriptor: Clone {
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub columns: HashMap<String, FeatureDataType>,
}

impl VectorResultDescriptor {
    /// Create a new `VectorResultDescriptor` by only modifying the columns
    pub fn map_columns<F>(&self, f: F) -> Self
    where
        F: Fn(&HashMap<String, FeatureDataType>) -> HashMap<String, FeatureDataType>,
    {
        Self {
            data_type: self.data_type,
            spatial_reference: self.spatial_reference,
            columns: f(&self.columns),
        }
    }
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
            columns: Default::default(),
        };

        let columns = {
            let mut columns = HashMap::with_capacity(1);
            columns.insert("foo".to_string(), FeatureDataType::Number);
            columns
        };

        let descriptor = descriptor
            .map_data_type(|_d| VectorDataType::MultiPoint)
            .map_spatial_reference(|_sref| SpatialReference::epsg_4326().into())
            .map_columns(|_cols| columns.clone());

        assert_eq!(
            descriptor,
            VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns,
            }
        );
    }
}
