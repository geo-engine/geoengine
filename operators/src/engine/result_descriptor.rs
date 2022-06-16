use geoengine_datatypes::primitives::{FeatureDataType, Measurement};
use geoengine_datatypes::raster::FromPrimitive;
use geoengine_datatypes::{
    collections::VectorDataType, raster::RasterDataType, spatial_reference::SpatialReferenceOption,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A descriptor that contains information about the query result, for instance, the data type
/// and spatial reference.
pub trait ResultDescriptor: Clone + Serialize {
    type DataType;

    /// Return the type-specific result data type
    fn data_type(&self) -> Self::DataType;

    /// Return the spatial reference of the result
    fn spatial_reference(&self) -> SpatialReferenceOption;

    /// Map one descriptor to another one
    #[must_use]
    fn map<F>(&self, f: F) -> Self
    where
        F: Fn(&Self) -> Self,
    {
        f(self)
    }

    /// Map one descriptor to another one by modifying only the spatial reference
    #[must_use]
    fn map_data_type<F>(&self, f: F) -> Self
    where
        F: Fn(&Self::DataType) -> Self::DataType;

    /// Map one descriptor to another one by modifying only the data type
    #[must_use]
    fn map_spatial_reference<F>(&self, f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption;
}

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub measurement: Measurement,
    pub no_data_value: Option<f64>,
}

impl ResultDescriptor for RasterResultDescriptor {
    type DataType = RasterDataType;

    fn data_type(&self) -> Self::DataType {
        self.data_type
    }

    fn spatial_reference(&self) -> SpatialReferenceOption {
        self.spatial_reference
    }

    fn map_data_type<F>(&self, f: F) -> Self
    where
        F: Fn(&Self::DataType) -> Self::DataType,
    {
        Self {
            data_type: f(&self.data_type),
            measurement: self.measurement.clone(),
            ..*self
        }
    }

    fn map_spatial_reference<F>(&self, f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption,
    {
        Self {
            spatial_reference: f(&self.spatial_reference),
            measurement: self.measurement.clone(),
            ..*self
        }
    }
}

impl RasterResultDescriptor {
    pub fn no_data_value_as_<T: FromPrimitive<f64>>(&self) -> Option<T> {
        self.no_data_value.map(|v| T::from_(v))
    }
}

/// A `ResultDescriptor` for vector queries
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub columns: HashMap<String, VectorColumnInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorColumnInfo {
    pub data_type: FeatureDataType,
    pub measurement: Measurement,
}

impl VectorResultDescriptor {
    /// Create a new `VectorResultDescriptor` by only modifying the columns
    #[must_use]
    pub fn map_columns<F>(&self, f: F) -> Self
    where
        F: Fn(&HashMap<String, VectorColumnInfo>) -> HashMap<String, VectorColumnInfo>,
    {
        Self {
            data_type: self.data_type,
            spatial_reference: self.spatial_reference,
            columns: f(&self.columns),
        }
    }

    pub fn column_data_type(&self, column: &str) -> Option<FeatureDataType> {
        self.columns.get(column).map(|c| c.data_type)
    }

    pub fn column_measurement(&self, column: &str) -> Option<&Measurement> {
        self.columns.get(column).map(|c| &c.measurement)
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

    fn map_data_type<F>(&self, f: F) -> Self
    where
        F: Fn(&Self::DataType) -> Self::DataType,
    {
        Self {
            data_type: f(&self.data_type),
            spatial_reference: self.spatial_reference,
            columns: self.columns.clone(),
        }
    }

    fn map_spatial_reference<F>(&self, f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption,
    {
        Self {
            data_type: self.data_type,
            spatial_reference: f(&self.spatial_reference),
            columns: self.columns.clone(),
        }
    }
}

/// A `ResultDescriptor` for plot queries
#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlotResultDescriptor {
    pub spatial_reference: SpatialReferenceOption,
}

impl ResultDescriptor for PlotResultDescriptor {
    type DataType = (); // TODO: maybe distinguish between image, interactive plot, etc.

    fn data_type(&self) -> Self::DataType {}

    fn spatial_reference(&self) -> SpatialReferenceOption {
        self.spatial_reference
    }

    fn map_data_type<F>(&self, _f: F) -> Self
    where
        F: Fn(&Self::DataType) -> Self::DataType,
    {
        *self
    }

    fn map_spatial_reference<F>(&self, _f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption,
    {
        *self
    }
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum TypedResultDescriptor {
    Plot(PlotResultDescriptor),
    Raster(RasterResultDescriptor),
    Vector(VectorResultDescriptor),
}

impl From<PlotResultDescriptor> for TypedResultDescriptor {
    fn from(value: PlotResultDescriptor) -> Self {
        Self::Plot(value)
    }
}

impl From<RasterResultDescriptor> for TypedResultDescriptor {
    fn from(value: RasterResultDescriptor) -> Self {
        Self::Raster(value)
    }
}

impl From<VectorResultDescriptor> for TypedResultDescriptor {
    fn from(value: VectorResultDescriptor) -> Self {
        Self::Vector(value)
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
            columns.insert(
                "foo".to_string(),
                VectorColumnInfo {
                    data_type: FeatureDataType::Float,
                    measurement: Measurement::continuous("bar".into(), None),
                },
            );
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
