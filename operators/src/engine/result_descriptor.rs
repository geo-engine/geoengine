use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BoundingBox2D, Coordinate2D, FeatureDataType, Measurement,
    SpatialPartition2D, TimeInterval,
};
use geoengine_datatypes::raster::{
    GeoTransform, GridBoundingBox2D, GridShape2D, TilingSpecification,
};
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

    /// Map one descriptor to another one by modifying only the time
    #[must_use]
    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>;
}

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub measurement: Measurement,
    pub time: Option<TimeInterval>,
    pub geo_transform: GeoTransform,
    pub pixel_bounds: GridBoundingBox2D,
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

    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>,
    {
        Self {
            time: f(&self.time),
            measurement: self.measurement.clone(),
            ..*self
        }
    }
}

impl RasterResultDescriptor {
    /// Returns the geo transform of the data, i.e. the transformation from pixel coordinates to world coordinates.
    pub fn geo_transform(&self) -> GeoTransform {
        self.geo_transform
    }

    /// Returns the tiling origin of the data, i.e. the upper left corner of the pixel nearest to zero.
    pub fn tiling_origin(&self) -> Coordinate2D {
        self.geo_transform
            .grid_idx_to_pixel_upper_left_coordinate_2d(self.geo_transform.nearest_pixel_to_zero())
    }

    pub fn tiling_pixel_bounds(&self) -> GridBoundingBox2D {
        self.geo_transform
            .shape_to_nearest_to_zero_based(&self.pixel_bounds)
    }

    pub fn tiling_geo_transform(&self) -> GeoTransform {
        self.geo_transform.nearest_pixel_to_zero_based()
    }

    /// Returns the data tiling specification for the given tile size in pixels.
    pub fn generate_data_tiling_spec(
        &self,
        tile_size_in_pixels: GridShape2D,
    ) -> TilingSpecification {
        let tiling_origin = self.tiling_origin();

        TilingSpecification {
            origin_coordinate: tiling_origin,
            tile_size_in_pixels,
        }
    }

    pub fn spatial_tiling_equals(&self, other: &Self) -> bool {
        self.spatial_reference == other.spatial_reference
            && self.tiling_origin() == other.tiling_origin()
            && self.geo_transform.x_pixel_size() == other.geo_transform.x_pixel_size()
            && self.geo_transform.y_pixel_size() == other.geo_transform.y_pixel_size()
    }

    /// Returns `true` if the spatial reference, tiling origin and resolution are the same.
    pub fn spatial_tiling_compat(&self, other: &Self) -> bool {
        self.spatial_tiling_equals(other)
    }

    pub fn spatial_bounds(&self) -> SpatialPartition2D {
        self.geo_transform
            .grid_to_spatial_bounds(&self.pixel_bounds)
    }
}

/// A `ResultDescriptor` for vector queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub columns: HashMap<String, VectorColumnInfo>,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
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
            ..*self
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
            ..*self
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
            ..*self
        }
    }

    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>,
    {
        Self {
            time: f(&self.time),
            columns: self.columns.clone(),
            ..*self
        }
    }
}

/// A `ResultDescriptor` for plot queries
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlotResultDescriptor {
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
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

    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>,
    {
        Self {
            time: f(&self.time),
            ..*self
        }
    }
}

// implementing `From` is possible here because we don't need any additional information, while we would need
// a measurement and a no data value to convert it into a `RasterResultDescriptor`
impl From<VectorResultDescriptor> for PlotResultDescriptor {
    fn from(descriptor: VectorResultDescriptor) -> Self {
        Self {
            spatial_reference: descriptor.spatial_reference,
            time: descriptor.time,
            bbox: descriptor.bbox,
        }
    }
}

// implementing `From` is possible here because we don't need any additional information, while we would need
// to know the `columns` to convert it into a `VectorResultDescriptor`
impl From<RasterResultDescriptor> for PlotResultDescriptor {
    fn from(descriptor: RasterResultDescriptor) -> Self {
        Self {
            spatial_reference: descriptor.spatial_reference,
            time: descriptor.time,
            // converting `SpatialPartition2D` to `BoundingBox2D` is ok here, because is makes the covered area only larger
            bbox: Some(descriptor.spatial_bounds().as_bbox()),
        }
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
    use float_cmp::assert_approx_eq;
    use geoengine_datatypes::{raster::BoundedGrid, spatial_reference::SpatialReference};

    #[test]
    fn map_vector_descriptor() {
        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
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
                time: None,
                bbox: None,
            }
        );
    }

    #[test]
    fn raster_tiling_origin() {
        let descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            measurement: Measurement::Unitless,
            time: None,
            geo_transform: GeoTransform::new(Coordinate2D::new(-10., 10.), -0.3, 0.3),
            pixel_bounds: GridShape2D::new([36, 30]).bounding_box(),
        };

        let to = descriptor.tiling_origin();

        assert_approx_eq!(f64, to.x, -0.09);
        assert_approx_eq!(f64, to.y, 0.09);
    }

    #[test]
    fn raster_tiling_equals() {
        let descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            measurement: Measurement::Unitless,
            time: None,
            geo_transform: GeoTransform::new(Coordinate2D::new(-15., 15.), -0.5, 0.5),
            pixel_bounds: GridShape2D::new([50, 50]).bounding_box(),
        };

        let descriptor2 = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            measurement: Measurement::Unitless,
            time: None,
            geo_transform: GeoTransform::new(Coordinate2D::new(-10., 10.), -0.5, 0.5),
            pixel_bounds: GridShape2D::new([9, 11]).bounding_box(),
        };

        assert!(descriptor.spatial_tiling_equals(&descriptor2));
    }
}
