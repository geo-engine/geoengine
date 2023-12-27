use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, BoundingBox2D, ColumnSelection, FeatureDataType,
    Measurement, PlotSeriesSelection, QueryAttributeSelection, QueryRectangle, SpatialPartition2D,
    SpatialResolution, TimeInterval,
};
use geoengine_datatypes::util::ByteSize;
use geoengine_datatypes::{
    collections::VectorDataType, raster::RasterDataType, spatial_reference::SpatialReferenceOption,
};
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Deserializer, Serialize};
use snafu::ensure;
use std::collections::{HashMap, HashSet};
use std::ops::Index;

use crate::error::{
    Error, RasterBandNameMustNotBeEmpty, RasterBandNameTooLong, RasterBandNamesMustBeUnique,
};
use crate::util::Result;

/// A descriptor that contains information about the query result, for instance, the data type
/// and spatial reference.
pub trait ResultDescriptor: Clone + Serialize {
    type DataType;
    type QueryRectangleSpatialBounds: AxisAlignedRectangle;
    type QueryRectangleAttributeSelection: QueryAttributeSelection;

    // Check the `query` against the `ResultDescriptor` and return `true` if the query is valid
    // and `false` if, e.g., invalid attributes are specified
    fn validate_query(
        &self,
        query: &QueryRectangle<
            Self::QueryRectangleSpatialBounds,
            Self::QueryRectangleAttributeSelection,
        >,
    ) -> Result<()>;

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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSql, FromSql)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub bbox: Option<SpatialPartition2D>,
    pub resolution: Option<SpatialResolution>,
    pub bands: RasterBandDescriptors,
}

impl RasterResultDescriptor {
    pub fn with_datatype_and_num_bands(data_type: RasterDataType, num_bands: u32) -> Self {
        Self {
            data_type,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            time: None,
            bbox: None,
            resolution: None,
            bands: RasterBandDescriptors::new_multiple_bands(num_bands),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RasterBandDescriptors(Vec<RasterBandDescriptor>);

impl RasterBandDescriptors {
    pub fn new(bands: Vec<RasterBandDescriptor>) -> Result<Self> {
        let mut names = HashSet::new();
        for value in &bands {
            ensure!(!value.name.is_empty(), RasterBandNameMustNotBeEmpty);
            ensure!(value.name.byte_size() <= 256, RasterBandNameTooLong);
            ensure!(
                names.insert(&value.name),
                RasterBandNamesMustBeUnique {
                    duplicate_key: value.name.clone()
                }
            );
        }

        Ok(Self(bands))
    }

    /// Convenience method to crate a single band result descriptor with no specific name and a unitless measurement for single band rasters
    pub fn new_single_band() -> Self {
        Self(vec![RasterBandDescriptor {
            name: "band".into(),
            measurement: Measurement::Unitless,
        }])
    }

    /// Convenience method to crate multipe band result descriptors with no specific name and a unitless measurement
    pub fn new_multiple_bands(num_bands: u32) -> Self {
        Self(
            (0..num_bands)
                .map(RasterBandDescriptor::new_unitless_with_idx)
                .collect(),
        )
    }

    pub fn bands(&self) -> &[RasterBandDescriptor] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn count(&self) -> u32 {
        self.0.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &RasterBandDescriptor> {
        self.0.iter()
    }

    pub fn into_vec(self) -> Vec<RasterBandDescriptor> {
        self.0
    }

    // Merge the bands of two descriptors into a new one, fails if there are duplicate names
    pub fn merge(&self, other: &Self) -> Result<Self> {
        let mut bands = self.0.clone();
        bands.extend(other.0.clone());
        Self::new(bands)
    }

    // Merge the bands of two descriptors into a new one, adds a suffix to the band names of the second descriptor if there are duplicate names
    #[must_use]
    pub fn merge_with_suffix(&self, other: &Self, suffix: &str) -> Self {
        let mut bands = self.0.clone();
        let band_names = bands.iter().map(|b| b.name.clone()).collect::<HashSet<_>>();

        for other_band in other.iter() {
            let name = if band_names.contains(&other_band.name) {
                format!("{} {suffix}", other_band.name)
            } else {
                other_band.name.clone()
            };

            bands.push(RasterBandDescriptor::new(
                name,
                other_band.measurement.clone(),
            ));
        }

        Self(bands)
    }

    // Merge the bands of multiple descriptors into a new one, adds a suffix to the band names if there are duplicate names
    pub fn merge_all_with_suffix<'a, B>(mut bands: B, suffix: &str) -> Result<Self>
    where
        B: Iterator<Item = &'a RasterBandDescriptors>,
    {
        let accu = bands
            .next()
            .ok_or(Error::AtLeastOneRasterBandDescriptorRequired)?
            .clone();

        Ok(bands.fold(accu, |acc, other| acc.merge_with_suffix(other, suffix)))
    }
}

impl TryFrom<Vec<RasterBandDescriptor>> for RasterBandDescriptors {
    type Error = Error;

    fn try_from(value: Vec<RasterBandDescriptor>) -> Result<Self, Self::Error> {
        RasterBandDescriptors::new(value)
    }
}

impl Index<usize> for RasterBandDescriptors {
    type Output = RasterBandDescriptor;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl<'de> Deserialize<'de> for RasterBandDescriptors {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::deserialize(deserializer)?;
        RasterBandDescriptors::new(vec).map_err(serde::de::Error::custom)
    }
}

impl<'a> FromSql<'a> for RasterBandDescriptors {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let vec = Vec::<RasterBandDescriptor>::from_sql(ty, raw)?;
        Ok(RasterBandDescriptors(vec))
    }

    fn accepts(ty: &Type) -> bool {
        <Vec<RasterBandDescriptor> as FromSql>::accepts(ty)
    }
}

impl ToSql for RasterBandDescriptors {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        ToSql::to_sql(&self.0, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <Vec<RasterBandDescriptor> as FromSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        ToSql::to_sql_checked(&self.0, ty, w)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSql, FromSql)]
pub struct RasterBandDescriptor {
    pub name: String,
    pub measurement: Measurement,
}

impl RasterBandDescriptor {
    pub fn new(name: String, measurement: Measurement) -> Self {
        Self { name, measurement }
    }

    pub fn new_unitless(name: String) -> Self {
        Self {
            name,
            measurement: Measurement::Unitless,
        }
    }

    pub fn new_unitless_with_idx(idx: u32) -> Self {
        Self {
            name: format!("band {idx}"),
            measurement: Measurement::Unitless,
        }
    }
}

impl ResultDescriptor for RasterResultDescriptor {
    type DataType = RasterDataType;
    type QueryRectangleSpatialBounds = SpatialPartition2D;
    type QueryRectangleAttributeSelection = BandSelection;

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
            bands: self.bands.clone(),
            ..*self
        }
    }

    fn map_spatial_reference<F>(&self, f: F) -> Self
    where
        F: Fn(&SpatialReferenceOption) -> SpatialReferenceOption,
    {
        Self {
            spatial_reference: f(&self.spatial_reference),
            bands: self.bands.clone(),
            ..*self
        }
    }

    fn map_time<F>(&self, f: F) -> Self
    where
        F: Fn(&Option<TimeInterval>) -> Option<TimeInterval>,
    {
        Self {
            time: f(&self.time),
            bands: self.bands.clone(),
            ..*self
        }
    }

    fn validate_query(
        &self,
        query: &QueryRectangle<
            Self::QueryRectangleSpatialBounds,
            Self::QueryRectangleAttributeSelection,
        >,
    ) -> Result<()> {
        for band in query.attributes.as_slice() {
            if *band as usize >= self.bands.len() {
                return Err(Error::BandDoesNotExist { band_idx: *band });
            }
        }

        Ok(())
    }
}

impl RasterResultDescriptor {}

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
    type QueryRectangleSpatialBounds = BoundingBox2D;
    type QueryRectangleAttributeSelection = ColumnSelection;

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

    fn validate_query(
        &self,
        _query: &QueryRectangle<
            Self::QueryRectangleSpatialBounds,
            Self::QueryRectangleAttributeSelection,
        >,
    ) -> Result<()> {
        Ok(())
    }
}

/// A `ResultDescriptor` for plot queries
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, ToSql, FromSql)]
#[serde(rename_all = "camelCase")]
pub struct PlotResultDescriptor {
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

impl ResultDescriptor for PlotResultDescriptor {
    type DataType = (); // TODO: maybe distinguish between image, interactive plot, etc.
    type QueryRectangleSpatialBounds = BoundingBox2D;
    type QueryRectangleAttributeSelection = PlotSeriesSelection;

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

    fn validate_query(
        &self,
        _query: &QueryRectangle<
            Self::QueryRectangleSpatialBounds,
            Self::QueryRectangleAttributeSelection,
        >,
    ) -> Result<()> {
        Ok(())
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
            bbox: descriptor
                .bbox
                .and_then(|p| BoundingBox2D::new(p.lower_left(), p.upper_right()).ok()),
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

mod db_types {
    use super::*;
    use crate::error::Error;
    use geoengine_datatypes::delegate_from_to_sql;
    use postgres_types::{FromSql, ToSql};

    #[derive(Debug, FromSql, ToSql)]
    #[postgres(name = "VectorColumnInfo")]
    pub struct VectorColumnInfoDbType {
        pub column: String,
        pub data_type: FeatureDataType,
        pub measurement: Measurement,
    }

    #[derive(Debug, FromSql, ToSql)]
    #[postgres(name = "VectorResultDescriptor")]
    pub struct VectorResultDescriptorDbType {
        pub data_type: VectorDataType,
        pub spatial_reference: SpatialReferenceOption,
        pub columns: Vec<VectorColumnInfoDbType>,
        pub time: Option<TimeInterval>,
        pub bbox: Option<BoundingBox2D>,
    }

    impl From<&VectorResultDescriptor> for VectorResultDescriptorDbType {
        fn from(result_descriptor: &VectorResultDescriptor) -> Self {
            Self {
                data_type: result_descriptor.data_type,
                spatial_reference: result_descriptor.spatial_reference,
                columns: result_descriptor
                    .columns
                    .iter()
                    .map(|(column, info)| VectorColumnInfoDbType {
                        column: column.clone(),
                        data_type: info.data_type,
                        measurement: info.measurement.clone(),
                    })
                    .collect(),
                time: result_descriptor.time,
                bbox: result_descriptor.bbox,
            }
        }
    }

    impl TryFrom<VectorResultDescriptorDbType> for VectorResultDescriptor {
        type Error = Error;

        fn try_from(result_descriptor: VectorResultDescriptorDbType) -> Result<Self, Self::Error> {
            Ok(Self {
                data_type: result_descriptor.data_type,
                spatial_reference: result_descriptor.spatial_reference,
                columns: result_descriptor
                    .columns
                    .into_iter()
                    .map(|info| {
                        (
                            info.column,
                            VectorColumnInfo {
                                data_type: info.data_type,
                                measurement: info.measurement,
                            },
                        )
                    })
                    .collect(),
                time: result_descriptor.time,
                bbox: result_descriptor.bbox,
            })
        }
    }

    #[derive(Debug, ToSql, FromSql)]
    #[postgres(name = "ResultDescriptor")]
    pub struct TypedResultDescriptorDbType {
        raster: Option<RasterResultDescriptor>,
        vector: Option<VectorResultDescriptor>,
        plot: Option<PlotResultDescriptor>,
    }

    impl From<&TypedResultDescriptor> for TypedResultDescriptorDbType {
        fn from(result_descriptor: &TypedResultDescriptor) -> Self {
            match result_descriptor {
                TypedResultDescriptor::Raster(raster) => Self {
                    raster: Some(raster.clone()),
                    vector: None,
                    plot: None,
                },
                TypedResultDescriptor::Vector(vector) => Self {
                    raster: None,
                    vector: Some(vector.clone()),
                    plot: None,
                },
                TypedResultDescriptor::Plot(plot) => Self {
                    raster: None,
                    vector: None,
                    plot: Some(*plot),
                },
            }
        }
    }

    impl TryFrom<TypedResultDescriptorDbType> for TypedResultDescriptor {
        type Error = Error;

        fn try_from(result_descriptor: TypedResultDescriptorDbType) -> Result<Self, Self::Error> {
            match result_descriptor {
                TypedResultDescriptorDbType {
                    raster: Some(raster),
                    vector: None,
                    plot: None,
                } => Ok(Self::Raster(raster)),
                TypedResultDescriptorDbType {
                    raster: None,
                    vector: Some(vector),
                    plot: None,
                } => Ok(Self::Vector(vector)),
                TypedResultDescriptorDbType {
                    raster: None,
                    vector: None,
                    plot: Some(plot),
                } => Ok(Self::Plot(plot)),
                _ => {
                    Err(geoengine_datatypes::error::Error::UnexpectedInvalidDbTypeConversion.into())
                }
            }
        }
    }

    delegate_from_to_sql!(VectorResultDescriptor, VectorResultDescriptorDbType);
    delegate_from_to_sql!(TypedResultDescriptor, TypedResultDescriptorDbType);
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use serde_json::json;

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
    fn it_checks_duplicate_bands() {
        assert!(RasterBandDescriptors::new(vec![
            RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
            RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
        ])
        .is_ok());

        assert!(RasterBandDescriptors::new(vec![
            RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
            RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
            RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
        ])
        .is_err());
    }

    #[test]
    fn it_merges_band_descriptors() {
        assert_eq!(
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
            ])
            .unwrap()
            .merge(
                &RasterBandDescriptors::new(vec![
                    RasterBandDescriptor::new("baz".into(), Measurement::Unitless),
                    RasterBandDescriptor::new("bla".into(), Measurement::Unitless),
                ])
                .unwrap()
            )
            .unwrap(),
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
                RasterBandDescriptor::new("baz".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bla".into(), Measurement::Unitless),
            ])
            .unwrap()
        );
    }

    #[test]
    fn it_merges_band_descriptors_and_suffixes_duplicates() {
        assert_eq!(
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
            ])
            .unwrap()
            .merge_with_suffix(
                &RasterBandDescriptors::new(vec![
                    RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                    RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
                ])
                .unwrap(),
                "(dup)"
            ),
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
                RasterBandDescriptor::new("foo (dup)".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar (dup)".into(), Measurement::Unitless),
            ])
            .unwrap()
        );
    }

    #[test]
    fn it_merges_all_band_descriptors_and_suffixes_duplicates() {
        assert_eq!(
            RasterBandDescriptors::merge_all_with_suffix(
                [
                    RasterBandDescriptors::new(vec![
                        RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                        RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
                    ])
                    .unwrap(),
                    RasterBandDescriptors::new(vec![
                        RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                        RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
                    ])
                    .unwrap(),
                    RasterBandDescriptors::new(vec![
                        RasterBandDescriptor::new("bla".into(), Measurement::Unitless),
                        RasterBandDescriptor::new("blub".into(), Measurement::Unitless),
                    ])
                    .unwrap()
                ]
                .iter(),
                "(dup)"
            )
            .unwrap(),
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
                RasterBandDescriptor::new("foo (dup)".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar (dup)".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bla".into(), Measurement::Unitless),
                RasterBandDescriptor::new("blub".into(), Measurement::Unitless),
            ])
            .unwrap()
        );
    }

    #[test]
    fn it_checks_duplicates_while_deserializing_band_descriptors() {
        assert_eq!(
            serde_json::from_value::<RasterBandDescriptors>(json!([{
                "name": "foo",
                "measurement": {
                    "type": "unitless"
                }
            },{
                "name": "bar",
                "measurement": {
                    "type": "unitless"
                }
            }]))
            .unwrap(),
            RasterBandDescriptors::new(vec![
                RasterBandDescriptor::new("foo".into(), Measurement::Unitless),
                RasterBandDescriptor::new("bar".into(), Measurement::Unitless),
            ])
            .unwrap()
        );

        assert!(serde_json::from_value::<RasterBandDescriptors>(json!([{
            "name": "foo",
            "measurement": {
                "type": "unitless"
            }
        },{
            "name": "foo",
            "measurement": {
                "type": "unitless"
            }
        }]))
        .is_err());
    }
}
