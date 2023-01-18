use crate::error::{self, Result};
use crate::identifier;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, MultiLineStringAccess, MultiPointAccess, MultiPolygonAccess,
};
use ordered_float::NotNan;
use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
use snafu::ResultExt;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Formatter},
    str::FromStr,
};
use utoipa::ToSchema;

identifier!(DataProviderId);

impl From<DataProviderId> for geoengine_datatypes::dataset::DataProviderId {
    fn from(value: DataProviderId) -> Self {
        Self(value.0)
    }
}

// Identifier for datasets managed by Geo Engine
identifier!(DatasetId);

impl From<DatasetId> for geoengine_datatypes::dataset::DatasetId {
    fn from(value: DatasetId) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
/// The identifier for loadable data. It is used in the source operators to get the loading info (aka parametrization)
/// for accessing the data. Internal data is loaded from datasets, external from `DataProvider`s.
pub enum DataId {
    #[serde(rename_all = "camelCase")]
    Internal {
        dataset_id: DatasetId,
    },
    External(ExternalDataId),
}

impl DataId {
    pub fn internal(&self) -> Option<DatasetId> {
        if let Self::Internal {
            dataset_id: dataset,
        } = self
        {
            return Some(*dataset);
        }
        None
    }

    pub fn external(&self) -> Option<ExternalDataId> {
        if let Self::External(id) = self {
            return Some(id.clone());
        }
        None
    }
}

impl From<DatasetId> for DataId {
    fn from(value: DatasetId) -> Self {
        Self::Internal { dataset_id: value }
    }
}

impl From<ExternalDataId> for DataId {
    fn from(value: ExternalDataId) -> Self {
        Self::External(value)
    }
}

impl From<ExternalDataId> for geoengine_datatypes::dataset::DataId {
    fn from(value: ExternalDataId) -> Self {
        Self::External(value.into())
    }
}

impl From<DatasetId> for geoengine_datatypes::dataset::DataId {
    fn from(value: DatasetId) -> Self {
        Self::Internal {
            dataset_id: value.into(),
        }
    }
}

impl From<ExternalDataId> for geoengine_datatypes::dataset::ExternalDataId {
    fn from(value: ExternalDataId) -> Self {
        Self {
            provider_id: value.provider_id.into(),
            layer_id: value.layer_id.into(),
        }
    }
}

impl From<geoengine_datatypes::dataset::DataId> for DataId {
    fn from(id: geoengine_datatypes::dataset::DataId) -> Self {
        match id {
            geoengine_datatypes::dataset::DataId::Internal { dataset_id } => Self::Internal {
                dataset_id: dataset_id.into(),
            },
            geoengine_datatypes::dataset::DataId::External(external_id) => {
                Self::External(external_id.into())
            }
        }
    }
}

impl From<DataId> for geoengine_datatypes::dataset::DataId {
    fn from(id: DataId) -> Self {
        match id {
            DataId::Internal { dataset_id } => Self::Internal {
                dataset_id: dataset_id.into(),
            },
            DataId::External(external_id) => Self::External(external_id.into()),
        }
    }
}

impl From<geoengine_datatypes::dataset::DatasetId> for DatasetId {
    fn from(id: geoengine_datatypes::dataset::DatasetId) -> Self {
        Self(id.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema)]
pub struct LayerId(pub String);

impl From<LayerId> for geoengine_datatypes::dataset::LayerId {
    fn from(value: LayerId) -> Self {
        Self(value.0)
    }
}

impl std::fmt::Display for LayerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExternalDataId {
    pub provider_id: DataProviderId,
    pub layer_id: LayerId,
}

impl From<geoengine_datatypes::dataset::ExternalDataId> for ExternalDataId {
    fn from(id: geoengine_datatypes::dataset::ExternalDataId) -> Self {
        Self {
            provider_id: id.provider_id.into(),
            layer_id: id.layer_id.into(),
        }
    }
}

impl From<geoengine_datatypes::dataset::DataProviderId> for DataProviderId {
    fn from(id: geoengine_datatypes::dataset::DataProviderId) -> Self {
        Self(id.0)
    }
}

impl From<geoengine_datatypes::dataset::LayerId> for LayerId {
    fn from(id: geoengine_datatypes::dataset::LayerId) -> Self {
        Self(id.0)
    }
}

/// A spatial reference authority that is part of a spatial reference definition
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SpatialReferenceAuthority {
    Epsg,
    SrOrg,
    Iau2000,
    Esri,
}

impl From<geoengine_datatypes::spatial_reference::SpatialReferenceAuthority>
    for SpatialReferenceAuthority
{
    fn from(value: geoengine_datatypes::spatial_reference::SpatialReferenceAuthority) -> Self {
        match value {
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Epsg => Self::Epsg,
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::SrOrg => Self::SrOrg,
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Iau2000 => {
                Self::Iau2000
            }
            geoengine_datatypes::spatial_reference::SpatialReferenceAuthority::Esri => Self::Esri,
        }
    }
}

impl From<SpatialReferenceAuthority>
    for geoengine_datatypes::spatial_reference::SpatialReferenceAuthority
{
    fn from(value: SpatialReferenceAuthority) -> Self {
        match value {
            SpatialReferenceAuthority::Epsg => Self::Epsg,
            SpatialReferenceAuthority::SrOrg => Self::SrOrg,
            SpatialReferenceAuthority::Iau2000 => Self::Iau2000,
            SpatialReferenceAuthority::Esri => Self::Esri,
        }
    }
}

impl FromStr for SpatialReferenceAuthority {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "EPSG" => SpatialReferenceAuthority::Epsg,
            "SR-ORG" => SpatialReferenceAuthority::SrOrg,
            "IAU2000" => SpatialReferenceAuthority::Iau2000,
            "ESRI" => SpatialReferenceAuthority::Esri,
            _ => {
                return Err(error::Error::InvalidSpatialReferenceString {
                    spatial_reference_string: s.into(),
                })
            }
        })
    }
}

impl std::fmt::Display for SpatialReferenceAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SpatialReferenceAuthority::Epsg => "EPSG",
                SpatialReferenceAuthority::SrOrg => "SR-ORG",
                SpatialReferenceAuthority::Iau2000 => "IAU2000",
                SpatialReferenceAuthority::Esri => "ESRI",
            }
        )
    }
}

/// A spatial reference consists of an authority and a code
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct SpatialReference {
    authority: SpatialReferenceAuthority,
    code: u32,
}

impl SpatialReference {
    pub fn proj_string(self) -> Result<String> {
        match self.authority {
            SpatialReferenceAuthority::Epsg | SpatialReferenceAuthority::Iau2000 => {
                Ok(format!("{}:{}", self.authority, self.code))
            }
            // poor-mans integration of Meteosat Second Generation
            SpatialReferenceAuthority::SrOrg if self.code == 81 => Ok("+proj=geos +lon_0=0 +h=35785831 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs +type=crs".to_owned()),
            SpatialReferenceAuthority::SrOrg | SpatialReferenceAuthority::Esri => {
                Err(error::Error::ProjStringUnresolvable { spatial_ref: self })
                //TODO: we might need to look them up somehow! Best solution would be a registry where we can store user definexd srs strings.
            }
        }
    }

    /// Return the srs-string "authority:code"
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn srs_string(&self) -> String {
        format!("{}:{}", self.authority, self.code)
    }
}

impl ToSchema for SpatialReference {
    fn schema() -> utoipa::openapi::schema::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new().schema_type(SchemaType::String).into()
    }
}

impl From<geoengine_datatypes::spatial_reference::SpatialReference> for SpatialReference {
    fn from(value: geoengine_datatypes::spatial_reference::SpatialReference) -> Self {
        Self {
            authority: (*value.authority()).into(),
            code: value.code(),
        }
    }
}

impl From<SpatialReference> for geoengine_datatypes::spatial_reference::SpatialReference {
    fn from(value: SpatialReference) -> Self {
        geoengine_datatypes::spatial_reference::SpatialReference::new(
            value.authority.into(),
            value.code,
        )
    }
}

impl SpatialReference {
    pub fn new(authority: SpatialReferenceAuthority, code: u32) -> Self {
        Self { authority, code }
    }

    pub fn authority(&self) -> &SpatialReferenceAuthority {
        &self.authority
    }

    pub fn code(self) -> u32 {
        self.code
    }
}

impl FromStr for SpatialReference {
    type Err = error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split(':');

        match (split.next(), split.next(), split.next()) {
            (Some(authority), Some(code), None) => Ok(Self::new(
                authority.parse()?,
                code.parse::<u32>().context(error::ParseU32)?,
            )),
            _ => Err(error::Error::InvalidSpatialReferenceString {
                spatial_reference_string: s.into(),
            }),
        }
    }
}

impl std::fmt::Display for SpatialReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.authority, self.code)
    }
}

impl Serialize for SpatialReference {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Helper struct for deserializing a `SpatialReferencce`
struct SpatialReferenceDeserializeVisitor;

impl<'de> Visitor<'de> for SpatialReferenceDeserializeVisitor {
    type Value = SpatialReference;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a spatial reference in the form authority:code")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse().map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for SpatialReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SpatialReferenceDeserializeVisitor)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, ToSchema)]
pub enum SpatialReferenceOption {
    SpatialReference(SpatialReference),
    Unreferenced,
}

impl From<geoengine_datatypes::spatial_reference::SpatialReferenceOption>
    for SpatialReferenceOption
{
    fn from(value: geoengine_datatypes::spatial_reference::SpatialReferenceOption) -> Self {
        match value {
            geoengine_datatypes::spatial_reference::SpatialReferenceOption::SpatialReference(s) => {
                Self::SpatialReference(s.into())
            }
            geoengine_datatypes::spatial_reference::SpatialReferenceOption::Unreferenced => {
                Self::Unreferenced
            }
        }
    }
}

impl From<SpatialReferenceOption>
    for geoengine_datatypes::spatial_reference::SpatialReferenceOption
{
    fn from(value: SpatialReferenceOption) -> Self {
        match value {
            SpatialReferenceOption::SpatialReference(sr) => Self::SpatialReference(sr.into()),
            SpatialReferenceOption::Unreferenced => Self::Unreferenced,
        }
    }
}

impl From<SpatialReference> for SpatialReferenceOption {
    fn from(spatial_reference: SpatialReference) -> Self {
        Self::SpatialReference(spatial_reference)
    }
}

impl std::fmt::Display for SpatialReferenceOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpatialReferenceOption::SpatialReference(p) => write!(f, "{p}"),
            SpatialReferenceOption::Unreferenced => Ok(()),
        }
    }
}

impl Serialize for SpatialReferenceOption {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Helper struct for deserializing a `SpatialReferenceOption`
struct SpatialReferenceOptionDeserializeVisitor;

impl<'de> Visitor<'de> for SpatialReferenceOptionDeserializeVisitor {
    type Value = SpatialReferenceOption;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a spatial reference in the form authority:code")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.is_empty() {
            return Ok(SpatialReferenceOption::Unreferenced);
        }

        let spatial_reference: SpatialReference = v.parse().map_err(serde::de::Error::custom)?;

        Ok(spatial_reference.into())
    }
}

impl<'de> Deserialize<'de> for SpatialReferenceOption {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SpatialReferenceOptionDeserializeVisitor)
    }
}

impl From<Option<SpatialReference>> for SpatialReferenceOption {
    fn from(option: Option<SpatialReference>) -> Self {
        match option {
            Some(p) => SpatialReferenceOption::SpatialReference(p),
            None => SpatialReferenceOption::Unreferenced,
        }
    }
}

impl From<SpatialReferenceOption> for Option<SpatialReference> {
    fn from(option: SpatialReferenceOption) -> Self {
        match option {
            SpatialReferenceOption::SpatialReference(p) => Some(p),
            SpatialReferenceOption::Unreferenced => None,
        }
    }
}

/// An enum that contains all possible vector data types
#[derive(
    Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Deserialize, Serialize, Copy, Clone, ToSchema,
)]
pub enum VectorDataType {
    Data,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

impl From<geoengine_datatypes::collections::VectorDataType> for VectorDataType {
    fn from(value: geoengine_datatypes::collections::VectorDataType) -> Self {
        match value {
            geoengine_datatypes::collections::VectorDataType::Data => Self::Data,
            geoengine_datatypes::collections::VectorDataType::MultiPoint => Self::MultiPoint,
            geoengine_datatypes::collections::VectorDataType::MultiLineString => {
                Self::MultiLineString
            }
            geoengine_datatypes::collections::VectorDataType::MultiPolygon => Self::MultiPolygon,
        }
    }
}

impl From<VectorDataType> for geoengine_datatypes::collections::VectorDataType {
    fn from(value: VectorDataType) -> Self {
        match value {
            VectorDataType::Data => Self::Data,
            VectorDataType::MultiPoint => Self::MultiPoint,
            VectorDataType::MultiLineString => Self::MultiLineString,
            VectorDataType::MultiPolygon => Self::MultiPolygon,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, PartialOrd, Serialize, Default, ToSchema)]
pub struct Coordinate2D {
    pub x: f64,
    pub y: f64,
}

impl From<geoengine_datatypes::primitives::Coordinate2D> for Coordinate2D {
    fn from(coordinate: geoengine_datatypes::primitives::Coordinate2D) -> Self {
        Self {
            x: coordinate.x,
            y: coordinate.y,
        }
    }
}

impl From<Coordinate2D> for geoengine_datatypes::primitives::Coordinate2D {
    fn from(coordinate: Coordinate2D) -> Self {
        Self {
            x: coordinate.x,
            y: coordinate.y,
        }
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
/// A bounding box that includes all border points.
/// Note: may degenerate to a point!
pub struct BoundingBox2D {
    pub lower_left_coordinate: Coordinate2D,
    pub upper_right_coordinate: Coordinate2D,
}

impl From<geoengine_datatypes::primitives::BoundingBox2D> for BoundingBox2D {
    fn from(bbox: geoengine_datatypes::primitives::BoundingBox2D) -> Self {
        Self {
            lower_left_coordinate:
                geoengine_datatypes::primitives::AxisAlignedRectangle::lower_left(&bbox).into(),
            upper_right_coordinate:
                geoengine_datatypes::primitives::AxisAlignedRectangle::upper_right(&bbox).into(),
        }
    }
}

impl From<BoundingBox2D> for geoengine_datatypes::primitives::BoundingBox2D {
    fn from(bbox: BoundingBox2D) -> Self {
        Self::new_unchecked(
            bbox.lower_left_coordinate.into(),
            bbox.upper_right_coordinate.into(),
        )
    }
}

/// An object that composes the date and a timestamp with time zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ToSchema)]
pub struct DateTime {
    datetime: chrono::DateTime<chrono::Utc>,
}

impl FromStr for DateTime {
    type Err = geoengine_datatypes::primitives::DateTimeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let date_time = chrono::DateTime::<chrono::FixedOffset>::from_str(input).map_err(|e| {
            Self::Err::DateParse {
                source: Box::new(e),
            }
        })?;

        Ok(date_time.into())
    }
}

impl From<chrono::DateTime<chrono::FixedOffset>> for DateTime {
    fn from(datetime: chrono::DateTime<chrono::FixedOffset>) -> Self {
        Self {
            datetime: datetime.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum FeatureDataType {
    Category,
    Int,
    Float,
    Text,
    Bool,
    DateTime,
}

impl From<geoengine_datatypes::primitives::FeatureDataType> for FeatureDataType {
    fn from(value: geoengine_datatypes::primitives::FeatureDataType) -> Self {
        match value {
            geoengine_datatypes::primitives::FeatureDataType::Category => Self::Category,
            geoengine_datatypes::primitives::FeatureDataType::Int => Self::Int,
            geoengine_datatypes::primitives::FeatureDataType::Float => Self::Float,
            geoengine_datatypes::primitives::FeatureDataType::Text => Self::Text,
            geoengine_datatypes::primitives::FeatureDataType::Bool => Self::Bool,
            geoengine_datatypes::primitives::FeatureDataType::DateTime => Self::DateTime,
        }
    }
}

impl From<FeatureDataType> for geoengine_datatypes::primitives::FeatureDataType {
    fn from(value: FeatureDataType) -> Self {
        match value {
            FeatureDataType::Category => Self::Category,
            FeatureDataType::Int => Self::Int,
            FeatureDataType::Float => Self::Float,
            FeatureDataType::Text => Self::Text,
            FeatureDataType::Bool => Self::Bool,
            FeatureDataType::DateTime => Self::DateTime,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Measurement {
    Unitless,
    Continuous(ContinuousMeasurement),
    Classification(ClassificationMeasurement),
}

impl From<geoengine_datatypes::primitives::Measurement> for Measurement {
    fn from(value: geoengine_datatypes::primitives::Measurement) -> Self {
        match value {
            geoengine_datatypes::primitives::Measurement::Unitless => Self::Unitless,
            geoengine_datatypes::primitives::Measurement::Continuous(cm) => {
                Self::Continuous(cm.into())
            }
            geoengine_datatypes::primitives::Measurement::Classification(cm) => {
                Self::Classification(cm.into())
            }
        }
    }
}

impl From<Measurement> for geoengine_datatypes::primitives::Measurement {
    fn from(value: Measurement) -> Self {
        match value {
            Measurement::Unitless => Self::Unitless,
            Measurement::Continuous(cm) => Self::Continuous(cm.into()),
            Measurement::Classification(cm) => Self::Classification(cm.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct ContinuousMeasurement {
    pub measurement: String,
    pub unit: Option<String>,
}

impl From<geoengine_datatypes::primitives::ContinuousMeasurement> for ContinuousMeasurement {
    fn from(value: geoengine_datatypes::primitives::ContinuousMeasurement) -> Self {
        Self {
            measurement: value.measurement,
            unit: value.unit,
        }
    }
}

impl From<ContinuousMeasurement> for geoengine_datatypes::primitives::ContinuousMeasurement {
    fn from(value: ContinuousMeasurement) -> Self {
        Self {
            measurement: value.measurement,
            unit: value.unit,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(
    try_from = "SerializableClassificationMeasurement",
    into = "SerializableClassificationMeasurement"
)]
pub struct ClassificationMeasurement {
    pub measurement: String,
    pub classes: HashMap<u8, String>,
}

impl From<geoengine_datatypes::primitives::ClassificationMeasurement>
    for ClassificationMeasurement
{
    fn from(value: geoengine_datatypes::primitives::ClassificationMeasurement) -> Self {
        Self {
            measurement: value.measurement,
            classes: value.classes,
        }
    }
}

impl From<ClassificationMeasurement>
    for geoengine_datatypes::primitives::ClassificationMeasurement
{
    fn from(value: ClassificationMeasurement) -> Self {
        Self {
            measurement: value.measurement,
            classes: value.classes,
        }
    }
}

/// A type that is solely for serde's serializability.
/// You cannot serialize floats as JSON map keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializableClassificationMeasurement {
    pub measurement: String,
    // use a BTreeMap to preserve the order of the keys
    pub classes: BTreeMap<String, String>,
}

impl From<ClassificationMeasurement> for SerializableClassificationMeasurement {
    fn from(measurement: ClassificationMeasurement) -> Self {
        let mut classes = BTreeMap::new();
        for (k, v) in measurement.classes {
            classes.insert(k.to_string(), v);
        }
        Self {
            measurement: measurement.measurement,
            classes,
        }
    }
}

impl TryFrom<SerializableClassificationMeasurement> for ClassificationMeasurement {
    type Error = <u8 as FromStr>::Err;

    fn try_from(measurement: SerializableClassificationMeasurement) -> Result<Self, Self::Error> {
        let mut classes = HashMap::with_capacity(measurement.classes.len());
        for (k, v) in measurement.classes {
            classes.insert(k.parse::<u8>()?, v);
        }
        Ok(Self {
            measurement: measurement.measurement,
            classes,
        })
    }
}

/// A partition of space that include the upper left but excludes the lower right coordinate
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SpatialPartition2D {
    upper_left_coordinate: Coordinate2D,
    lower_right_coordinate: Coordinate2D,
}

impl From<geoengine_datatypes::primitives::SpatialPartition2D> for SpatialPartition2D {
    fn from(value: geoengine_datatypes::primitives::SpatialPartition2D) -> Self {
        Self {
            upper_left_coordinate: value.upper_left().into(),
            lower_right_coordinate: value.lower_right().into(),
        }
    }
}

impl From<SpatialPartition2D> for geoengine_datatypes::primitives::SpatialPartition2D {
    fn from(value: SpatialPartition2D) -> Self {
        Self::new_unchecked(
            value.upper_left_coordinate.into(),
            value.lower_right_coordinate.into(),
        )
    }
}

/// A spatio-temporal rectangle with a specified resolution
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRectangle<SpatialBounds> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D>;
pub type RasterQueryRectangle = QueryRectangle<SpatialPartition2D>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D>;

// manual implementation, because derivation can't handle the `SpatialBounds` generic (yet)
impl ToSchema for QueryRectangle<SpatialPartition2D> {
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
impl ToSchema for QueryRectangle<BoundingBox2D> {
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

/// The spatial resolution in SRS units
#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct SpatialResolution {
    pub x: f64,
    pub y: f64,
}

impl From<geoengine_datatypes::primitives::SpatialResolution> for SpatialResolution {
    fn from(value: geoengine_datatypes::primitives::SpatialResolution) -> Self {
        Self {
            x: value.x,
            y: value.y,
        }
    }
}

impl From<SpatialResolution> for geoengine_datatypes::primitives::SpatialResolution {
    fn from(value: SpatialResolution) -> Self {
        Self {
            x: value.x,
            y: value.y,
        }
    }
}

#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord, Debug, ToSchema)]
#[repr(C)]
pub struct TimeInstance(i64);

impl FromStr for TimeInstance {
    type Err = geoengine_datatypes::primitives::DateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let date_time = DateTime::from_str(s)?;
        Ok(date_time.into())
    }
}

impl From<geoengine_datatypes::primitives::TimeInstance> for TimeInstance {
    fn from(value: geoengine_datatypes::primitives::TimeInstance) -> Self {
        Self(value.inner())
    }
}

impl From<TimeInstance> for geoengine_datatypes::primitives::TimeInstance {
    fn from(value: TimeInstance) -> Self {
        geoengine_datatypes::primitives::TimeInstance::from_millis_unchecked(value.inner())
    }
}

impl From<DateTime> for TimeInstance {
    fn from(datetime: DateTime) -> Self {
        Self::from(&datetime)
    }
}

impl From<&DateTime> for TimeInstance {
    fn from(datetime: &DateTime) -> Self {
        geoengine_datatypes::primitives::TimeInstance::from_millis_unchecked(
            datetime.datetime.timestamp_millis(),
        )
        .into()
    }
}

impl TimeInstance {
    pub const fn inner(self) -> i64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for TimeInstance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IsoStringOrUnixTimestamp;

        impl<'de> serde::de::Visitor<'de> for IsoStringOrUnixTimestamp {
            type Value = TimeInstance;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("RFC 3339 timestamp string or Unix timestamp integer")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                TimeInstance::from_str(value).map_err(E::custom)
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                geoengine_datatypes::primitives::TimeInstance::from_millis(v)
                    .map(Into::into)
                    .map_err(E::custom)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Self::visit_i64(self, v as i64)
            }
        }

        deserializer.deserialize_any(IsoStringOrUnixTimestamp)
    }
}

/// A time granularity.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum TimeGranularity {
    Millis,
    Seconds,
    Minutes,
    Hours,
    Days,
    Months,
    Years,
}

impl From<geoengine_datatypes::primitives::TimeGranularity> for TimeGranularity {
    fn from(value: geoengine_datatypes::primitives::TimeGranularity) -> Self {
        match value {
            geoengine_datatypes::primitives::TimeGranularity::Millis => Self::Millis,
            geoengine_datatypes::primitives::TimeGranularity::Seconds => Self::Seconds,
            geoengine_datatypes::primitives::TimeGranularity::Minutes => Self::Minutes,
            geoengine_datatypes::primitives::TimeGranularity::Hours => Self::Hours,
            geoengine_datatypes::primitives::TimeGranularity::Days => Self::Days,
            geoengine_datatypes::primitives::TimeGranularity::Months => Self::Months,
            geoengine_datatypes::primitives::TimeGranularity::Years => Self::Years,
        }
    }
}

impl From<TimeGranularity> for geoengine_datatypes::primitives::TimeGranularity {
    fn from(value: TimeGranularity) -> Self {
        match value {
            TimeGranularity::Millis => Self::Millis,
            TimeGranularity::Seconds => Self::Seconds,
            TimeGranularity::Minutes => Self::Minutes,
            TimeGranularity::Hours => Self::Hours,
            TimeGranularity::Days => Self::Days,
            TimeGranularity::Months => Self::Months,
            TimeGranularity::Years => Self::Years,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TimeStep {
    pub granularity: TimeGranularity,
    pub step: u32, // TODO: ensure on deserialization it is > 0
}

impl From<geoengine_datatypes::primitives::TimeStep> for TimeStep {
    fn from(value: geoengine_datatypes::primitives::TimeStep) -> Self {
        Self {
            granularity: value.granularity.into(),
            step: value.step,
        }
    }
}

impl From<TimeStep> for geoengine_datatypes::primitives::TimeStep {
    fn from(value: TimeStep) -> Self {
        Self {
            granularity: value.granularity.into(),
            step: value.step,
        }
    }
}

/// Stores time intervals in ms in close-open semantic [start, end)
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub struct TimeInterval {
    start: TimeInstance,
    end: TimeInstance,
}

impl ToSchema for TimeInterval {
    fn schema() -> utoipa::openapi::schema::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new().schema_type(SchemaType::String).into()
    }
}

impl From<TimeInterval> for geoengine_datatypes::primitives::TimeInterval {
    fn from(value: TimeInterval) -> Self {
        geoengine_datatypes::primitives::TimeInterval::new_unchecked::<
            geoengine_datatypes::primitives::TimeInstance,
            geoengine_datatypes::primitives::TimeInstance,
        >(value.start.into(), value.end.into())
    }
}

impl From<geoengine_datatypes::primitives::TimeInterval> for TimeInterval {
    fn from(value: geoengine_datatypes::primitives::TimeInterval) -> Self {
        Self {
            start: value.start().into(),
            end: value.end().into(),
        }
    }
}

impl core::fmt::Debug for TimeInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "TimeInterval [{}, {})",
            self.start.inner(),
            &self.end.inner()
        )
    }
}

#[derive(
    Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Deserialize, Serialize, Copy, Clone, ToSchema,
)]
pub enum RasterDataType {
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
}

impl From<geoengine_datatypes::raster::RasterDataType> for RasterDataType {
    fn from(value: geoengine_datatypes::raster::RasterDataType) -> Self {
        match value {
            geoengine_datatypes::raster::RasterDataType::U8 => Self::U8,
            geoengine_datatypes::raster::RasterDataType::U16 => Self::U16,
            geoengine_datatypes::raster::RasterDataType::U32 => Self::U32,
            geoengine_datatypes::raster::RasterDataType::U64 => Self::U64,
            geoengine_datatypes::raster::RasterDataType::I8 => Self::I8,
            geoengine_datatypes::raster::RasterDataType::I16 => Self::I16,
            geoengine_datatypes::raster::RasterDataType::I32 => Self::I32,
            geoengine_datatypes::raster::RasterDataType::I64 => Self::I64,
            geoengine_datatypes::raster::RasterDataType::F32 => Self::F32,
            geoengine_datatypes::raster::RasterDataType::F64 => Self::F64,
        }
    }
}

impl From<RasterDataType> for geoengine_datatypes::raster::RasterDataType {
    fn from(value: RasterDataType) -> Self {
        match value {
            RasterDataType::U8 => Self::U8,
            RasterDataType::U16 => Self::U16,
            RasterDataType::U32 => Self::U32,
            RasterDataType::U64 => Self::U64,
            RasterDataType::I8 => Self::I8,
            RasterDataType::I16 => Self::I16,
            RasterDataType::I32 => Self::I32,
            RasterDataType::I64 => Self::I64,
            RasterDataType::F32 => Self::F32,
            RasterDataType::F64 => Self::F64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum ResamplingMethod {
    Nearest,
    Average,
    Bilinear,
    Cubic,
    CubicSpline,
    Lanczos,
}

impl std::fmt::Display for ResamplingMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResamplingMethod::Nearest => write!(f, "NEAREST"),
            ResamplingMethod::Average => write!(f, "AVERAGE"),
            ResamplingMethod::Bilinear => write!(f, "BILINEAR"),
            ResamplingMethod::Cubic => write!(f, "CUBIC"),
            ResamplingMethod::CubicSpline => write!(f, "CUBICSPLINE"),
            ResamplingMethod::Lanczos => write!(f, "LANCZOS"),
        }
    }
}

/// `RgbaColor` defines a 32 bit RGB color with alpha value
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RgbaColor([u8; 4]);

// manual implementation utoipa generates an integer field
impl ToSchema for RgbaColor {
    fn schema() -> utoipa::openapi::schema::Schema {
        use utoipa::openapi::*;
        ArrayBuilder::new()
            .items(ObjectBuilder::new().schema_type(SchemaType::Integer))
            .min_items(Some(4))
            .max_items(Some(4))
            .into()
    }
}

impl From<geoengine_datatypes::operations::image::RgbaColor> for RgbaColor {
    fn from(color: geoengine_datatypes::operations::image::RgbaColor) -> Self {
        Self(color.into_inner())
    }
}

/// A container type for breakpoints that specify a value to color mapping
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct Breakpoint {
    pub value: NotNan<f64>,
    pub color: RgbaColor,
}

// manual implementation because of NotNan
impl ToSchema for Breakpoint {
    fn schema() -> utoipa::openapi::schema::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property(
                "value",
                ObjectBuilder::new().schema_type(SchemaType::Number),
            )
            .property("color", Ref::from_schema_name("RgbaColor"))
            .into()
    }
}

impl From<geoengine_datatypes::operations::image::Breakpoint> for Breakpoint {
    fn from(breakpoint: geoengine_datatypes::operations::image::Breakpoint) -> Self {
        Self {
            value: breakpoint.value,
            color: breakpoint.color.into(),
        }
    }
}

/// A colorizer specifies a mapping between raster values and an output image
/// There are different variants that perform different kinds of mapping.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Colorizer {
    #[serde(rename_all = "camelCase")]
    LinearGradient {
        breakpoints: Vec<Breakpoint>,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    },
    #[serde(rename_all = "camelCase")]
    LogarithmicGradient {
        breakpoints: Vec<Breakpoint>,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    },
    #[serde(rename_all = "camelCase")]
    Palette {
        colors: Palette,
        no_data_color: RgbaColor,
        default_color: RgbaColor,
    },
    Rgba,
}

impl From<geoengine_datatypes::operations::image::Colorizer> for Colorizer {
    fn from(v: geoengine_datatypes::operations::image::Colorizer) -> Self {
        match v {
            geoengine_datatypes::operations::image::Colorizer::LinearGradient {
                breakpoints,
                no_data_color,
                default_color,
            } => Self::LinearGradient {
                breakpoints: breakpoints
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<Breakpoint>>(),
                no_data_color: no_data_color.into(),
                default_color: default_color.into(),
            },
            geoengine_datatypes::operations::image::Colorizer::LogarithmicGradient {
                breakpoints,
                no_data_color,
                default_color,
            } => Self::LogarithmicGradient {
                breakpoints: breakpoints
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<Breakpoint>>(),
                no_data_color: no_data_color.into(),
                default_color: default_color.into(),
            },
            geoengine_datatypes::operations::image::Colorizer::Palette {
                colors,
                no_data_color,
                default_color,
            } => Self::Palette {
                colors: colors.into(),
                no_data_color: no_data_color.into(),
                default_color: default_color.into(),
            },
            geoengine_datatypes::operations::image::Colorizer::Rgba => Self::Rgba,
        }
    }
}

/// A map from value to color
///
/// It is assumed that is has at least one and at most 256 entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(try_from = "SerializablePalette", into = "SerializablePalette")]
pub struct Palette(HashMap<NotNan<f64>, RgbaColor>);

impl From<geoengine_datatypes::operations::image::Palette> for Palette {
    fn from(palette: geoengine_datatypes::operations::image::Palette) -> Self {
        Self(
            palette
                .into_inner()
                .into_iter()
                .map(|(value, color)| (value, color.into()))
                .collect(),
        )
    }
}

/// A type that is solely for serde's serializability.
/// You cannot serialize floats as JSON map keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializablePalette(HashMap<String, RgbaColor>);

impl From<Palette> for SerializablePalette {
    fn from(palette: Palette) -> Self {
        Self(
            palette
                .0
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }
}

impl TryFrom<SerializablePalette> for Palette {
    type Error = <NotNan<f64> as FromStr>::Err;

    fn try_from(palette: SerializablePalette) -> Result<Self, Self::Error> {
        let mut inner = HashMap::<NotNan<f64>, RgbaColor>::with_capacity(palette.0.len());
        for (k, v) in palette.0 {
            inner.insert(k.parse()?, v);
        }
        Ok(Self(inner))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq, PartialOrd, Ord, ToSchema)]
pub struct RasterPropertiesKey {
    pub domain: Option<String>,
    pub key: String,
}

impl From<geoengine_datatypes::raster::RasterPropertiesKey> for RasterPropertiesKey {
    fn from(value: geoengine_datatypes::raster::RasterPropertiesKey) -> Self {
        Self {
            domain: value.domain,
            key: value.key,
        }
    }
}

impl From<RasterPropertiesKey> for geoengine_datatypes::raster::RasterPropertiesKey {
    fn from(value: RasterPropertiesKey) -> Self {
        Self {
            domain: value.domain,
            key: value.key,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum RasterPropertiesEntryType {
    Number,
    String,
}

impl From<geoengine_datatypes::raster::RasterPropertiesEntryType> for RasterPropertiesEntryType {
    fn from(value: geoengine_datatypes::raster::RasterPropertiesEntryType) -> Self {
        match value {
            geoengine_datatypes::raster::RasterPropertiesEntryType::Number => Self::Number,
            geoengine_datatypes::raster::RasterPropertiesEntryType::String => Self::String,
        }
    }
}

impl From<RasterPropertiesEntryType> for geoengine_datatypes::raster::RasterPropertiesEntryType {
    fn from(value: RasterPropertiesEntryType) -> Self {
        match value {
            RasterPropertiesEntryType::Number => Self::Number,
            RasterPropertiesEntryType::String => Self::String,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug, ToSchema)]
pub struct DateTimeParseFormat {
    fmt: String,
    has_tz: bool,
    has_time: bool,
}

impl From<geoengine_datatypes::primitives::DateTimeParseFormat> for DateTimeParseFormat {
    fn from(value: geoengine_datatypes::primitives::DateTimeParseFormat) -> Self {
        Self {
            fmt: value._to_parse_format().to_string(),
            has_tz: value.has_tz(),
            has_time: value.has_time(),
        }
    }
}

impl From<DateTimeParseFormat> for geoengine_datatypes::primitives::DateTimeParseFormat {
    fn from(value: DateTimeParseFormat) -> Self {
        Self::custom(value.fmt)
    }
}

impl DateTimeParseFormat {
    // this is used as default value
    pub fn unix() -> Self {
        geoengine_datatypes::primitives::DateTimeParseFormat::unix().into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoGeometry;

impl From<geoengine_datatypes::primitives::NoGeometry> for NoGeometry {
    fn from(_: geoengine_datatypes::primitives::NoGeometry) -> Self {
        Self {}
    }
}

impl From<NoGeometry> for geoengine_datatypes::primitives::NoGeometry {
    fn from(_: NoGeometry) -> Self {
        Self {}
    }
}

impl ToSchema for NoGeometry {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .default(Some(serde_json::Value::Null))
            .example(Some(serde_json::Value::Null))
            .into()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MultiPoint {
    coordinates: Vec<Coordinate2D>,
}

impl From<geoengine_datatypes::primitives::MultiPoint> for MultiPoint {
    fn from(value: geoengine_datatypes::primitives::MultiPoint) -> Self {
        Self {
            coordinates: value.points().iter().map(|x| (*x).into()).collect(),
        }
    }
}

impl From<MultiPoint> for geoengine_datatypes::primitives::MultiPoint {
    fn from(value: MultiPoint) -> Self {
        Self::new(value.coordinates.into_iter().map(Into::into).collect()).unwrap()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MultiLineString {
    coordinates: Vec<Vec<Coordinate2D>>,
}

impl From<geoengine_datatypes::primitives::MultiLineString> for MultiLineString {
    fn from(value: geoengine_datatypes::primitives::MultiLineString) -> Self {
        Self {
            coordinates: value
                .lines()
                .iter()
                .map(|x| x.iter().map(|x| (*x).into()).collect())
                .collect(),
        }
    }
}

impl From<MultiLineString> for geoengine_datatypes::primitives::MultiLineString {
    fn from(value: MultiLineString) -> Self {
        Self::new(
            value
                .coordinates
                .into_iter()
                .map(|x| x.into_iter().map(Into::into).collect())
                .collect(),
        )
        .unwrap()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MultiPolygon {
    polygons: Vec<Vec<Vec<Coordinate2D>>>,
}

impl From<geoengine_datatypes::primitives::MultiPolygon> for MultiPolygon {
    fn from(value: geoengine_datatypes::primitives::MultiPolygon) -> Self {
        Self {
            polygons: value
                .polygons()
                .iter()
                .map(|x| {
                    x.iter()
                        .map(|y| y.iter().map(|y| (*y).into()).collect())
                        .collect()
                })
                .collect(),
        }
    }
}

impl From<MultiPolygon> for geoengine_datatypes::primitives::MultiPolygon {
    fn from(value: MultiPolygon) -> Self {
        Self::new(
            value
                .polygons
                .iter()
                .map(|x| {
                    x.iter()
                        .map(|y| y.iter().map(|y| (*y).into()).collect())
                        .collect()
                })
                .collect(),
        )
        .unwrap()
    }
}

#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, Clone)]
pub struct StringPair((String, String));

impl ToSchema for StringPair {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ArrayBuilder::new()
            .items(Object::with_type(SchemaType::String))
            .min_items(Some(2))
            .max_items(Some(2))
            .into()
    }
}

impl From<(String, String)> for StringPair {
    fn from(value: (String, String)) -> Self {
        Self(value)
    }
}

impl From<StringPair> for (String, String) {
    fn from(value: StringPair) -> Self {
        value.0
    }
}
