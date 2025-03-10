use super::type_tag;
use crate::error::{self, Error, Result};
use crate::identifier;
use geoengine_datatypes::operations::image::RgbParams;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, MultiLineStringAccess, MultiPointAccess, MultiPolygonAccess,
};
use ordered_float::NotNan;
use postgres_types::{FromSql, ToSql};
use serde::{de::Visitor, Deserialize, Deserializer, Serialize, Serializer};
use snafu::ResultExt;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Formatter},
    str::FromStr,
};
use utoipa::{PartialSchema, ToSchema};

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
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
/// The identifier for loadable data. It is used in the source operators to get the loading info (aka parametrization)
/// for accessing the data. Internal data is loaded from datasets, external from `DataProvider`s.
pub enum DataId {
    Internal(InternalDataId),
    External(ExternalDataId),
}

type_tag!(Internal);
type_tag!(External);

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InternalDataId {
    r#type: InternalTag,
    pub dataset_id: DatasetId,
}

impl DataId {
    pub fn internal(&self) -> Option<DatasetId> {
        if let Self::Internal(internal) = self {
            return Some(internal.dataset_id);
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
        Self::Internal(InternalDataId {
            r#type: Default::default(),
            dataset_id: value,
        })
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
            geoengine_datatypes::dataset::DataId::Internal { dataset_id } => {
                Self::Internal(InternalDataId {
                    r#type: Default::default(),
                    dataset_id: dataset_id.into(),
                })
            }
            geoengine_datatypes::dataset::DataId::External(external_id) => {
                Self::External(external_id.into())
            }
        }
    }
}

impl From<DataId> for geoengine_datatypes::dataset::DataId {
    fn from(id: DataId) -> Self {
        match id {
            DataId::Internal(internal) => Self::Internal {
                dataset_id: internal.dataset_id.into(),
            },
            DataId::External(external_id) => Self::External(external_id.into()),
        }
    }
}

impl From<&DataId> for geoengine_datatypes::dataset::DataId {
    fn from(id: &DataId) -> Self {
        match id {
            DataId::Internal(internal) => Self::Internal {
                dataset_id: internal.dataset_id.into(),
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

impl From<&DatasetId> for geoengine_datatypes::dataset::DatasetId {
    fn from(value: &DatasetId) -> Self {
        Self(value.0)
    }
}

impl From<&ExternalDataId> for geoengine_datatypes::dataset::ExternalDataId {
    fn from(value: &ExternalDataId) -> Self {
        Self {
            provider_id: value.provider_id.into(),
            layer_id: value.layer_id.clone().into(),
        }
    }
}

/// The user-facing identifier for loadable data.
/// It can be resolved into a [`DataId`].
#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
// TODO: Have separate type once `geoengine_datatypes::dataset::NamedData` is not part of the API anymore.
#[serde(
    from = "geoengine_datatypes::dataset::NamedData",
    into = "geoengine_datatypes::dataset::NamedData"
)]
pub struct NamedData {
    pub namespace: Option<String>,
    pub provider: Option<String>,
    pub name: String,
}

impl From<geoengine_datatypes::dataset::NamedData> for NamedData {
    fn from(
        geoengine_datatypes::dataset::NamedData {
            namespace,
            provider,
            name,
        }: geoengine_datatypes::dataset::NamedData,
    ) -> Self {
        Self {
            namespace,
            provider,
            name,
        }
    }
}

impl From<&geoengine_datatypes::dataset::NamedData> for NamedData {
    fn from(named_data: &geoengine_datatypes::dataset::NamedData) -> Self {
        Self::from(named_data.clone())
    }
}

impl From<NamedData> for geoengine_datatypes::dataset::NamedData {
    fn from(
        NamedData {
            namespace,
            provider,
            name,
        }: NamedData,
    ) -> Self {
        Self {
            namespace,
            provider,
            name,
        }
    }
}

impl From<&NamedData> for geoengine_datatypes::dataset::NamedData {
    fn from(named_data: &NamedData) -> Self {
        Self::from(named_data.clone())
    }
}

impl PartialSchema for NamedData {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, SchemaType, Type};
        ObjectBuilder::new()
            .schema_type(SchemaType::Type(Type::String))
            .into()
    }
}

impl ToSchema for NamedData {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, ToSql, FromSql)]
pub struct LayerId(pub String); // TODO: differentiate between internal layer ids (UUID) and external layer ids (String)

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
    r#type: ExternalTag,
    pub provider_id: DataProviderId,
    pub layer_id: LayerId,
}

impl From<geoengine_datatypes::dataset::ExternalDataId> for ExternalDataId {
    fn from(id: geoengine_datatypes::dataset::ExternalDataId) -> Self {
        Self {
            r#type: Default::default(),
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
#[derive(
    Debug,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    ToSchema,
    FromSql,
    ToSql,
)]
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
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, FromSql, ToSql)]
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

impl Visitor<'_> for SpatialReferenceDeserializeVisitor {
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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
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

impl Visitor<'_> for SpatialReferenceOptionDeserializeVisitor {
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

impl ToSql for SpatialReferenceOption {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            SpatialReferenceOption::SpatialReference(sref) => sref.to_sql(ty, out),
            SpatialReferenceOption::Unreferenced => Ok(postgres_types::IsNull::Yes),
        }
    }

    fn accepts(ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        <SpatialReference as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            SpatialReferenceOption::SpatialReference(sref) => sref.to_sql_checked(ty, out),
            SpatialReferenceOption::Unreferenced => Ok(postgres_types::IsNull::Yes),
        }
    }
}

impl<'a> FromSql<'a> for SpatialReferenceOption {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(SpatialReferenceOption::SpatialReference(
            SpatialReference::from_sql(ty, raw)?,
        ))
    }

    fn from_sql_null(
        _: &postgres_types::Type,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(SpatialReferenceOption::Unreferenced)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <SpatialReference as FromSql>::accepts(ty)
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

#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    PartialEq,
    PartialOrd,
    Serialize,
    Default,
    ToSchema,
    ToSql,
    FromSql,
)]
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

#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug, ToSchema, ToSql, FromSql)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateTimeString {
    datetime: chrono::DateTime<chrono::Utc>,
}

// TODO: use derive ToSchema when OpenAPI derive does not break
impl PartialSchema for DateTimeString {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, SchemaType, Type};
        ObjectBuilder::new()
            .schema_type(SchemaType::Type(Type::String))
            .into()
    }
}

impl ToSchema for DateTimeString {
    // fn name() -> Cow<'static, str> {
    //     "DateTime2".into()
    // }
}

impl FromStr for DateTimeString {
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

impl From<chrono::DateTime<chrono::FixedOffset>> for DateTimeString {
    fn from(datetime: chrono::DateTime<chrono::FixedOffset>) -> Self {
        Self {
            datetime: datetime.into(),
        }
    }
}

impl From<geoengine_datatypes::primitives::DateTime> for DateTimeString {
    fn from(datetime: geoengine_datatypes::primitives::DateTime) -> Self {
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

type_tag!(Unitless);
type_tag!(Continuous);
type_tag!(Classification);

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum Measurement {
    Unitless(UnitlessMeasurement),
    Continuous(ContinuousMeasurement),
    Classification(ClassificationMeasurement),
}

impl From<geoengine_datatypes::primitives::Measurement> for Measurement {
    fn from(value: geoengine_datatypes::primitives::Measurement) -> Self {
        match value {
            geoengine_datatypes::primitives::Measurement::Unitless => {
                Self::Unitless(UnitlessMeasurement {
                    r#type: Default::default(),
                })
            }
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
            Measurement::Unitless { .. } => Self::Unitless,
            Measurement::Continuous(cm) => Self::Continuous(cm.into()),
            Measurement::Classification(cm) => Self::Classification(cm.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema, Default)]
pub struct UnitlessMeasurement {
    r#type: UnitlessTag,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct ContinuousMeasurement {
    r#type: ContinuousTag,
    pub measurement: String,
    pub unit: Option<String>,
}

impl From<geoengine_datatypes::primitives::ContinuousMeasurement> for ContinuousMeasurement {
    fn from(value: geoengine_datatypes::primitives::ContinuousMeasurement) -> Self {
        Self {
            r#type: Default::default(),
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
    r#type: ClassificationTag,
    pub measurement: String,
    pub classes: HashMap<u8, String>,
}

impl From<geoengine_datatypes::primitives::ClassificationMeasurement>
    for ClassificationMeasurement
{
    fn from(value: geoengine_datatypes::primitives::ClassificationMeasurement) -> Self {
        Self {
            r#type: Default::default(),
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
            r#type: Default::default(),
            measurement: measurement.measurement,
            classes,
        })
    }
}

/// A partition of space that include the upper left but excludes the lower right coordinate
#[derive(Copy, Clone, Serialize, Deserialize, PartialEq, Debug, ToSchema, FromSql, ToSql)]
#[serde(rename_all = "camelCase")]
pub struct SpatialPartition2D {
    pub upper_left_coordinate: Coordinate2D,
    pub lower_right_coordinate: Coordinate2D,
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
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
// #[aliases( // TODO: find solution?
//     VectorQueryRectangle = QueryRectangle<BoundingBox2D>,
//     RasterQueryRectangle = QueryRectangle<SpatialPartition2D>,
//     PlotQueryRectangle = QueryRectangle<BoundingBox2D>)
// ]
pub struct QueryRectangle<SpatialBounds> {
    pub spatial_bounds: SpatialBounds,
    pub time_interval: TimeInterval,
    pub spatial_resolution: SpatialResolution,
}

pub type RasterQueryRectangle = QueryRectangle<SpatialPartition2D>;
pub type VectorQueryRectangle = QueryRectangle<BoundingBox2D>;
pub type PlotQueryRectangle = QueryRectangle<BoundingBox2D>;

impl
    From<
        geoengine_datatypes::primitives::QueryRectangle<
            geoengine_datatypes::primitives::SpatialPartition2D,
            geoengine_datatypes::primitives::BandSelection,
        >,
    > for RasterQueryRectangle
{
    fn from(
        value: geoengine_datatypes::primitives::QueryRectangle<
            geoengine_datatypes::primitives::SpatialPartition2D,
            geoengine_datatypes::primitives::BandSelection,
        >,
    ) -> Self {
        Self {
            spatial_bounds: value.spatial_bounds.into(),
            time_interval: value.time_interval.into(),
            spatial_resolution: value.spatial_resolution.into(),
        }
    }
}

impl From<QueryRectangle<SpatialPartition2D>>
    for geoengine_datatypes::primitives::RasterQueryRectangle
{
    fn from(value: QueryRectangle<SpatialPartition2D>) -> Self {
        Self {
            spatial_bounds: value.spatial_bounds.into(),
            time_interval: value.time_interval.into(),
            spatial_resolution: value.spatial_resolution.into(),
            attributes: geoengine_datatypes::primitives::BandSelection::first(), // TODO: adjust once API supports attribute selection
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct BandSelection(pub Vec<usize>);

impl From<geoengine_datatypes::primitives::BandSelection> for BandSelection {
    fn from(value: geoengine_datatypes::primitives::BandSelection) -> Self {
        Self(value.as_vec().into_iter().map(|b| b as usize).collect())
    }
}

impl TryFrom<BandSelection> for geoengine_datatypes::primitives::BandSelection {
    type Error = Error;

    fn try_from(value: BandSelection) -> Result<Self> {
        geoengine_datatypes::primitives::BandSelection::new(
            value.0.into_iter().map(|b| b as u32).collect(),
        )
        .map_err(Into::into)
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

#[derive(
    Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord, Debug, ToSchema, FromSql, ToSql,
)]
#[repr(C)]
#[postgres(transparent)]
pub struct TimeInstance(i64);

impl FromStr for TimeInstance {
    type Err = geoengine_datatypes::primitives::DateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let date_time = DateTimeString::from_str(s)?;
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

impl From<DateTimeString> for TimeInstance {
    fn from(datetime: DateTimeString) -> Self {
        Self::from(&datetime)
    }
}

impl From<&DateTimeString> for TimeInstance {
    fn from(datetime: &DateTimeString) -> Self {
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

        impl serde::de::Visitor<'_> for IsoStringOrUnixTimestamp {
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, FromSql, ToSql)]
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, FromSql, ToSql)]
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
#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, ToSql, FromSql, ToSchema)]
pub struct TimeInterval {
    start: TimeInstance,
    end: TimeInstance,
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
    Debug,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash,
    Deserialize,
    Serialize,
    Copy,
    Clone,
    ToSchema,
    FromSql,
    ToSql,
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

impl From<ResamplingMethod> for geoengine_datatypes::util::gdal::ResamplingMethod {
    fn from(value: ResamplingMethod) -> Self {
        match value {
            ResamplingMethod::Nearest => Self::Nearest,
            ResamplingMethod::Average => Self::Average,
            ResamplingMethod::Bilinear => Self::Bilinear,
            ResamplingMethod::Cubic => Self::Cubic,
            ResamplingMethod::CubicSpline => Self::CubicSpline,
            ResamplingMethod::Lanczos => Self::Lanczos,
        }
    }
}

/// `RgbaColor` defines a 32 bit RGB color with alpha value
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RgbaColor(pub [u8; 4]);

impl PartialSchema for RgbaColor {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::schema::{ArrayBuilder, ObjectBuilder, SchemaType, Type};
        ArrayBuilder::new()
            .items(ObjectBuilder::new().schema_type(SchemaType::Type(Type::Integer)))
            .min_items(Some(4))
            .max_items(Some(4))
            .into()
    }
}

// manual implementation utoipa generates an integer field
impl ToSchema for RgbaColor {}

impl From<geoengine_datatypes::operations::image::RgbaColor> for RgbaColor {
    fn from(color: geoengine_datatypes::operations::image::RgbaColor) -> Self {
        Self(color.into_inner())
    }
}

impl From<RgbaColor> for geoengine_datatypes::operations::image::RgbaColor {
    fn from(color: RgbaColor) -> Self {
        Self::new(color.0[0], color.0[1], color.0[2], color.0[3])
    }
}

/// A container type for breakpoints that specify a value to color mapping
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct Breakpoint {
    pub value: NotNanF64,
    pub color: RgbaColor,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct NotNanF64(NotNan<f64>);

impl From<NotNan<f64>> for NotNanF64 {
    fn from(value: NotNan<f64>) -> Self {
        Self(value)
    }
}

impl From<NotNanF64> for NotNan<f64> {
    fn from(value: NotNanF64) -> Self {
        value.0
    }
}

impl ToSql for NotNanF64 {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <f64 as ToSql>::to_sql(&self.0.into_inner(), ty, w)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <f64 as ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for NotNanF64 {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<NotNanF64, Box<dyn std::error::Error + Sync + Send>> {
        let value = <f64 as FromSql>::from_sql(ty, raw)?;

        Ok(NotNanF64(value.try_into()?))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <f64 as FromSql>::accepts(ty)
    }
}

impl PartialSchema for Breakpoint {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::schema::{Object, ObjectBuilder, Ref, SchemaType, Type};
        ObjectBuilder::new()
            .property("value", Object::with_type(SchemaType::Type(Type::Number)))
            .property("color", Ref::from_schema_name("RgbaColor"))
            .required("value")
            .required("color")
            .into()
    }
}

// manual implementation because of NotNan
impl ToSchema for Breakpoint {}

impl From<geoengine_datatypes::operations::image::Breakpoint> for Breakpoint {
    fn from(breakpoint: geoengine_datatypes::operations::image::Breakpoint) -> Self {
        Self {
            value: breakpoint.value.into(),
            color: breakpoint.color.into(),
        }
    }
}

impl From<Breakpoint> for geoengine_datatypes::operations::image::Breakpoint {
    fn from(breakpoint: Breakpoint) -> Self {
        Self {
            value: breakpoint.value.into(),
            color: breakpoint.color.into(),
        }
    }
}

type_tag!(LinearGradient);
type_tag!(LogarithmicGradient);
type_tag!(Palette);

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LinearGradient {
    r#type: LogarithmicGradientTag,
    pub breakpoints: Vec<Breakpoint>,
    pub no_data_color: RgbaColor,
    pub over_color: RgbaColor,
    pub under_color: RgbaColor,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LogarithmicGradient {
    r#type: LogarithmicGradientTag,
    pub breakpoints: Vec<Breakpoint>,
    pub no_data_color: RgbaColor,
    pub over_color: RgbaColor,
    pub under_color: RgbaColor,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaletteColorizer {
    r#type: PaletteTag,
    pub colors: Palette,
    pub no_data_color: RgbaColor,
    pub default_color: RgbaColor,
}

/// A colorizer specifies a mapping between raster values and an output image
/// There are different variants that perform different kinds of mapping.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum Colorizer {
    LinearGradient(LinearGradient),
    LogarithmicGradient(LogarithmicGradient),
    Palette(PaletteColorizer),
}

impl From<geoengine_datatypes::operations::image::Colorizer> for Colorizer {
    fn from(v: geoengine_datatypes::operations::image::Colorizer) -> Self {
        match v {
            geoengine_datatypes::operations::image::Colorizer::LinearGradient {
                breakpoints,
                no_data_color,
                over_color,
                under_color,
            } => Self::LinearGradient(LinearGradient {
                r#type: Default::default(),
                breakpoints: breakpoints
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<Breakpoint>>(),
                no_data_color: no_data_color.into(),
                over_color: over_color.into(),
                under_color: under_color.into(),
            }),
            geoengine_datatypes::operations::image::Colorizer::LogarithmicGradient {
                breakpoints,
                no_data_color,
                over_color,
                under_color,
            } => Self::LogarithmicGradient(LogarithmicGradient {
                r#type: Default::default(),
                breakpoints: breakpoints
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<Breakpoint>>(),
                no_data_color: no_data_color.into(),
                over_color: over_color.into(),
                under_color: under_color.into(),
            }),
            geoengine_datatypes::operations::image::Colorizer::Palette {
                colors,
                no_data_color,
                default_color,
            } => Self::Palette(PaletteColorizer {
                r#type: Default::default(),
                colors: colors.into(),
                no_data_color: no_data_color.into(),
                default_color: default_color.into(),
            }),
        }
    }
}

impl From<Colorizer> for geoengine_datatypes::operations::image::Colorizer {
    fn from(v: Colorizer) -> Self {
        match v {
            Colorizer::LinearGradient(linear_gradient) => Self::LinearGradient {
                breakpoints: linear_gradient
                    .breakpoints
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<geoengine_datatypes::operations::image::Breakpoint>>(),
                no_data_color: linear_gradient.no_data_color.into(),
                over_color: linear_gradient.over_color.into(),
                under_color: linear_gradient.under_color.into(),
            },
            Colorizer::LogarithmicGradient(logarithmic_gradient) => Self::LogarithmicGradient {
                breakpoints: logarithmic_gradient
                    .breakpoints
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<geoengine_datatypes::operations::image::Breakpoint>>(),
                no_data_color: logarithmic_gradient.no_data_color.into(),
                over_color: logarithmic_gradient.over_color.into(),
                under_color: logarithmic_gradient.under_color.into(),
            },

            Colorizer::Palette(PaletteColorizer {
                colors,
                no_data_color,
                default_color,
                ..
            }) => Self::Palette {
                colors: colors.into(),
                no_data_color: no_data_color.into(),
                default_color: default_color.into(),
            },
        }
    }
}

type_tag!(SingleBand);
type_tag!(MultiBand);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum RasterColorizer {
    SingleBand(SingleBandRasterColorizer),
    MultiBand(MultiBandRasterColorizer),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SingleBandRasterColorizer {
    pub r#type: SingleBandTag,
    pub band: u32,
    pub band_colorizer: Colorizer,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MultiBandRasterColorizer {
    pub r#type: MultiBandTag,

    /// The band index of the red channel.
    pub red_band: u32,
    /// The minimum value for the red channel.
    pub red_min: f64,
    /// The maximum value for the red channel.
    pub red_max: f64,
    /// A scaling factor for the red channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub red_scale: f64,

    /// The band index of the green channel.
    pub green_band: u32,
    /// The minimum value for the red channel.
    pub green_min: f64,
    /// The maximum value for the red channel.
    pub green_max: f64,
    /// A scaling factor for the green channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub green_scale: f64,

    /// The band index of the blue channel.
    pub blue_band: u32,
    /// The minimum value for the red channel.
    pub blue_min: f64,
    /// The maximum value for the red channel.
    pub blue_max: f64,
    /// A scaling factor for the blue channel between 0 and 1.
    #[serde(default = "num_traits::One::one")]
    pub blue_scale: f64,

    /// The color to use for no data values.
    /// If not specified, the no data values will be transparent.
    #[serde(default = "rgba_transparent")]
    pub no_data_color: RgbaColor,
}

fn rgba_transparent() -> RgbaColor {
    RgbaColor([0, 0, 0, 0])
}

impl Eq for RasterColorizer {}

impl RasterColorizer {
    pub fn band_selection(&self) -> BandSelection {
        match self {
            RasterColorizer::SingleBand(SingleBandRasterColorizer { band, .. }) => {
                BandSelection(vec![*band as usize])
            }
            RasterColorizer::MultiBand(MultiBandRasterColorizer {
                red_band,
                green_band,
                blue_band,
                ..
            }) => {
                let mut bands = Vec::with_capacity(3);
                for band in [
                    *red_band as usize,
                    *green_band as usize,
                    *blue_band as usize,
                ] {
                    if !bands.contains(&band) {
                        bands.push(band);
                    }
                }
                bands.sort_unstable(); // bands will be returned in ascending order anyway
                BandSelection(bands)
            }
        }
    }
}

impl From<geoengine_datatypes::operations::image::RasterColorizer> for RasterColorizer {
    fn from(v: geoengine_datatypes::operations::image::RasterColorizer) -> Self {
        match v {
            geoengine_datatypes::operations::image::RasterColorizer::SingleBand {
                band,
                band_colorizer: colorizer,
            } => Self::SingleBand(SingleBandRasterColorizer {
                r#type: Default::default(),
                band,
                band_colorizer: colorizer.into(),
            }),
            geoengine_datatypes::operations::image::RasterColorizer::MultiBand {
                red_band,
                green_band,
                blue_band,
                rgb_params,
            } => Self::MultiBand(MultiBandRasterColorizer {
                r#type: Default::default(),
                red_band,
                green_band,
                blue_band,
                red_min: rgb_params.red_min,
                red_max: rgb_params.red_max,
                red_scale: rgb_params.red_scale,
                green_min: rgb_params.green_min,
                green_max: rgb_params.green_max,
                green_scale: rgb_params.green_scale,
                blue_min: rgb_params.blue_min,
                blue_max: rgb_params.blue_max,
                blue_scale: rgb_params.blue_scale,
                no_data_color: rgb_params.no_data_color.into(),
            }),
        }
    }
}

impl From<RasterColorizer> for geoengine_datatypes::operations::image::RasterColorizer {
    fn from(v: RasterColorizer) -> Self {
        match v {
            RasterColorizer::SingleBand(SingleBandRasterColorizer {
                band,
                band_colorizer: colorizer,
                ..
            }) => Self::SingleBand {
                band,
                band_colorizer: colorizer.into(),
            },
            RasterColorizer::MultiBand(MultiBandRasterColorizer {
                red_band,
                red_min,
                red_max,
                red_scale,
                green_band,
                green_min,
                green_max,
                green_scale,
                blue_band,
                blue_min,
                blue_max,
                blue_scale,
                no_data_color,
                ..
            }) => Self::MultiBand {
                red_band,
                green_band,
                blue_band,
                rgb_params: RgbParams {
                    red_min,
                    red_max,
                    red_scale,
                    green_min,
                    green_max,
                    green_scale,
                    blue_min,
                    blue_max,
                    blue_scale,
                    no_data_color: no_data_color.into(),
                },
            },
        }
    }
}

/// A map from value to color
///
/// It is assumed that is has at least one and at most 256 entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(try_from = "SerializablePalette", into = "SerializablePalette")]
#[schema(value_type = HashMap<f64, RgbaColor>)]
pub struct Palette(pub HashMap<NotNan<f64>, RgbaColor>);

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

impl From<Palette> for geoengine_datatypes::operations::image::Palette {
    fn from(palette: Palette) -> Self {
        Self::new(
            palette
                .0
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

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct DateTimeParseFormat {
    fmt: String,
    has_tz: bool,
    has_time: bool,
}

impl PartialSchema for DateTimeParseFormat {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, SchemaType, Type};
        ObjectBuilder::new()
            .schema_type(SchemaType::Type(Type::String))
            .into()
    }
}

impl ToSchema for DateTimeParseFormat {}

impl<'de> Deserialize<'de> for DateTimeParseFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(geoengine_datatypes::primitives::DateTimeParseFormat::custom(s).into())
    }
}

impl Serialize for DateTimeParseFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.fmt)
    }
}

impl From<geoengine_datatypes::primitives::DateTimeParseFormat> for DateTimeParseFormat {
    fn from(value: geoengine_datatypes::primitives::DateTimeParseFormat) -> Self {
        Self {
            fmt: value.parse_format().to_string(),
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MultiPoint {
    pub coordinates: Vec<Coordinate2D>,
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
        Self::new(value.coordinates.into_iter().map(Into::into).collect())
            .expect("it should always be able to convert it")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MultiLineString {
    pub coordinates: Vec<Vec<Coordinate2D>>,
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
        .expect("it should always be able to convert it")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MultiPolygon {
    pub polygons: Vec<Vec<Vec<Coordinate2D>>>,
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
        .expect("it should always be able to convert it")
    }
}

#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, Clone)]
pub struct StringPair((String, String));

pub type GdalConfigOption = StringPair;
pub type AxisLabels = StringPair;

impl PartialSchema for StringPair {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::schema::{ArrayBuilder, Object, SchemaType, Type};
        ArrayBuilder::new()
            .items(Object::with_type(SchemaType::Type(Type::String)))
            .min_items(Some(2))
            .max_items(Some(2))
            .into()
    }
}

impl ToSchema for StringPair {
    // fn aliases() -> Vec<(&'a str, utoipa::openapi::Schema)> { // TODO: how to do this?
    //     let utoipa::openapi::RefOr::T(unpacked_schema) = Self::schema().1 else {
    //         unreachable!()
    //     };
    //     vec![
    //         ("GdalConfigOption", unpacked_schema.clone()),
    //         ("AxisLabels", unpacked_schema),
    //     ]
    // }
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

impl From<StringPair> for geoengine_datatypes::util::StringPair {
    fn from(value: StringPair) -> Self {
        Self::new(value.0 .0, value.0 .1)
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize, ToSchema)]
pub enum PlotOutputFormat {
    JsonPlain,
    JsonVega,
    ImagePng,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Serialize, PartialOrd, Deserialize, ToSchema)]
pub struct CacheTtlSeconds(u32);

const MAX_CACHE_TTL_SECONDS: u32 = 31_536_000; // 1 year

impl CacheTtlSeconds {
    pub fn new(seconds: u32) -> Self {
        Self(seconds.min(MAX_CACHE_TTL_SECONDS))
    }

    pub fn max() -> Self {
        Self(MAX_CACHE_TTL_SECONDS)
    }

    pub fn seconds(self) -> u32 {
        self.0
    }
}

impl From<geoengine_datatypes::primitives::CacheTtlSeconds> for CacheTtlSeconds {
    fn from(value: geoengine_datatypes::primitives::CacheTtlSeconds) -> Self {
        Self(value.seconds())
    }
}

impl From<CacheTtlSeconds> for geoengine_datatypes::primitives::CacheTtlSeconds {
    fn from(value: CacheTtlSeconds) -> Self {
        Self::new(value.0)
    }
}

impl ToSql for CacheTtlSeconds {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        w: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        <i32 as ToSql>::to_sql(&(self.0 as i32), ty, w)
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <i32 as ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for CacheTtlSeconds {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(<i32 as FromSql>::from_sql(ty, raw)? as u32))
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <i32 as FromSql>::accepts(ty)
    }
}
