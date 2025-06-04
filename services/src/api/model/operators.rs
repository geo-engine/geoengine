use super::datatypes::{
    BoundingBox2D, FeatureDataType, Measurement, RasterDataType, SpatialGridDefinition,
    SpatialReferenceOption, TimeInterval, VectorDataType,
};
use crate::api::model::datatypes::{
    CacheTtlSeconds, Coordinate2D, DateTimeParseFormat, GdalConfigOption, MultiLineString,
    MultiPoint, MultiPolygon, NoGeometry, RasterPropertiesEntryType, RasterPropertiesKey,
    TimeInstance, TimeStep,
};
use crate::error::{
    RasterBandNameMustNotBeEmpty, RasterBandNameTooLong, RasterBandNamesMustBeUnique, Result,
};
use geoengine_datatypes::util::ByteSize;
use geoengine_macros::type_tag;
use geoengine_operators::util::input::float_option_with_nan;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::ensure;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;
use utoipa::{PartialSchema, ToSchema};

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    #[schema(value_type = String)]
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub spatial_grid: SpatialGridDescriptor,
    pub bands: RasterBandDescriptors,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SpatialGridDescriptorState {
    Source,
    Derived,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SpatialGridDescriptor {
    pub spatial_grid: SpatialGridDefinition,
    pub descriptor: SpatialGridDescriptorState,
}

impl From<SpatialGridDescriptor> for geoengine_operators::engine::SpatialGridDescriptor {
    fn from(value: SpatialGridDescriptor) -> Self {
        let sp = geoengine_operators::engine::SpatialGridDescriptor::new_source(
            value.spatial_grid.into(),
        );
        match value.descriptor {
            SpatialGridDescriptorState::Source => sp,
            SpatialGridDescriptorState::Derived => sp.as_derived(),
        }
    }
}

impl From<geoengine_operators::engine::SpatialGridDescriptor> for SpatialGridDescriptor {
    fn from(value: geoengine_operators::engine::SpatialGridDescriptor) -> Self {
        if value.is_source() {
            let sp = value.source_spatial_grid_definition().expect("is source");
            return SpatialGridDescriptor {
                spatial_grid: sp.into(),
                descriptor: SpatialGridDescriptorState::Source,
            };
        }
        let sp = value
            .derived_spatial_grid_definition()
            .expect("if not source it must be derived");
        SpatialGridDescriptor {
            spatial_grid: sp.into(),
            descriptor: SpatialGridDescriptorState::Derived,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
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

impl From<geoengine_operators::engine::RasterBandDescriptors> for RasterBandDescriptors {
    fn from(value: geoengine_operators::engine::RasterBandDescriptors) -> Self {
        Self(value.into_vec().into_iter().map(Into::into).collect())
    }
}

impl From<RasterBandDescriptors> for geoengine_operators::engine::RasterBandDescriptors {
    fn from(value: RasterBandDescriptors) -> Self {
        geoengine_operators::engine::RasterBandDescriptors::new(
            value.0.into_iter().map(Into::into).collect(),
        )
        .expect("RasterBandDescriptors should be valid, because the API descriptors are valid")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RasterBandDescriptor {
    pub name: String,
    pub measurement: Measurement,
}

impl From<geoengine_operators::engine::RasterBandDescriptor> for RasterBandDescriptor {
    fn from(value: geoengine_operators::engine::RasterBandDescriptor) -> Self {
        Self {
            name: value.name,
            measurement: value.measurement.into(),
        }
    }
}

impl From<RasterBandDescriptor> for geoengine_operators::engine::RasterBandDescriptor {
    fn from(value: RasterBandDescriptor) -> Self {
        Self {
            name: value.name,
            measurement: value.measurement.into(),
        }
    }
}

impl From<geoengine_operators::engine::RasterResultDescriptor> for RasterResultDescriptor {
    fn from(value: geoengine_operators::engine::RasterResultDescriptor) -> Self {
        Self {
            data_type: value.data_type.into(),
            spatial_reference: value.spatial_reference.into(),
            time: value.time.map(Into::into),
            spatial_grid: value.spatial_grid.into(),
            bands: value.bands.into(),
        }
    }
}

impl From<RasterResultDescriptor> for geoengine_operators::engine::RasterResultDescriptor {
    fn from(value: RasterResultDescriptor) -> Self {
        Self {
            data_type: value.data_type.into(),
            spatial_reference: value.spatial_reference.into(),
            time: value.time.map(Into::into),
            spatial_grid: value.spatial_grid.into(),
            bands: value.bands.into(),
        }
    }
}

/// An enum to differentiate between `Operator` variants
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "operator")]
pub enum TypedOperator {
    Vector(Box<dyn geoengine_operators::engine::VectorOperator>),
    Raster(Box<dyn geoengine_operators::engine::RasterOperator>),
    Plot(Box<dyn geoengine_operators::engine::PlotOperator>),
}

impl PartialSchema for TypedOperator {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        use utoipa::openapi::schema::{Object, ObjectBuilder, SchemaType, Type};
        ObjectBuilder::new()
            .property(
                "type",
                ObjectBuilder::new()
                    .schema_type(SchemaType::Type(Type::String))
                    .enum_values(Some(vec!["Vector", "Raster", "Plot"]))
            )
            .required("type")
            .property(
                "operator",
                ObjectBuilder::new()
                    .property(
                        "type",
                        Object::with_type(SchemaType::Type(Type::String))
                    )
                    .required("type")
                    .property(
                        "params",
                        Object::with_type(SchemaType::Type(Type::Object))
                    )
                    .property(
                        "sources",
                        Object::with_type(SchemaType::Type(Type::Object))
                    )
            )
            .required("operator")
            .examples(vec![serde_json::json!(
                {"type": "MockPointSource", "params": {"points": [{"x": 0.0, "y": 0.1}, {"x": 1.0, "y": 1.1}]}
            })])
            .description(Some("An enum to differentiate between `Operator` variants"))
            .into()
    }
}

impl ToSchema for TypedOperator {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
    #[schema(value_type = String)]
    pub spatial_reference: SpatialReferenceOption,
    pub columns: HashMap<String, VectorColumnInfo>,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

impl From<geoengine_operators::engine::VectorResultDescriptor> for VectorResultDescriptor {
    fn from(value: geoengine_operators::engine::VectorResultDescriptor) -> Self {
        Self {
            data_type: value.data_type.into(),
            spatial_reference: value.spatial_reference.into(),
            columns: value
                .columns
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
            time: value.time.map(Into::into),
            bbox: value.bbox.map(Into::into),
        }
    }
}

impl From<VectorResultDescriptor> for geoengine_operators::engine::VectorResultDescriptor {
    fn from(value: VectorResultDescriptor) -> Self {
        Self {
            data_type: value.data_type.into(),
            spatial_reference: value.spatial_reference.into(),
            columns: value
                .columns
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
            time: value.time.map(Into::into),
            bbox: value.bbox.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorColumnInfo {
    pub data_type: FeatureDataType,
    pub measurement: Measurement,
}

impl From<geoengine_operators::engine::VectorColumnInfo> for VectorColumnInfo {
    fn from(value: geoengine_operators::engine::VectorColumnInfo) -> Self {
        Self {
            data_type: value.data_type.into(),
            measurement: value.measurement.into(),
        }
    }
}

impl From<VectorColumnInfo> for geoengine_operators::engine::VectorColumnInfo {
    fn from(value: VectorColumnInfo) -> Self {
        Self {
            data_type: value.data_type.into(),
            measurement: value.measurement.into(),
        }
    }
}

/// A `ResultDescriptor` for plot queries
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlotResultDescriptor {
    #[schema(value_type = String)]
    pub spatial_reference: SpatialReferenceOption,
    pub time: Option<TimeInterval>,
    pub bbox: Option<BoundingBox2D>,
}

impl From<geoengine_operators::engine::PlotResultDescriptor> for PlotResultDescriptor {
    fn from(value: geoengine_operators::engine::PlotResultDescriptor) -> Self {
        Self {
            spatial_reference: value.spatial_reference.into(),
            time: value.time.map(Into::into),
            bbox: value.bbox.map(Into::into),
        }
    }
}

impl From<PlotResultDescriptor> for geoengine_operators::engine::PlotResultDescriptor {
    fn from(value: PlotResultDescriptor) -> Self {
        Self {
            spatial_reference: value.spatial_reference.into(),
            time: value.time.map(Into::into),
            bbox: value.bbox.map(Into::into),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum TypedResultDescriptor {
    Plot(TypedPlotResultDescriptor),
    Raster(TypedRasterResultDescriptor),
    Vector(TypedVectorResultDescriptor),
}

#[type_tag(value = "plot")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TypedPlotResultDescriptor {
    #[serde(flatten)]
    pub result_descriptor: PlotResultDescriptor,
}

#[type_tag(value = "raster")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TypedRasterResultDescriptor {
    #[serde(flatten)]
    pub result_descriptor: RasterResultDescriptor,
}

#[type_tag(value = "vector")]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TypedVectorResultDescriptor {
    #[serde(flatten)]
    pub result_descriptor: VectorResultDescriptor,
}

impl From<geoengine_operators::engine::TypedResultDescriptor> for TypedResultDescriptor {
    fn from(value: geoengine_operators::engine::TypedResultDescriptor) -> Self {
        match value {
            geoengine_operators::engine::TypedResultDescriptor::Plot(p) => {
                Self::Plot(TypedPlotResultDescriptor {
                    r#type: Default::default(),
                    result_descriptor: p.into(),
                })
            }
            geoengine_operators::engine::TypedResultDescriptor::Raster(r) => {
                Self::Raster(TypedRasterResultDescriptor {
                    r#type: Default::default(),
                    result_descriptor: r.into(),
                })
            }
            geoengine_operators::engine::TypedResultDescriptor::Vector(v) => {
                Self::Vector(TypedVectorResultDescriptor {
                    r#type: Default::default(),
                    result_descriptor: v.into(),
                })
            }
        }
    }
}

impl From<TypedResultDescriptor> for geoengine_operators::engine::TypedResultDescriptor {
    fn from(value: TypedResultDescriptor) -> Self {
        match value {
            TypedResultDescriptor::Plot(p) => {
                geoengine_operators::engine::TypedResultDescriptor::Plot(p.result_descriptor.into())
            }
            TypedResultDescriptor::Raster(r) => {
                geoengine_operators::engine::TypedResultDescriptor::Raster(
                    r.result_descriptor.into(),
                )
            }
            TypedResultDescriptor::Vector(v) => {
                geoengine_operators::engine::TypedResultDescriptor::Vector(
                    v.result_descriptor.into(),
                )
            }
        }
    }
}

impl From<geoengine_operators::engine::PlotResultDescriptor> for TypedResultDescriptor {
    fn from(value: geoengine_operators::engine::PlotResultDescriptor) -> Self {
        Self::Plot(TypedPlotResultDescriptor {
            r#type: Default::default(),
            result_descriptor: value.into(),
        })
    }
}

impl From<PlotResultDescriptor> for TypedResultDescriptor {
    fn from(value: PlotResultDescriptor) -> Self {
        Self::Plot(TypedPlotResultDescriptor {
            r#type: Default::default(),
            result_descriptor: value,
        })
    }
}

impl From<geoengine_operators::engine::RasterResultDescriptor> for TypedResultDescriptor {
    fn from(value: geoengine_operators::engine::RasterResultDescriptor) -> Self {
        Self::Raster(TypedRasterResultDescriptor {
            r#type: Default::default(),
            result_descriptor: value.into(),
        })
    }
}

impl From<RasterResultDescriptor> for TypedResultDescriptor {
    fn from(value: RasterResultDescriptor) -> Self {
        Self::Raster(TypedRasterResultDescriptor {
            r#type: Default::default(),
            result_descriptor: value,
        })
    }
}

impl From<geoengine_operators::engine::VectorResultDescriptor> for TypedResultDescriptor {
    fn from(value: geoengine_operators::engine::VectorResultDescriptor) -> Self {
        Self::Vector(TypedVectorResultDescriptor {
            r#type: Default::default(),
            result_descriptor: value.into(),
        })
    }
}

impl From<VectorResultDescriptor> for TypedResultDescriptor {
    fn from(value: VectorResultDescriptor) -> Self {
        Self::Vector(TypedVectorResultDescriptor {
            r#type: Default::default(),
            result_descriptor: value,
        })
    }
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MockDatasetDataSourceLoadingInfo {
    pub points: Vec<Coordinate2D>,
}

impl From<geoengine_operators::mock::MockDatasetDataSourceLoadingInfo>
    for MockDatasetDataSourceLoadingInfo
{
    fn from(value: geoengine_operators::mock::MockDatasetDataSourceLoadingInfo) -> Self {
        Self {
            points: value.points.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<MockDatasetDataSourceLoadingInfo>
    for geoengine_operators::mock::MockDatasetDataSourceLoadingInfo
{
    fn from(value: MockDatasetDataSourceLoadingInfo) -> Self {
        Self {
            points: value.points.into_iter().map(Into::into).collect(),
        }
    }
}

#[type_tag(value = "MockMetaData")]
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MockMetaData {
    pub loading_info: MockDatasetDataSourceLoadingInfo,
    pub result_descriptor: VectorResultDescriptor,
}
#[type_tag(value = "OgrMetaData")]
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrMetaData {
    pub loading_info: OgrSourceDataset,
    pub result_descriptor: VectorResultDescriptor,
}

impl
    From<
        geoengine_operators::engine::StaticMetaData<
            geoengine_operators::mock::MockDatasetDataSourceLoadingInfo,
            geoengine_operators::engine::VectorResultDescriptor,
            geoengine_datatypes::primitives::VectorQueryRectangle,
        >,
    > for MockMetaData
{
    fn from(
        value: geoengine_operators::engine::StaticMetaData<
            geoengine_operators::mock::MockDatasetDataSourceLoadingInfo,
            geoengine_operators::engine::VectorResultDescriptor,
            geoengine_datatypes::primitives::VectorQueryRectangle,
        >,
    ) -> Self {
        Self {
            r#type: Default::default(),
            loading_info: value.loading_info.into(),
            result_descriptor: value.result_descriptor.into(),
        }
    }
}

impl
    From<
        geoengine_operators::engine::StaticMetaData<
            geoengine_operators::source::OgrSourceDataset,
            geoengine_operators::engine::VectorResultDescriptor,
            geoengine_datatypes::primitives::VectorQueryRectangle,
        >,
    > for OgrMetaData
{
    fn from(
        value: geoengine_operators::engine::StaticMetaData<
            geoengine_operators::source::OgrSourceDataset,
            geoengine_operators::engine::VectorResultDescriptor,
            geoengine_datatypes::primitives::VectorQueryRectangle,
        >,
    ) -> Self {
        Self {
            r#type: Default::default(),
            loading_info: value.loading_info.into(),
            result_descriptor: value.result_descriptor.into(),
        }
    }
}

impl From<MockMetaData>
    for geoengine_operators::engine::StaticMetaData<
        geoengine_operators::mock::MockDatasetDataSourceLoadingInfo,
        geoengine_operators::engine::VectorResultDescriptor,
        geoengine_datatypes::primitives::VectorQueryRectangle,
    >
{
    fn from(value: MockMetaData) -> Self {
        Self {
            loading_info: value.loading_info.into(),
            result_descriptor: value.result_descriptor.into(),
            phantom: Default::default(),
        }
    }
}

impl From<OgrMetaData>
    for geoengine_operators::engine::StaticMetaData<
        geoengine_operators::source::OgrSourceDataset,
        geoengine_operators::engine::VectorResultDescriptor,
        geoengine_datatypes::primitives::VectorQueryRectangle,
    >
{
    fn from(value: OgrMetaData) -> Self {
        Self {
            loading_info: value.loading_info.into(),
            result_descriptor: value.result_descriptor.into(),
            phantom: Default::default(),
        }
    }
}

#[type_tag(value = "GdalStatic")]
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataStatic {
    pub time: Option<TimeInterval>,
    pub params: GdalDatasetParameters,
    pub result_descriptor: RasterResultDescriptor,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<geoengine_operators::source::GdalMetaDataStatic> for GdalMetaDataStatic {
    fn from(value: geoengine_operators::source::GdalMetaDataStatic) -> Self {
        Self {
            r#type: Default::default(),
            time: value.time.map(Into::into),
            params: value.params.into(),
            result_descriptor: value.result_descriptor.into(),
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<GdalMetaDataStatic> for geoengine_operators::source::GdalMetaDataStatic {
    fn from(value: GdalMetaDataStatic) -> Self {
        Self {
            time: value.time.map(Into::into),
            params: value.params.into(),
            result_descriptor: value.result_descriptor.into(),
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceDataset {
    #[schema(value_type = String)]
    pub file_name: PathBuf,
    pub layer_name: String,
    pub data_type: Option<VectorDataType>,
    #[serde(default)]
    pub time: OgrSourceDatasetTimeType,
    pub default_geometry: Option<TypedGeometry>,
    pub columns: Option<OgrSourceColumnSpec>,
    #[serde(default)]
    pub force_ogr_time_filter: bool,
    #[serde(default)]
    pub force_ogr_spatial_filter: bool,
    pub on_error: OgrSourceErrorSpec,
    pub sql_query: Option<String>,
    pub attribute_query: Option<String>,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<geoengine_operators::source::OgrSourceDataset> for OgrSourceDataset {
    fn from(value: geoengine_operators::source::OgrSourceDataset) -> Self {
        Self {
            file_name: value.file_name,
            layer_name: value.layer_name,
            data_type: value.data_type.map(Into::into),
            time: value.time.into(),
            default_geometry: value.default_geometry.map(Into::into),
            columns: value.columns.map(Into::into),
            force_ogr_time_filter: value.force_ogr_time_filter,
            force_ogr_spatial_filter: value.force_ogr_spatial_filter,
            on_error: value.on_error.into(),
            sql_query: value.sql_query,
            attribute_query: value.attribute_query,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<OgrSourceDataset> for geoengine_operators::source::OgrSourceDataset {
    fn from(value: OgrSourceDataset) -> Self {
        Self {
            file_name: value.file_name,
            layer_name: value.layer_name,
            data_type: value.data_type.map(Into::into),
            time: value.time.into(),
            default_geometry: value.default_geometry.map(Into::into),
            columns: value.columns.map(Into::into),
            force_ogr_time_filter: value.force_ogr_time_filter,
            force_ogr_spatial_filter: value.force_ogr_spatial_filter,
            on_error: value.on_error.into(),
            sql_query: value.sql_query,
            attribute_query: value.attribute_query,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "format")]
pub enum OgrSourceTimeFormat {
    Custom(OgrSourceTimeFormatCustom),
    UnixTimeStamp(OgrSourceTimeFormatUnixTimeStamp),
    Auto(OgrSourceTimeFormatAuto),
}

#[type_tag(tag = "format", value = "custom")]
#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceTimeFormatCustom {
    custom_format: DateTimeParseFormat,
}

#[type_tag(tag = "format", value = "unixTimeStamp")]
#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceTimeFormatUnixTimeStamp {
    timestamp_type: UnixTimeStampType,
    #[serde(skip)]
    #[serde(default = "DateTimeParseFormat::unix")]
    fmt: DateTimeParseFormat,
}

#[type_tag(tag = "format", value = "auto")]
#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Debug, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceTimeFormatAuto {}

impl From<geoengine_operators::source::OgrSourceTimeFormat> for OgrSourceTimeFormat {
    fn from(value: geoengine_operators::source::OgrSourceTimeFormat) -> Self {
        match value {
            geoengine_operators::source::OgrSourceTimeFormat::Custom { custom_format } => {
                Self::Custom(OgrSourceTimeFormatCustom {
                    format: Default::default(),
                    custom_format: custom_format.into(),
                })
            }
            geoengine_operators::source::OgrSourceTimeFormat::UnixTimeStamp {
                timestamp_type,
                fmt,
            } => Self::UnixTimeStamp(OgrSourceTimeFormatUnixTimeStamp {
                format: Default::default(),
                timestamp_type: timestamp_type.into(),
                fmt: fmt.into(),
            }),
            geoengine_operators::source::OgrSourceTimeFormat::Auto => {
                Self::Auto(Default::default())
            }
        }
    }
}

impl From<OgrSourceTimeFormat> for geoengine_operators::source::OgrSourceTimeFormat {
    fn from(value: OgrSourceTimeFormat) -> Self {
        match value {
            OgrSourceTimeFormat::Custom(OgrSourceTimeFormatCustom { custom_format, .. }) => {
                Self::Custom {
                    custom_format: custom_format.into(),
                }
            }
            OgrSourceTimeFormat::UnixTimeStamp(OgrSourceTimeFormatUnixTimeStamp {
                timestamp_type,
                fmt,
                ..
            }) => Self::UnixTimeStamp {
                timestamp_type: timestamp_type.into(),
                fmt: fmt.into(),
            },
            OgrSourceTimeFormat::Auto(_) => Self::Auto,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum UnixTimeStampType {
    EpochSeconds,
    EpochMilliseconds,
}

impl From<geoengine_operators::source::UnixTimeStampType> for UnixTimeStampType {
    fn from(value: geoengine_operators::source::UnixTimeStampType) -> Self {
        match value {
            geoengine_operators::source::UnixTimeStampType::EpochSeconds => Self::EpochSeconds,
            geoengine_operators::source::UnixTimeStampType::EpochMilliseconds => {
                Self::EpochMilliseconds
            }
        }
    }
}

impl From<UnixTimeStampType> for geoengine_operators::source::UnixTimeStampType {
    fn from(value: UnixTimeStampType) -> Self {
        match value {
            UnixTimeStampType::EpochSeconds => Self::EpochSeconds,
            UnixTimeStampType::EpochMilliseconds => Self::EpochMilliseconds,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum OgrSourceErrorSpec {
    Ignore,
    Abort,
}

impl From<geoengine_operators::source::OgrSourceErrorSpec> for OgrSourceErrorSpec {
    fn from(value: geoengine_operators::source::OgrSourceErrorSpec) -> Self {
        match value {
            geoengine_operators::source::OgrSourceErrorSpec::Ignore => Self::Ignore,
            geoengine_operators::source::OgrSourceErrorSpec::Abort => Self::Abort,
        }
    }
}

impl From<OgrSourceErrorSpec> for geoengine_operators::source::OgrSourceErrorSpec {
    fn from(value: OgrSourceErrorSpec) -> Self {
        match value {
            OgrSourceErrorSpec::Ignore => Self::Ignore,
            OgrSourceErrorSpec::Abort => Self::Abort,
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum OgrSourceDatasetTimeType {
    None(OgrSourceDatasetTimeTypeNone),
    Start(OgrSourceDatasetTimeTypeStart),
    StartEnd(OgrSourceDatasetTimeTypeStartEnd),
    StartDuration(OgrSourceDatasetTimeTypeStartDuration),
}

#[type_tag(value = "none")]
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceDatasetTimeTypeNone {}

#[type_tag(value = "start")]
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceDatasetTimeTypeStart {
    pub start_field: String,
    pub start_format: OgrSourceTimeFormat,
    pub duration: OgrSourceDurationSpec,
}

#[type_tag(value = "start+end")]
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceDatasetTimeTypeStartEnd {
    pub start_field: String,
    pub start_format: OgrSourceTimeFormat,
    pub end_field: String,
    pub end_format: OgrSourceTimeFormat,
}

#[type_tag(value = "start+duration")]
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceDatasetTimeTypeStartDuration {
    pub start_field: String,
    pub start_format: OgrSourceTimeFormat,
    pub duration_field: String,
}

impl From<geoengine_operators::source::OgrSourceDatasetTimeType> for OgrSourceDatasetTimeType {
    fn from(value: geoengine_operators::source::OgrSourceDatasetTimeType) -> Self {
        match value {
            geoengine_operators::source::OgrSourceDatasetTimeType::None => {
                Self::None(OgrSourceDatasetTimeTypeNone {
                    r#type: Default::default(),
                })
            }
            geoengine_operators::source::OgrSourceDatasetTimeType::Start {
                start_field,
                start_format,
                duration,
            } => Self::Start(OgrSourceDatasetTimeTypeStart {
                r#type: Default::default(),
                start_field,
                start_format: start_format.into(),
                duration: duration.into(),
            }),
            geoengine_operators::source::OgrSourceDatasetTimeType::StartEnd {
                start_field,
                start_format,
                end_field,
                end_format,
            } => Self::StartEnd(OgrSourceDatasetTimeTypeStartEnd {
                r#type: Default::default(),
                start_field,
                start_format: start_format.into(),
                end_field,
                end_format: end_format.into(),
            }),
            geoengine_operators::source::OgrSourceDatasetTimeType::StartDuration {
                start_field,
                start_format,
                duration_field,
            } => Self::StartDuration(OgrSourceDatasetTimeTypeStartDuration {
                r#type: Default::default(),
                start_field,
                start_format: start_format.into(),
                duration_field,
            }),
        }
    }
}

impl From<OgrSourceDatasetTimeType> for geoengine_operators::source::OgrSourceDatasetTimeType {
    fn from(value: OgrSourceDatasetTimeType) -> Self {
        match value {
            OgrSourceDatasetTimeType::None(..) => Self::None,
            OgrSourceDatasetTimeType::Start(OgrSourceDatasetTimeTypeStart {
                start_field,
                start_format,
                duration,
                ..
            }) => Self::Start {
                start_field,
                start_format: start_format.into(),
                duration: duration.into(),
            },
            OgrSourceDatasetTimeType::StartEnd(OgrSourceDatasetTimeTypeStartEnd {
                start_field,
                start_format,
                end_field,
                end_format,
                ..
            }) => Self::StartEnd {
                start_field,
                start_format: start_format.into(),
                end_field,
                end_format: end_format.into(),
            },
            OgrSourceDatasetTimeType::StartDuration(OgrSourceDatasetTimeTypeStartDuration {
                start_field,
                start_format,
                duration_field,
                ..
            }) => Self::StartDuration {
                start_field,
                start_format: start_format.into(),
                duration_field,
            },
        }
    }
}

/// If no time is specified, expect to parse none
impl Default for OgrSourceDatasetTimeType {
    fn default() -> Self {
        Self::None(OgrSourceDatasetTimeTypeNone {
            r#type: Default::default(),
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", untagged)]
#[schema(discriminator = "type")]
pub enum OgrSourceDurationSpec {
    Infinite(OgrSourceDurationSpecInfinite),
    Zero(OgrSourceDurationSpecZero),
    Value(OgrSourceDurationSpecValue),
}

#[type_tag(value = "infinite")]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema, Default)]
pub struct OgrSourceDurationSpecInfinite {}

#[type_tag(value = "zero")]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema, Default)]
pub struct OgrSourceDurationSpecZero {}

#[type_tag(value = "value")]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
pub struct OgrSourceDurationSpecValue {
    #[serde(flatten)]
    pub time_step: TimeStep,
}

impl From<geoengine_operators::source::OgrSourceDurationSpec> for OgrSourceDurationSpec {
    fn from(value: geoengine_operators::source::OgrSourceDurationSpec) -> Self {
        match value {
            geoengine_operators::source::OgrSourceDurationSpec::Infinite => {
                Self::Infinite(Default::default())
            }
            geoengine_operators::source::OgrSourceDurationSpec::Zero => {
                Self::Zero(Default::default())
            }
            geoengine_operators::source::OgrSourceDurationSpec::Value(v) => {
                Self::Value(OgrSourceDurationSpecValue {
                    r#type: Default::default(),
                    time_step: v.into(),
                })
            }
        }
    }
}

impl From<OgrSourceDurationSpec> for geoengine_operators::source::OgrSourceDurationSpec {
    fn from(value: OgrSourceDurationSpec) -> Self {
        match value {
            OgrSourceDurationSpec::Infinite(..) => Self::Infinite,
            OgrSourceDurationSpec::Zero(..) => Self::Zero,
            OgrSourceDurationSpec::Value(v) => {
                Self::Value(geoengine_datatypes::primitives::TimeStep {
                    ..v.time_step.into()
                })
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub enum TypedGeometry {
    Data(NoGeometry),
    MultiPoint(MultiPoint),
    MultiLineString(MultiLineString),
    MultiPolygon(MultiPolygon),
}

impl From<geoengine_datatypes::primitives::TypedGeometry> for TypedGeometry {
    fn from(value: geoengine_datatypes::primitives::TypedGeometry) -> Self {
        match value {
            geoengine_datatypes::primitives::TypedGeometry::Data(x) => Self::Data(x.into()),
            geoengine_datatypes::primitives::TypedGeometry::MultiPoint(x) => {
                Self::MultiPoint(x.into())
            }
            geoengine_datatypes::primitives::TypedGeometry::MultiLineString(x) => {
                Self::MultiLineString(x.into())
            }
            geoengine_datatypes::primitives::TypedGeometry::MultiPolygon(x) => {
                Self::MultiPolygon(x.into())
            }
        }
    }
}

impl From<TypedGeometry> for geoengine_datatypes::primitives::TypedGeometry {
    fn from(value: TypedGeometry) -> Self {
        match value {
            TypedGeometry::Data(x) => Self::Data(x.into()),
            TypedGeometry::MultiPoint(x) => Self::MultiPoint(x.into()),
            TypedGeometry::MultiLineString(x) => Self::MultiLineString(x.into()),
            TypedGeometry::MultiPolygon(x) => Self::MultiPolygon(x.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OgrSourceColumnSpec {
    pub format_specifics: Option<FormatSpecifics>,
    pub x: String,
    pub y: Option<String>,
    #[serde(default)]
    pub int: Vec<String>,
    #[serde(default)]
    pub float: Vec<String>,
    #[serde(default)]
    pub text: Vec<String>,
    #[serde(default)]
    pub bool: Vec<String>,
    #[serde(default)]
    pub datetime: Vec<String>,
    pub rename: Option<HashMap<String, String>>,
}

impl From<geoengine_operators::source::OgrSourceColumnSpec> for OgrSourceColumnSpec {
    fn from(value: geoengine_operators::source::OgrSourceColumnSpec) -> Self {
        Self {
            format_specifics: value.format_specifics.map(Into::into),
            x: value.x,
            y: value.y,
            int: value.int,
            float: value.float,
            text: value.text,
            bool: value.bool,
            datetime: value.datetime,
            rename: value.rename,
        }
    }
}

impl From<OgrSourceColumnSpec> for geoengine_operators::source::OgrSourceColumnSpec {
    fn from(value: OgrSourceColumnSpec) -> Self {
        Self {
            format_specifics: value.format_specifics.map(Into::into),
            x: value.x,
            y: value.y,
            int: value.int,
            float: value.float,
            text: value.text,
            bool: value.bool,
            datetime: value.datetime,
            rename: value.rename,
        }
    }
}

#[type_tag(value = "GdalMetaDataRegular")]
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataRegular {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
    pub data_time: TimeInterval,
    pub step: TimeStep,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<geoengine_operators::source::GdalMetaDataRegular> for GdalMetaDataRegular {
    fn from(value: geoengine_operators::source::GdalMetaDataRegular) -> Self {
        Self {
            r#type: Default::default(),
            result_descriptor: value.result_descriptor.into(),
            params: value.params.into(),
            time_placeholders: value
                .time_placeholders
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            data_time: value.data_time.into(),
            step: value.step.into(),
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<GdalMetaDataRegular> for geoengine_operators::source::GdalMetaDataRegular {
    fn from(value: GdalMetaDataRegular) -> Self {
        Self {
            result_descriptor: value.result_descriptor.into(),
            params: value.params.into(),
            time_placeholders: value
                .time_placeholders
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            data_time: value.data_time.into(),
            step: value.step.into(),
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

/// Parameters for loading data using Gdal
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetParameters {
    #[schema(value_type = String)]
    pub file_path: PathBuf,
    pub rasterband_channel: usize,
    pub geo_transform: GdalDatasetGeoTransform, // TODO: discuss if we need this at all
    pub width: usize,
    pub height: usize,
    pub file_not_found_handling: FileNotFoundHandling,
    #[serde(with = "float_option_with_nan")]
    #[serde(default)]
    pub no_data_value: Option<f64>,
    pub properties_mapping: Option<Vec<GdalMetadataMapping>>,
    // Dataset open option as strings, e.g. `vec!["UserPwd=geoengine:pwd".to_owned(), "HttpAuth=BASIC".to_owned()]`
    pub gdal_open_options: Option<Vec<String>>,
    // Configs as key, value pairs that will be set as thread local config options, e.g.
    // `vec!["AWS_REGION".to_owned(), "eu-central-1".to_owned()]` and unset afterwards
    // TODO: validate the config options: only allow specific keys and specific values
    pub gdal_config_options: Option<Vec<GdalConfigOption>>,
    #[serde(default)]
    pub allow_alphaband_as_mask: bool,
}

impl From<geoengine_operators::source::GdalDatasetParameters> for GdalDatasetParameters {
    fn from(value: geoengine_operators::source::GdalDatasetParameters) -> Self {
        Self {
            file_path: value.file_path,
            rasterband_channel: value.rasterband_channel,
            geo_transform: value.geo_transform.into(),
            width: value.width,
            height: value.height,
            file_not_found_handling: value.file_not_found_handling.into(),
            no_data_value: value.no_data_value,
            properties_mapping: value
                .properties_mapping
                .map(|x| x.into_iter().map(Into::into).collect()),
            gdal_open_options: value.gdal_open_options,
            gdal_config_options: value
                .gdal_config_options
                .map(|x| x.into_iter().map(Into::into).collect()),
            allow_alphaband_as_mask: value.allow_alphaband_as_mask,
        }
    }
}

impl From<GdalDatasetParameters> for geoengine_operators::source::GdalDatasetParameters {
    fn from(value: GdalDatasetParameters) -> Self {
        Self {
            file_path: value.file_path,
            rasterband_channel: value.rasterband_channel,
            geo_transform: value.geo_transform.into(),
            width: value.width,
            height: value.height,
            file_not_found_handling: value.file_not_found_handling.into(),
            no_data_value: value.no_data_value,
            properties_mapping: value
                .properties_mapping
                .map(|x| x.into_iter().map(Into::into).collect()),
            gdal_open_options: value.gdal_open_options,
            gdal_config_options: value
                .gdal_config_options
                .map(|x| x.into_iter().map(Into::into).collect()),
            allow_alphaband_as_mask: value.allow_alphaband_as_mask,
            retry: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, ToSchema)]
pub struct GdalSourceTimePlaceholder {
    pub format: DateTimeParseFormat,
    pub reference: TimeReference,
}

impl From<geoengine_operators::source::GdalSourceTimePlaceholder> for GdalSourceTimePlaceholder {
    fn from(value: geoengine_operators::source::GdalSourceTimePlaceholder) -> Self {
        Self {
            format: value.format.into(),
            reference: value.reference.into(),
        }
    }
}

impl From<GdalSourceTimePlaceholder> for geoengine_operators::source::GdalSourceTimePlaceholder {
    fn from(value: GdalSourceTimePlaceholder) -> Self {
        Self {
            format: value.format.into(),
            reference: value.reference.into(),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalDatasetGeoTransform {
    pub origin_coordinate: Coordinate2D,
    pub x_pixel_size: f64,
    pub y_pixel_size: f64,
}

impl From<geoengine_operators::source::GdalDatasetGeoTransform> for GdalDatasetGeoTransform {
    fn from(value: geoengine_operators::source::GdalDatasetGeoTransform) -> Self {
        Self {
            origin_coordinate: value.origin_coordinate.into(),
            x_pixel_size: value.x_pixel_size,
            y_pixel_size: value.y_pixel_size,
        }
    }
}

impl From<GdalDatasetGeoTransform> for geoengine_operators::source::GdalDatasetGeoTransform {
    fn from(value: GdalDatasetGeoTransform) -> Self {
        Self {
            origin_coordinate: value.origin_coordinate.into(),
            x_pixel_size: value.x_pixel_size,
            y_pixel_size: value.y_pixel_size,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, ToSchema)]
pub enum FileNotFoundHandling {
    NoData, // output tiles filled with nodata
    Error,  // return error tile
}

impl From<geoengine_operators::source::FileNotFoundHandling> for FileNotFoundHandling {
    fn from(value: geoengine_operators::source::FileNotFoundHandling) -> Self {
        match value {
            geoengine_operators::source::FileNotFoundHandling::NoData => Self::NoData,
            geoengine_operators::source::FileNotFoundHandling::Error => Self::Error,
        }
    }
}

impl From<FileNotFoundHandling> for geoengine_operators::source::FileNotFoundHandling {
    fn from(value: FileNotFoundHandling) -> Self {
        match value {
            FileNotFoundHandling::NoData => Self::NoData,
            FileNotFoundHandling::Error => Self::Error,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct GdalMetadataMapping {
    pub source_key: RasterPropertiesKey,
    pub target_key: RasterPropertiesKey,
    pub target_type: RasterPropertiesEntryType,
}

impl From<geoengine_operators::source::GdalMetadataMapping> for GdalMetadataMapping {
    fn from(value: geoengine_operators::source::GdalMetadataMapping) -> Self {
        Self {
            source_key: value.source_key.into(),
            target_key: value.target_key.into(),
            target_type: value.target_type.into(),
        }
    }
}

impl From<GdalMetadataMapping> for geoengine_operators::source::GdalMetadataMapping {
    fn from(value: GdalMetadataMapping) -> Self {
        Self {
            source_key: value.source_key.into(),
            target_key: value.target_key.into(),
            target_type: value.target_type.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum TimeReference {
    Start,
    End,
}

impl From<geoengine_operators::source::TimeReference> for TimeReference {
    fn from(value: geoengine_operators::source::TimeReference) -> Self {
        match value {
            geoengine_operators::source::TimeReference::Start => Self::Start,
            geoengine_operators::source::TimeReference::End => Self::End,
        }
    }
}

impl From<TimeReference> for geoengine_operators::source::TimeReference {
    fn from(value: TimeReference) -> Self {
        match value {
            TimeReference::Start => Self::Start,
            TimeReference::End => Self::End,
        }
    }
}

/// Meta data for 4D `NetCDF` CF datasets
#[type_tag(value = "GdalMetaDataNetCdfCf")]
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetadataNetCdfCf {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub start: TimeInstance,
    /// We use the end to specify the last, non-inclusive valid time point.
    /// Queries behind this point return no data.
    /// TODO: Alternatively, we could think about using the number of possible time steps in the future.
    pub end: TimeInstance,
    pub step: TimeStep,
    /// A band offset specifies the first band index to use for the first point in time.
    /// All other time steps are added to this offset.
    pub band_offset: usize,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<geoengine_operators::source::GdalMetadataNetCdfCf> for GdalMetadataNetCdfCf {
    fn from(value: geoengine_operators::source::GdalMetadataNetCdfCf) -> Self {
        Self {
            r#type: Default::default(),
            result_descriptor: value.result_descriptor.into(),
            params: value.params.into(),
            start: value.start.into(),
            end: value.end.into(),
            step: value.step.into(),
            band_offset: value.band_offset,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<GdalMetadataNetCdfCf> for geoengine_operators::source::GdalMetadataNetCdfCf {
    fn from(value: GdalMetadataNetCdfCf) -> Self {
        Self {
            result_descriptor: value.result_descriptor.into(),
            params: value.params.into(),
            start: value.start.into(),
            end: value.end.into(),
            step: value.step.into(),
            band_offset: value.band_offset,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[type_tag(value = "GdalMetaDataList")]
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataList {
    pub result_descriptor: RasterResultDescriptor,
    pub params: Vec<GdalLoadingInfoTemporalSlice>,
}

impl From<geoengine_operators::source::GdalMetaDataList> for GdalMetaDataList {
    fn from(value: geoengine_operators::source::GdalMetaDataList) -> Self {
        Self {
            r#type: Default::default(),
            result_descriptor: value.result_descriptor.into(),
            params: value.params.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<GdalMetaDataList> for geoengine_operators::source::GdalMetaDataList {
    fn from(value: GdalMetaDataList) -> Self {
        Self {
            result_descriptor: value.result_descriptor.into(),
            params: value.params.into_iter().map(Into::into).collect(),
        }
    }
}

/// one temporal slice of the dataset that requires reading from exactly one Gdal dataset
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalLoadingInfoTemporalSlice {
    pub time: TimeInterval,
    pub params: Option<GdalDatasetParameters>,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<geoengine_operators::source::GdalLoadingInfoTemporalSlice>
    for GdalLoadingInfoTemporalSlice
{
    fn from(value: geoengine_operators::source::GdalLoadingInfoTemporalSlice) -> Self {
        Self {
            time: value.time.into(),
            params: value.params.map(Into::into),
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<GdalLoadingInfoTemporalSlice>
    for geoengine_operators::source::GdalLoadingInfoTemporalSlice
{
    fn from(value: GdalLoadingInfoTemporalSlice) -> Self {
        Self {
            time: value.time.into(),
            params: value.params.map(Into::into),
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[type_tag(value = "GdalMultiBand")]
#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMultiBand {
    pub result_descriptor: RasterResultDescriptor, // TODO: omit, already part of dataset
}

impl From<geoengine_operators::source::GdalMultiBand> for GdalMultiBand {
    fn from(value: geoengine_operators::source::GdalMultiBand) -> Self {
        Self {
            r#type: Default::default(),
            result_descriptor: value.result_descriptor.into(),
        }
    }
}

impl From<GdalMultiBand> for geoengine_operators::source::GdalMultiBand {
    fn from(value: GdalMultiBand) -> Self {
        Self {
            result_descriptor: value.result_descriptor.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum CsvHeader {
    Yes,
    No,
    Auto,
}

impl From<geoengine_operators::source::CsvHeader> for CsvHeader {
    fn from(value: geoengine_operators::source::CsvHeader) -> Self {
        match value {
            geoengine_operators::source::CsvHeader::Yes => Self::Yes,
            geoengine_operators::source::CsvHeader::No => Self::No,
            geoengine_operators::source::CsvHeader::Auto => Self::Auto,
        }
    }
}

impl From<CsvHeader> for geoengine_operators::source::CsvHeader {
    fn from(value: CsvHeader) -> Self {
        match value {
            CsvHeader::Yes => Self::Yes,
            CsvHeader::No => Self::No,
            CsvHeader::Auto => Self::Auto,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum FormatSpecifics {
    Csv { header: CsvHeader },
}

impl From<geoengine_operators::source::FormatSpecifics> for FormatSpecifics {
    fn from(value: geoengine_operators::source::FormatSpecifics) -> Self {
        match value {
            geoengine_operators::source::FormatSpecifics::Csv { header } => Self::Csv {
                header: header.into(),
            },
        }
    }
}

impl From<FormatSpecifics> for geoengine_operators::source::FormatSpecifics {
    fn from(value: FormatSpecifics) -> Self {
        match value {
            FormatSpecifics::Csv { header } => Self::Csv {
                header: header.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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
                RasterBandDescriptor {
                    name: "foo".into(),
                    measurement: Measurement::Unitless(Default::default()),
                },
                RasterBandDescriptor {
                    name: "bar".into(),
                    measurement: Measurement::Unitless(Default::default()),
                },
            ])
            .unwrap()
        );

        assert!(
            serde_json::from_value::<RasterBandDescriptors>(json!([{
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
            .is_err()
        );
    }
}
