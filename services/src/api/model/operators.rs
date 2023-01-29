use crate::api::model::datatypes::{
    Coordinate2D, DateTimeParseFormat, MultiLineString, MultiPoint, MultiPolygon, NoGeometry,
    QueryRectangle, RasterPropertiesEntryType, RasterPropertiesKey, SpatialResolution, StringPair,
    TimeInstance, TimeStep, VectorQueryRectangle,
};
use async_trait::async_trait;
use geoengine_operators::engine::{MetaData, ResultDescriptor};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::PathBuf;
use utoipa::ToSchema;

use super::datatypes::{
    BoundingBox2D, FeatureDataType, Measurement, RasterDataType, SpatialPartition2D,
    SpatialReferenceOption, TimeInterval, VectorDataType,
};

/// A `ResultDescriptor` for raster queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RasterResultDescriptor {
    pub data_type: RasterDataType,
    pub spatial_reference: SpatialReferenceOption,
    pub measurement: Measurement,
    pub time: Option<TimeInterval>,
    pub bbox: Option<SpatialPartition2D>,
    pub resolution: Option<SpatialResolution>,
}

impl From<geoengine_operators::engine::RasterResultDescriptor> for RasterResultDescriptor {
    fn from(value: geoengine_operators::engine::RasterResultDescriptor) -> Self {
        Self {
            data_type: value.data_type.into(),
            spatial_reference: value.spatial_reference.into(),
            measurement: value.measurement.into(),
            time: value.time.map(Into::into),
            bbox: value.bbox.map(Into::into),
            resolution: value.resolution.map(Into::into),
        }
    }
}

impl From<RasterResultDescriptor> for geoengine_operators::engine::RasterResultDescriptor {
    fn from(value: RasterResultDescriptor) -> Self {
        Self {
            data_type: value.data_type.into(),
            spatial_reference: value.spatial_reference.into(),
            measurement: value.measurement.into(),
            time: value.time.map(Into::into),
            bbox: value.bbox.map(Into::into),
            resolution: value.resolution.map(Into::into),
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

impl ToSchema for TypedOperator {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property(
                "type",
                ObjectBuilder::new()
                    .schema_type(SchemaType::String)
                    .enum_values(Some(vec!["Vector", "Raster", "Plot"]))
            )
            .required("type")
            .property(
                "operator",
                ObjectBuilder::new()
                    .property(
                        "type",
                        Object::with_type(SchemaType::String)
                    )
                    .required("type")
                    .property(
                        "params",
                        Object::with_type(SchemaType::Object)
                    )
                    .property(
                        "sources",
                        Object::with_type(SchemaType::Object)
                    )
            )
            .required("operator")
            .example(Some(serde_json::json!(
                {"type": "MockPointSource", "params": {"points": [{"x": 0.0, "y": 0.1}, {"x": 1.0, "y": 1.1}]}
            })))
            .description(Some("An enum to differentiate between `Operator` variants"))
            .into()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VectorResultDescriptor {
    pub data_type: VectorDataType,
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum TypedResultDescriptor {
    Plot(PlotResultDescriptor),
    Raster(RasterResultDescriptor),
    Vector(VectorResultDescriptor),
}

impl From<geoengine_operators::engine::TypedResultDescriptor> for TypedResultDescriptor {
    fn from(value: geoengine_operators::engine::TypedResultDescriptor) -> Self {
        match value {
            geoengine_operators::engine::TypedResultDescriptor::Plot(p) => Self::Plot(p.into()),
            geoengine_operators::engine::TypedResultDescriptor::Raster(r) => Self::Raster(r.into()),
            geoengine_operators::engine::TypedResultDescriptor::Vector(v) => Self::Vector(v.into()),
        }
    }
}

impl From<geoengine_operators::engine::PlotResultDescriptor> for TypedResultDescriptor {
    fn from(value: geoengine_operators::engine::PlotResultDescriptor) -> Self {
        Self::Plot(value.into())
    }
}

impl From<PlotResultDescriptor> for TypedResultDescriptor {
    fn from(value: PlotResultDescriptor) -> Self {
        Self::Plot(value)
    }
}

impl From<geoengine_operators::engine::RasterResultDescriptor> for TypedResultDescriptor {
    fn from(value: geoengine_operators::engine::RasterResultDescriptor) -> Self {
        Self::Raster(value.into())
    }
}

impl From<RasterResultDescriptor> for TypedResultDescriptor {
    fn from(value: RasterResultDescriptor) -> Self {
        Self::Raster(value)
    }
}

impl From<geoengine_operators::engine::VectorResultDescriptor> for TypedResultDescriptor {
    fn from(value: geoengine_operators::engine::VectorResultDescriptor) -> Self {
        Self::Vector(value.into())
    }
}

impl From<VectorResultDescriptor> for TypedResultDescriptor {
    fn from(value: VectorResultDescriptor) -> Self {
        Self::Vector(value)
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

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StaticMetaData<L, R, Q> {
    pub loading_info: L,
    pub result_descriptor: R,
    #[serde(skip)]
    pub phantom: PhantomData<Q>,
}

impl
    From<
        geoengine_operators::engine::StaticMetaData<
            geoengine_operators::mock::MockDatasetDataSourceLoadingInfo,
            geoengine_operators::engine::VectorResultDescriptor,
            geoengine_datatypes::primitives::VectorQueryRectangle,
        >,
    >
    for StaticMetaData<
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        QueryRectangle<BoundingBox2D>,
    >
{
    fn from(
        value: geoengine_operators::engine::StaticMetaData<
            geoengine_operators::mock::MockDatasetDataSourceLoadingInfo,
            geoengine_operators::engine::VectorResultDescriptor,
            geoengine_datatypes::primitives::VectorQueryRectangle,
        >,
    ) -> Self {
        Self {
            loading_info: value.loading_info.into(),
            result_descriptor: value.result_descriptor.into(),
            phantom: Default::default(),
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
    > for StaticMetaData<OgrSourceDataset, VectorResultDescriptor, QueryRectangle<BoundingBox2D>>
{
    fn from(
        value: geoengine_operators::engine::StaticMetaData<
            geoengine_operators::source::OgrSourceDataset,
            geoengine_operators::engine::VectorResultDescriptor,
            geoengine_datatypes::primitives::VectorQueryRectangle,
        >,
    ) -> Self {
        Self {
            loading_info: value.loading_info.into(),
            result_descriptor: value.result_descriptor.into(),
            phantom: Default::default(),
        }
    }
}

#[async_trait]
impl<L, R, Q> MetaData<L, R, Q> for StaticMetaData<L, R, Q>
where
    L: Debug + Clone + Send + Sync + 'static,
    R: Debug + Send + Sync + 'static + ResultDescriptor,
    Q: Debug + Clone + Send + Sync + 'static,
{
    async fn loading_info(&self, _query: Q) -> geoengine_operators::util::Result<L> {
        Ok(self.loading_info.clone())
    }

    async fn result_descriptor(&self) -> geoengine_operators::util::Result<R> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(&self) -> Box<dyn MetaData<L, R, Q>> {
        Box::new(self.clone())
    }
}

pub type MockMetaData =
    StaticMetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>;
pub type OgrMetaData =
    StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>;

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

impl ToSchema for MockMetaData {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property(
                "loadingInfo",
                Ref::from_schema_name("MockDatasetDataSourceLoadingInfo"),
            )
            .required("loadingInfo")
            .property(
                "resultDescriptor",
                Ref::from_schema_name("VectorResultDescriptor"),
            )
            .required("resultDescriptor")
            .into()
    }
}

impl ToSchema for OgrMetaData {
    fn schema() -> utoipa::openapi::Schema {
        use utoipa::openapi::*;
        ObjectBuilder::new()
            .property("loadingInfo", Ref::from_schema_name("OgrSourceDataset"))
            .required("loadingInfo")
            .property(
                "resultDescriptor",
                Ref::from_schema_name("VectorResultDescriptor"),
            )
            .required("resultDescriptor")
            .into()
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataStatic {
    pub time: Option<TimeInterval>,
    pub params: GdalDatasetParameters,
    pub result_descriptor: RasterResultDescriptor,
}

impl From<geoengine_operators::source::GdalMetaDataStatic> for GdalMetaDataStatic {
    fn from(value: geoengine_operators::source::GdalMetaDataStatic) -> Self {
        Self {
            time: value.time.map(Into::into),
            params: value.params.into(),
            result_descriptor: value.result_descriptor.into(),
        }
    }
}

impl From<GdalMetaDataStatic> for geoengine_operators::source::GdalMetaDataStatic {
    fn from(value: GdalMetaDataStatic) -> Self {
        Self {
            time: value.time.map(Into::into),
            params: value.params.into(),
            result_descriptor: value.result_descriptor.into(),
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
        }
    }
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Clone, Debug, ToSchema)]
#[serde(tag = "format")]
#[serde(rename_all = "camelCase")]
pub enum OgrSourceTimeFormat {
    #[serde(rename_all = "camelCase")]
    Custom {
        custom_format: DateTimeParseFormat,
    },
    #[serde(rename_all = "camelCase")]
    UnixTimeStamp {
        timestamp_type: UnixTimeStampType,
        #[serde(skip)]
        #[serde(default = "DateTimeParseFormat::unix")]
        fmt: DateTimeParseFormat,
    },
    Auto,
}

impl From<geoengine_operators::source::OgrSourceTimeFormat> for OgrSourceTimeFormat {
    fn from(value: geoengine_operators::source::OgrSourceTimeFormat) -> Self {
        match value {
            geoengine_operators::source::OgrSourceTimeFormat::Custom { custom_format } => {
                Self::Custom {
                    custom_format: custom_format.into(),
                }
            }
            geoengine_operators::source::OgrSourceTimeFormat::UnixTimeStamp {
                timestamp_type,
                fmt,
            } => Self::UnixTimeStamp {
                timestamp_type: timestamp_type.into(),
                fmt: fmt.into(),
            },
            geoengine_operators::source::OgrSourceTimeFormat::Auto => Self::Auto,
        }
    }
}

impl From<OgrSourceTimeFormat> for geoengine_operators::source::OgrSourceTimeFormat {
    fn from(value: OgrSourceTimeFormat) -> Self {
        match value {
            OgrSourceTimeFormat::Custom { custom_format } => Self::Custom {
                custom_format: custom_format.into(),
            },
            OgrSourceTimeFormat::UnixTimeStamp {
                timestamp_type,
                fmt,
            } => Self::UnixTimeStamp {
                timestamp_type: timestamp_type.into(),
                fmt: fmt.into(),
            },
            OgrSourceTimeFormat::Auto => Self::Auto,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
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
#[serde(rename_all = "camelCase", tag = "type")]
pub enum OgrSourceDatasetTimeType {
    None,
    #[serde(rename_all = "camelCase")]
    Start {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        duration: OgrSourceDurationSpec,
    },
    #[serde(rename = "start+end")]
    #[serde(rename_all = "camelCase")]
    StartEnd {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        end_field: String,
        end_format: OgrSourceTimeFormat,
    },
    #[serde(rename = "start+duration")]
    #[serde(rename_all = "camelCase")]
    StartDuration {
        start_field: String,
        start_format: OgrSourceTimeFormat,
        duration_field: String,
    },
}

impl From<geoengine_operators::source::OgrSourceDatasetTimeType> for OgrSourceDatasetTimeType {
    fn from(value: geoengine_operators::source::OgrSourceDatasetTimeType) -> Self {
        match value {
            geoengine_operators::source::OgrSourceDatasetTimeType::None => Self::None,
            geoengine_operators::source::OgrSourceDatasetTimeType::Start {
                start_field,
                start_format,
                duration,
            } => Self::Start {
                start_field,
                start_format: start_format.into(),
                duration: duration.into(),
            },
            geoengine_operators::source::OgrSourceDatasetTimeType::StartEnd {
                start_field,
                start_format,
                end_field,
                end_format,
            } => Self::StartEnd {
                start_field,
                start_format: start_format.into(),
                end_field,
                end_format: end_format.into(),
            },
            geoengine_operators::source::OgrSourceDatasetTimeType::StartDuration {
                start_field,
                start_format,
                duration_field,
            } => Self::StartDuration {
                start_field,
                start_format: start_format.into(),
                duration_field,
            },
        }
    }
}

impl From<OgrSourceDatasetTimeType> for geoengine_operators::source::OgrSourceDatasetTimeType {
    fn from(value: OgrSourceDatasetTimeType) -> Self {
        match value {
            OgrSourceDatasetTimeType::None => Self::None,
            OgrSourceDatasetTimeType::Start {
                start_field,
                start_format,
                duration,
            } => Self::Start {
                start_field,
                start_format: start_format.into(),
                duration: duration.into(),
            },
            OgrSourceDatasetTimeType::StartEnd {
                start_field,
                start_format,
                end_field,
                end_format,
            } => Self::StartEnd {
                start_field,
                start_format: start_format.into(),
                end_field,
                end_format: end_format.into(),
            },
            OgrSourceDatasetTimeType::StartDuration {
                start_field,
                start_format,
                duration_field,
            } => Self::StartDuration {
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
        Self::None
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum OgrSourceDurationSpec {
    Infinite,
    Zero,
    Value(TimeStep),
}

impl From<geoengine_operators::source::OgrSourceDurationSpec> for OgrSourceDurationSpec {
    fn from(value: geoengine_operators::source::OgrSourceDurationSpec) -> Self {
        match value {
            geoengine_operators::source::OgrSourceDurationSpec::Infinite => Self::Infinite,
            geoengine_operators::source::OgrSourceDurationSpec::Zero => Self::Zero,
            geoengine_operators::source::OgrSourceDurationSpec::Value(v) => Self::Value(v.into()),
        }
    }
}

impl From<OgrSourceDurationSpec> for geoengine_operators::source::OgrSourceDurationSpec {
    fn from(value: OgrSourceDurationSpec) -> Self {
        match value {
            OgrSourceDurationSpec::Infinite => Self::Infinite,
            OgrSourceDurationSpec::Zero => Self::Zero,
            OgrSourceDurationSpec::Value(v) => Self::Value(v.into()),
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

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataRegular {
    pub result_descriptor: RasterResultDescriptor,
    pub params: GdalDatasetParameters,
    pub time_placeholders: HashMap<String, GdalSourceTimePlaceholder>,
    pub data_time: TimeInterval,
    pub step: TimeStep,
}

impl From<geoengine_operators::source::GdalMetaDataRegular> for GdalMetaDataRegular {
    fn from(value: geoengine_operators::source::GdalMetaDataRegular) -> Self {
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
        }
    }
}

pub type GdalConfigOption = StringPair;

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
    #[serde(default)]
    //#[serde(with = "float_option_with_nan")]
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
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
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
}

impl From<geoengine_operators::source::GdalMetadataNetCdfCf> for GdalMetadataNetCdfCf {
    fn from(value: geoengine_operators::source::GdalMetadataNetCdfCf) -> Self {
        Self {
            result_descriptor: value.result_descriptor.into(),
            params: value.params.into(),
            start: value.start.into(),
            end: value.end.into(),
            step: value.step.into(),
            band_offset: value.band_offset,
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
        }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GdalMetaDataList {
    pub result_descriptor: RasterResultDescriptor,
    pub params: Vec<GdalLoadingInfoTemporalSlice>,
}

impl From<geoengine_operators::source::GdalMetaDataList> for GdalMetaDataList {
    fn from(value: geoengine_operators::source::GdalMetaDataList) -> Self {
        Self {
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
}

impl From<geoengine_operators::source::GdalLoadingInfoTemporalSlice>
    for GdalLoadingInfoTemporalSlice
{
    fn from(value: geoengine_operators::source::GdalLoadingInfoTemporalSlice) -> Self {
        Self {
            time: value.time.into(),
            params: value.params.map(Into::into),
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
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
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
