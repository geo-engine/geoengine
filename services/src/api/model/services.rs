use super::datatypes::{CacheTtlSeconds, DataId, DataProviderId, GdalConfigOption, RasterDataType};
use crate::api::model::datatypes::MlModelName;
use crate::api::model::operators::{
    GdalMetaDataList, GdalMetaDataRegular, GdalMetaDataStatic, GdalMetadataNetCdfCf,
    MlModelMetadata, MockMetaData, OgrMetaData,
};
use crate::datasets::DatasetName;
use crate::datasets::external::{GdalRetries, WildliveDataConnectorAuth};
use crate::datasets::storage::validate_tags;
use crate::datasets::upload::{UploadId, VolumeName};
use crate::projects::Symbology;
use crate::util::Secret;
use crate::util::oidc::RefreshToken;
use crate::util::parsing::deserialize_base_url;
use geoengine_datatypes::primitives::DateTime;
use geoengine_macros::type_tag;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use url::Url;
use utoipa::ToSchema;
use validator::{Validate, ValidationErrors};

pub const SECRET_REPLACEMENT: &str = "*****";

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema, PartialEq)]
#[serde(untagged)]
#[schema(discriminator = "type")]
pub enum MetaDataDefinition {
    MockMetaData(MockMetaData),
    OgrMetaData(OgrMetaData),
    GdalMetaDataRegular(GdalMetaDataRegular),
    GdalStatic(GdalMetaDataStatic),
    GdalMetadataNetCdfCf(GdalMetadataNetCdfCf),
    GdalMetaDataList(GdalMetaDataList),
}

impl From<crate::datasets::storage::MetaDataDefinition> for MetaDataDefinition {
    fn from(value: crate::datasets::storage::MetaDataDefinition) -> Self {
        match value {
            crate::datasets::storage::MetaDataDefinition::MockMetaData(x) => {
                Self::MockMetaData(MockMetaData {
                    r#type: Default::default(),
                    loading_info: x.loading_info.into(),
                    result_descriptor: x.result_descriptor.into(),
                })
            }
            crate::datasets::storage::MetaDataDefinition::OgrMetaData(x) => {
                Self::OgrMetaData(OgrMetaData {
                    r#type: Default::default(),
                    loading_info: x.loading_info.into(),
                    result_descriptor: x.result_descriptor.into(),
                })
            }
            crate::datasets::storage::MetaDataDefinition::GdalMetaDataRegular(x) => {
                Self::GdalMetaDataRegular(x.into())
            }
            crate::datasets::storage::MetaDataDefinition::GdalStatic(x) => {
                Self::GdalStatic(x.into())
            }
            crate::datasets::storage::MetaDataDefinition::GdalMetadataNetCdfCf(x) => {
                Self::GdalMetadataNetCdfCf(x.into())
            }
            crate::datasets::storage::MetaDataDefinition::GdalMetaDataList(x) => {
                Self::GdalMetaDataList(x.into())
            }
        }
    }
}

impl From<MetaDataDefinition> for crate::datasets::storage::MetaDataDefinition {
    fn from(value: MetaDataDefinition) -> Self {
        match value {
            MetaDataDefinition::MockMetaData(x) => Self::MockMetaData(x.into()),
            MetaDataDefinition::OgrMetaData(x) => Self::OgrMetaData(x.into()),
            MetaDataDefinition::GdalMetaDataRegular(x) => Self::GdalMetaDataRegular(x.into()),
            MetaDataDefinition::GdalStatic(x) => Self::GdalStatic(x.into()),
            MetaDataDefinition::GdalMetadataNetCdfCf(x) => Self::GdalMetadataNetCdfCf(x.into()),
            MetaDataDefinition::GdalMetaDataList(x) => Self::GdalMetaDataList(x.into()),
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetaDataSuggestion {
    pub main_file: String,
    pub layer_name: String,
    pub meta_data: MetaDataDefinition,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
// TODO: validate user input
pub struct AddDataset {
    pub name: Option<DatasetName>,
    pub display_name: String,
    pub description: String,
    pub source_operator: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Vec<Provenance>>,
    pub tags: Option<Vec<String>>,
}

impl From<AddDataset> for crate::datasets::storage::AddDataset {
    fn from(value: AddDataset) -> Self {
        Self {
            name: value.name,
            display_name: value.display_name,
            description: value.description,
            source_operator: value.source_operator,
            symbology: value.symbology,
            provenance: value
                .provenance
                .map(|v| v.into_iter().map(Into::into).collect()),
            tags: value.tags,
        }
    }
}

impl From<crate::datasets::storage::AddDataset> for AddDataset {
    fn from(value: crate::datasets::storage::AddDataset) -> Self {
        Self {
            name: value.name,
            display_name: value.display_name,
            description: value.description,
            source_operator: value.source_operator,
            symbology: value.symbology,
            provenance: value
                .provenance
                .map(|v| v.into_iter().map(Into::into).collect()),
            tags: value.tags,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetDefinition {
    pub properties: AddDataset,
    pub meta_data: MetaDataDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateDataset {
    pub data_path: DataPath,
    pub definition: DatasetDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum DataPath {
    Volume(VolumeName),
    Upload(UploadId),
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema, Validate)]
pub struct UpdateDataset {
    pub name: DatasetName,
    #[validate(length(min = 1))]
    pub display_name: String,
    pub description: String,
    #[validate(custom(function = "validate_tags"))]
    pub tags: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Eq, Debug, Clone, ToSchema, Validate)]
pub struct Provenance {
    #[validate(length(min = 1))]
    pub citation: String,
    #[validate(length(min = 1))]
    pub license: String,
    #[validate(length(min = 1))]
    pub uri: String,
}

impl From<Provenance> for crate::datasets::listing::Provenance {
    fn from(value: Provenance) -> Self {
        Self {
            citation: value.citation,
            license: value.license,
            uri: value.uri,
        }
    }
}

impl From<crate::datasets::listing::Provenance> for Provenance {
    fn from(value: crate::datasets::listing::Provenance) -> Self {
        Self {
            citation: value.citation,
            license: value.license,
            uri: value.uri,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct Provenances {
    pub provenances: Vec<Provenance>,
}

impl Validate for Provenances {
    fn validate(&self) -> Result<(), ValidationErrors> {
        for provenance in &self.provenances {
            provenance.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ProvenanceOutput {
    pub data: DataId,
    pub provenance: Option<Vec<Provenance>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct Volume {
    pub name: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, ToSchema, Serialize)]
pub struct LayerProviderListing {
    pub id: DataProviderId,
    pub name: String,
    pub priority: i16,
}

impl From<crate::layers::storage::LayerProviderListing> for LayerProviderListing {
    fn from(value: crate::layers::storage::LayerProviderListing) -> Self {
        LayerProviderListing {
            id: value.id.into(),
            name: value.name,
            priority: value.priority,
        }
    }
}

#[type_tag(value = "Aruna")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ArunaDataProviderDefinition {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub api_url: String,
    pub project_id: String,
    pub api_token: String,
    pub filter_label: String,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<ArunaDataProviderDefinition>
    for crate::datasets::external::aruna::ArunaDataProviderDefinition
{
    fn from(value: ArunaDataProviderDefinition) -> Self {
        crate::datasets::external::aruna::ArunaDataProviderDefinition {
            id: value.id.into(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            api_url: value.api_url,
            project_id: value.project_id,
            api_token: value.api_token,
            filter_label: value.filter_label,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<crate::datasets::external::aruna::ArunaDataProviderDefinition>
    for ArunaDataProviderDefinition
{
    fn from(value: crate::datasets::external::aruna::ArunaDataProviderDefinition) -> Self {
        ArunaDataProviderDefinition {
            r#type: Default::default(),
            id: value.id.into(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            api_url: value.api_url,
            project_id: value.project_id,
            api_token: SECRET_REPLACEMENT.to_string(),
            filter_label: value.filter_label,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[type_tag(value = "CopernicusDataspace")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CopernicusDataspaceDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub id: DataProviderId,
    pub stac_url: String,
    pub s3_url: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub gdal_config: Vec<GdalConfigOption>,
    pub priority: Option<i16>,
}

impl From<CopernicusDataspaceDataProviderDefinition>
    for crate::datasets::external::copernicus_dataspace::CopernicusDataspaceDataProviderDefinition
{
    fn from(value: CopernicusDataspaceDataProviderDefinition) -> Self {
        crate::datasets::external::copernicus_dataspace::CopernicusDataspaceDataProviderDefinition {
            name: value.name,
            description: value.description,
            id: value.id.into(),
            stac_url: value.stac_url,
            s3_url: value.s3_url,
            s3_access_key: value.s3_access_key,
            s3_secret_key: value.s3_secret_key,
            gdal_config: value.gdal_config,
            priority: value.priority,
        }
    }
}

impl
    From<crate::datasets::external::copernicus_dataspace::CopernicusDataspaceDataProviderDefinition>
    for CopernicusDataspaceDataProviderDefinition
{
    fn from(
        value: crate::datasets::external::copernicus_dataspace::CopernicusDataspaceDataProviderDefinition,
    ) -> Self {
        CopernicusDataspaceDataProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            description: value.description,
            id: value.id.into(),
            stac_url: value.stac_url,
            s3_url: value.s3_url,
            s3_access_key: value.s3_access_key,
            s3_secret_key: SECRET_REPLACEMENT.to_string(),
            gdal_config: value.gdal_config,
            priority: value.priority,
        }
    }
}

#[type_tag(value = "EbvPortal")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EbvPortalDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub base_url: Url,
    /// Path were the `NetCDF` data can be found
    #[schema(value_type = String)]
    pub data: PathBuf,
    /// Path were overview files are stored
    #[schema(value_type = String)]
    pub overviews: PathBuf,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<EbvPortalDataProviderDefinition>
    for crate::datasets::external::netcdfcf::EbvPortalDataProviderDefinition
{
    fn from(value: EbvPortalDataProviderDefinition) -> Self {
        crate::datasets::external::netcdfcf::EbvPortalDataProviderDefinition {
            name: value.name,
            description: value.description,
            priority: value.priority,
            base_url: value.base_url,
            data: value.data,
            overviews: value.overviews,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<crate::datasets::external::netcdfcf::EbvPortalDataProviderDefinition>
    for EbvPortalDataProviderDefinition
{
    fn from(value: crate::datasets::external::netcdfcf::EbvPortalDataProviderDefinition) -> Self {
        EbvPortalDataProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            base_url: value.base_url,
            data: value.data,
            overviews: value.overviews,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[type_tag(value = "NetCdfCf")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NetCdfCfDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    /// Path were the `NetCDF` data can be found
    #[schema(value_type = String)]
    pub data: PathBuf,
    /// Path were overview files are stored
    #[schema(value_type = String)]
    pub overviews: PathBuf,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<NetCdfCfDataProviderDefinition>
    for crate::datasets::external::netcdfcf::NetCdfCfDataProviderDefinition
{
    fn from(value: NetCdfCfDataProviderDefinition) -> Self {
        crate::datasets::external::netcdfcf::NetCdfCfDataProviderDefinition {
            name: value.name,
            description: value.description,
            priority: value.priority,
            data: value.data,
            overviews: value.overviews,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<crate::datasets::external::netcdfcf::NetCdfCfDataProviderDefinition>
    for NetCdfCfDataProviderDefinition
{
    fn from(value: crate::datasets::external::netcdfcf::NetCdfCfDataProviderDefinition) -> Self {
        NetCdfCfDataProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            data: value.data,
            overviews: value.overviews,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[type_tag(value = "Pangaea")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PangaeaDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub base_url: Url,
    pub cache_ttl: CacheTtlSeconds,
}

impl From<PangaeaDataProviderDefinition>
    for crate::datasets::external::pangaea::PangaeaDataProviderDefinition
{
    fn from(value: PangaeaDataProviderDefinition) -> Self {
        crate::datasets::external::pangaea::PangaeaDataProviderDefinition {
            name: value.name,
            description: value.description,
            priority: value.priority,
            base_url: value.base_url,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<crate::datasets::external::pangaea::PangaeaDataProviderDefinition>
    for PangaeaDataProviderDefinition
{
    fn from(value: crate::datasets::external::pangaea::PangaeaDataProviderDefinition) -> Self {
        PangaeaDataProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            base_url: value.base_url,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[type_tag(value = "Edr")]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EdrDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub id: DataProviderId,
    #[serde(deserialize_with = "deserialize_base_url")]
    pub base_url: Url,
    pub vector_spec: Option<EdrVectorSpec>,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
    #[serde(default)]
    /// List of vertical reference systems with a discrete scale
    pub discrete_vrs: Vec<String>,
    pub provenance: Option<Vec<Provenance>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct EdrVectorSpec {
    pub x: String,
    pub y: Option<String>,
    pub time: String,
}

impl From<EdrVectorSpec> for crate::datasets::external::edr::EdrVectorSpec {
    fn from(value: EdrVectorSpec) -> Self {
        crate::datasets::external::edr::EdrVectorSpec {
            x: value.x,
            y: value.y,
            time: value.time,
        }
    }
}

impl From<crate::datasets::external::edr::EdrVectorSpec> for EdrVectorSpec {
    fn from(value: crate::datasets::external::edr::EdrVectorSpec) -> Self {
        EdrVectorSpec {
            x: value.x,
            y: value.y,
            time: value.time,
        }
    }
}

impl From<EdrDataProviderDefinition> for crate::datasets::external::edr::EdrDataProviderDefinition {
    fn from(value: EdrDataProviderDefinition) -> Self {
        crate::datasets::external::edr::EdrDataProviderDefinition {
            name: value.name,
            description: value.description,
            priority: value.priority,
            id: value.id.into(),
            base_url: value.base_url,
            vector_spec: value.vector_spec.map(Into::into),
            cache_ttl: value.cache_ttl.into(),
            discrete_vrs: value.discrete_vrs,
            provenance: value
                .provenance
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

impl From<crate::datasets::external::edr::EdrDataProviderDefinition> for EdrDataProviderDefinition {
    fn from(value: crate::datasets::external::edr::EdrDataProviderDefinition) -> Self {
        EdrDataProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            id: value.id.into(),
            base_url: value.base_url,
            vector_spec: value.vector_spec.map(Into::into),
            cache_ttl: value.cache_ttl.into(),
            discrete_vrs: value.discrete_vrs,
            provenance: value
                .provenance
                .map(|v| v.into_iter().map(Into::into).collect()),
        }
    }
}

#[type_tag(value = "Gbif")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GbifDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub db_config: DatabaseConnectionConfig,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
    pub autocomplete_timeout: i32,
    pub columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct DatabaseConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub schema: String,
    pub user: String,
    pub password: String,
}

impl From<DatabaseConnectionConfig> for crate::util::postgres::DatabaseConnectionConfig {
    fn from(value: DatabaseConnectionConfig) -> Self {
        crate::util::postgres::DatabaseConnectionConfig {
            host: value.host,
            port: value.port,
            database: value.database,
            schema: value.schema,
            user: value.user,
            password: value.password,
        }
    }
}

impl From<crate::util::postgres::DatabaseConnectionConfig> for DatabaseConnectionConfig {
    fn from(value: crate::util::postgres::DatabaseConnectionConfig) -> Self {
        DatabaseConnectionConfig {
            host: value.host,
            port: value.port,
            database: value.database,
            schema: value.schema,
            user: value.user,
            password: SECRET_REPLACEMENT.to_string(),
        }
    }
}

impl From<GbifDataProviderDefinition>
    for crate::datasets::external::gbif::GbifDataProviderDefinition
{
    fn from(value: GbifDataProviderDefinition) -> Self {
        crate::datasets::external::gbif::GbifDataProviderDefinition {
            name: value.name,
            description: value.description,
            priority: value.priority,
            db_config: value.db_config.into(),
            cache_ttl: value.cache_ttl.into(),
            autocomplete_timeout: value.autocomplete_timeout,
            columns: value.columns,
        }
    }
}

impl From<crate::datasets::external::gbif::GbifDataProviderDefinition>
    for GbifDataProviderDefinition
{
    fn from(value: crate::datasets::external::gbif::GbifDataProviderDefinition) -> Self {
        GbifDataProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            db_config: value.db_config.into(),
            cache_ttl: value.cache_ttl.into(),
            autocomplete_timeout: value.autocomplete_timeout,
            columns: value.columns,
        }
    }
}

#[type_tag(value = "GfbioAbcd")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GfbioAbcdDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub db_config: DatabaseConnectionConfig,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<GfbioAbcdDataProviderDefinition>
    for crate::datasets::external::gfbio_abcd::GfbioAbcdDataProviderDefinition
{
    fn from(value: GfbioAbcdDataProviderDefinition) -> Self {
        crate::datasets::external::gfbio_abcd::GfbioAbcdDataProviderDefinition {
            name: value.name,
            description: value.description,
            priority: value.priority,
            db_config: value.db_config.into(),
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<crate::datasets::external::gfbio_abcd::GfbioAbcdDataProviderDefinition>
    for GfbioAbcdDataProviderDefinition
{
    fn from(value: crate::datasets::external::gfbio_abcd::GfbioAbcdDataProviderDefinition) -> Self {
        GfbioAbcdDataProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            db_config: value.db_config.into(),
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[type_tag(value = "GfbioCollections")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GfbioCollectionsDataProviderDefinition {
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub collection_api_url: Url,
    pub collection_api_auth_token: String,
    pub abcd_db_config: DatabaseConnectionConfig,
    pub pangaea_url: Url,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
}

impl From<GfbioCollectionsDataProviderDefinition>
    for crate::datasets::external::gfbio_collections::GfbioCollectionsDataProviderDefinition
{
    fn from(value: GfbioCollectionsDataProviderDefinition) -> Self {
        crate::datasets::external::gfbio_collections::GfbioCollectionsDataProviderDefinition {
            name: value.name,
            description: value.description,
            priority: value.priority,
            collection_api_url: value.collection_api_url,
            collection_api_auth_token: value.collection_api_auth_token,
            abcd_db_config: value.abcd_db_config.into(),
            pangaea_url: value.pangaea_url,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

impl From<crate::datasets::external::gfbio_collections::GfbioCollectionsDataProviderDefinition>
    for GfbioCollectionsDataProviderDefinition
{
    fn from(
        value: crate::datasets::external::gfbio_collections::GfbioCollectionsDataProviderDefinition,
    ) -> Self {
        GfbioCollectionsDataProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            collection_api_url: value.collection_api_url,
            collection_api_auth_token: value.collection_api_auth_token,
            abcd_db_config: value.abcd_db_config.into(),
            pangaea_url: value.pangaea_url,
            cache_ttl: value.cache_ttl.into(),
        }
    }
}

#[type_tag(value = "SentinelS2L2ACogs")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SentinelS2L2ACogsProviderDefinition {
    pub name: String,
    pub id: DataProviderId,
    pub description: String,
    pub priority: Option<i16>,
    pub api_url: String,
    pub bands: Vec<StacBand>,
    pub zones: Vec<StacZone>,
    #[serde(default)]
    pub stac_api_retries: StacApiRetries,
    #[serde(default)]
    pub gdal_retries: usize,
    #[serde(default)]
    pub cache_ttl: CacheTtlSeconds,
    #[serde(default)]
    pub query_buffer: StacQueryBuffer,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StacBand {
    pub name: String,
    pub no_data_value: Option<f64>,
    pub data_type: RasterDataType,
}

impl From<StacBand> for crate::datasets::external::sentinel_s2_l2a_cogs::StacBand {
    fn from(value: StacBand) -> Self {
        crate::datasets::external::sentinel_s2_l2a_cogs::StacBand {
            name: value.name,
            no_data_value: value.no_data_value,
            data_type: value.data_type.into(),
        }
    }
}

impl From<crate::datasets::external::sentinel_s2_l2a_cogs::StacBand> for StacBand {
    fn from(value: crate::datasets::external::sentinel_s2_l2a_cogs::StacBand) -> Self {
        StacBand {
            name: value.name,
            no_data_value: value.no_data_value,
            data_type: value.data_type.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, ToSchema)]
pub struct StacZone {
    pub name: String,
    pub epsg: u32,
}

impl From<StacZone> for crate::datasets::external::sentinel_s2_l2a_cogs::StacZone {
    fn from(value: StacZone) -> Self {
        crate::datasets::external::sentinel_s2_l2a_cogs::StacZone {
            name: value.name,
            epsg: value.epsg,
        }
    }
}

impl From<crate::datasets::external::sentinel_s2_l2a_cogs::StacZone> for StacZone {
    fn from(value: crate::datasets::external::sentinel_s2_l2a_cogs::StacZone) -> Self {
        StacZone {
            name: value.name,
            epsg: value.epsg,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct StacApiRetries {
    pub number_of_retries: usize,
    pub initial_delay_ms: u64,
    pub exponential_backoff_factor: f64,
}

impl From<StacApiRetries> for crate::datasets::external::sentinel_s2_l2a_cogs::StacApiRetries {
    fn from(value: StacApiRetries) -> Self {
        crate::datasets::external::sentinel_s2_l2a_cogs::StacApiRetries {
            number_of_retries: value.number_of_retries,
            initial_delay_ms: value.initial_delay_ms,
            exponential_backoff_factor: value.exponential_backoff_factor,
        }
    }
}

impl From<crate::datasets::external::sentinel_s2_l2a_cogs::StacApiRetries> for StacApiRetries {
    fn from(value: crate::datasets::external::sentinel_s2_l2a_cogs::StacApiRetries) -> Self {
        StacApiRetries {
            number_of_retries: value.number_of_retries,
            initial_delay_ms: value.initial_delay_ms,
            exponential_backoff_factor: value.exponential_backoff_factor,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
/// A struct that represents buffers to apply to stac requests
pub struct StacQueryBuffer {
    pub start_seconds: i64,
    pub end_seconds: i64,
    // TODO: add also spatial buffers?
}

impl From<StacQueryBuffer> for crate::datasets::external::sentinel_s2_l2a_cogs::StacQueryBuffer {
    fn from(value: StacQueryBuffer) -> Self {
        crate::datasets::external::sentinel_s2_l2a_cogs::StacQueryBuffer {
            start_seconds: value.start_seconds,
            end_seconds: value.end_seconds,
        }
    }
}

impl From<crate::datasets::external::sentinel_s2_l2a_cogs::StacQueryBuffer> for StacQueryBuffer {
    fn from(value: crate::datasets::external::sentinel_s2_l2a_cogs::StacQueryBuffer) -> Self {
        StacQueryBuffer {
            start_seconds: value.start_seconds,
            end_seconds: value.end_seconds,
        }
    }
}

impl From<usize> for GdalRetries {
    fn from(value: usize) -> Self {
        GdalRetries {
            number_of_retries: value,
        }
    }
}

impl From<GdalRetries> for usize {
    fn from(value: GdalRetries) -> Self {
        value.number_of_retries
    }
}

impl From<SentinelS2L2ACogsProviderDefinition>
    for crate::datasets::external::sentinel_s2_l2a_cogs::SentinelS2L2ACogsProviderDefinition
{
    fn from(value: SentinelS2L2ACogsProviderDefinition) -> Self {
        crate::datasets::external::sentinel_s2_l2a_cogs::SentinelS2L2ACogsProviderDefinition {
            name: value.name,
            id: value.id.into(),
            description: value.description,
            priority: value.priority,
            api_url: value.api_url,
            bands: value.bands.into_iter().map(Into::into).collect(),
            zones: value.zones.into_iter().map(Into::into).collect(),
            stac_api_retries: value.stac_api_retries.into(),
            gdal_retries: value.gdal_retries.into(),
            cache_ttl: value.cache_ttl.into(),
            query_buffer: value.query_buffer.into(),
        }
    }
}

impl From<crate::datasets::external::sentinel_s2_l2a_cogs::SentinelS2L2ACogsProviderDefinition>
    for SentinelS2L2ACogsProviderDefinition
{
    fn from(
        value: crate::datasets::external::sentinel_s2_l2a_cogs::SentinelS2L2ACogsProviderDefinition,
    ) -> Self {
        SentinelS2L2ACogsProviderDefinition {
            r#type: Default::default(),
            name: value.name,
            id: value.id.into(),
            description: value.description,
            priority: value.priority,
            api_url: value.api_url,
            bands: value.bands.into_iter().map(Into::into).collect(),
            zones: value.zones.into_iter().map(Into::into).collect(),
            stac_api_retries: value.stac_api_retries.into(),
            gdal_retries: value.gdal_retries.into(),
            cache_ttl: value.cache_ttl.into(),
            query_buffer: value.query_buffer.into(),
        }
    }
}

#[type_tag(value = "DatasetLayerListing")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetLayerListingProviderDefinition {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub priority: Option<i16>,
    pub collections: Vec<DatasetLayerListingCollection>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct DatasetLayerListingCollection {
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
}

impl From<DatasetLayerListingCollection>
    for crate::datasets::dataset_listing_provider::DatasetLayerListingCollection
{
    fn from(value: DatasetLayerListingCollection) -> Self {
        crate::datasets::dataset_listing_provider::DatasetLayerListingCollection {
            name: value.name,
            description: value.description,
            tags: value.tags,
        }
    }
}

impl From<crate::datasets::dataset_listing_provider::DatasetLayerListingCollection>
    for DatasetLayerListingCollection
{
    fn from(
        value: crate::datasets::dataset_listing_provider::DatasetLayerListingCollection,
    ) -> Self {
        DatasetLayerListingCollection {
            name: value.name,
            description: value.description,
            tags: value.tags,
        }
    }
}

impl From<DatasetLayerListingProviderDefinition>
    for crate::datasets::dataset_listing_provider::DatasetLayerListingProviderDefinition
{
    fn from(value: DatasetLayerListingProviderDefinition) -> Self {
        crate::datasets::dataset_listing_provider::DatasetLayerListingProviderDefinition {
            id: value.id.into(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            collections: value.collections.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<crate::datasets::dataset_listing_provider::DatasetLayerListingProviderDefinition>
    for DatasetLayerListingProviderDefinition
{
    fn from(
        value: crate::datasets::dataset_listing_provider::DatasetLayerListingProviderDefinition,
    ) -> Self {
        DatasetLayerListingProviderDefinition {
            r#type: Default::default(),
            id: value.id.into(),
            name: value.name,
            description: value.description,
            priority: value.priority,
            collections: value.collections.into_iter().map(Into::into).collect(),
        }
    }
}

#[type_tag(value = "WildLIVE!")]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WildliveDataConnectorDefinition {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
    pub user: Option<String>,
    pub refresh_token: Option<Secret<String>>,
    pub expiry_date: Option<DateTime>,
    pub priority: Option<i16>,
}

impl From<WildliveDataConnectorDefinition>
    for crate::datasets::external::WildliveDataConnectorDefinition
{
    fn from(value: WildliveDataConnectorDefinition) -> Self {
        crate::datasets::external::WildliveDataConnectorDefinition {
            id: value.id.into(),
            name: value.name,
            description: value.description,
            auth: value.refresh_token.map(|t| WildliveDataConnectorAuth {
                user: value.user.unwrap_or_else(|| "<unknown user>".to_string()),
                refresh_token: Secret::new(RefreshToken::new(t.0)),
                expiry_date: value.expiry_date.unwrap_or_else(DateTime::now),
            }),
            priority: value.priority,
        }
    }
}

impl From<crate::datasets::external::WildliveDataConnectorDefinition>
    for WildliveDataConnectorDefinition
{
    fn from(value: crate::datasets::external::WildliveDataConnectorDefinition) -> Self {
        let (user, refresh_token, expiry_date) = match value.auth {
            Some(auth) => (
                Some(auth.user),
                Some(Secret::new(
                    auth.refresh_token.into_inner().into_inner().into_secret(),
                )),
                Some(auth.expiry_date),
            ),
            _ => (None, None, None),
        };

        WildliveDataConnectorDefinition {
            r#type: Default::default(),
            id: value.id.into(),
            name: value.name,
            description: value.description,
            user,
            refresh_token,
            expiry_date,
            priority: value.priority,
        }
    }
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone, ToSchema)]
#[allow(clippy::enum_variant_names)] // TODO: think about better names
#[schema(discriminator = "type")]
#[serde(untagged)]
pub enum TypedDataProviderDefinition {
    ArunaDataProviderDefinition(ArunaDataProviderDefinition),
    CopernicusDataspaceDataProviderDefinition(CopernicusDataspaceDataProviderDefinition),
    DatasetLayerListingProviderDefinition(DatasetLayerListingProviderDefinition),
    EbvPortalDataProviderDefinition(EbvPortalDataProviderDefinition),
    EdrDataProviderDefinition(EdrDataProviderDefinition),
    GbifDataProviderDefinition(GbifDataProviderDefinition),
    GfbioAbcdDataProviderDefinition(GfbioAbcdDataProviderDefinition),
    GfbioCollectionsDataProviderDefinition(GfbioCollectionsDataProviderDefinition),
    NetCdfCfDataProviderDefinition(NetCdfCfDataProviderDefinition),
    PangaeaDataProviderDefinition(PangaeaDataProviderDefinition),
    SentinelS2L2ACogsProviderDefinition(SentinelS2L2ACogsProviderDefinition),
    WildliveDataConnectorDefinition(WildliveDataConnectorDefinition),
}

impl From<TypedDataProviderDefinition> for crate::layers::external::TypedDataProviderDefinition {
    fn from(value: TypedDataProviderDefinition) -> Self {
        match value {
            TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::ArunaDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::CopernicusDataspaceDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::CopernicusDataspaceDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(def.into()),
            TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::EdrDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::EdrDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::GbifDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::GbifDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::PangaeaDataProviderDefinition(def.into()),
            TypedDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(def) => crate::layers::external::TypedDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(def.into()),
            TypedDataProviderDefinition::WildliveDataConnectorDefinition(def) => crate::layers::external::TypedDataProviderDefinition::WildliveDataConnectorDefinition(def.into()),
        }
    }
}

impl From<crate::layers::external::TypedDataProviderDefinition> for TypedDataProviderDefinition {
    fn from(value: crate::layers::external::TypedDataProviderDefinition) -> Self {
        match value {
            crate::layers::external::TypedDataProviderDefinition::ArunaDataProviderDefinition(def) => TypedDataProviderDefinition::ArunaDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::CopernicusDataspaceDataProviderDefinition(def) => TypedDataProviderDefinition::CopernicusDataspaceDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(def) => TypedDataProviderDefinition::DatasetLayerListingProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def) => TypedDataProviderDefinition::EbvPortalDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::EdrDataProviderDefinition(def) => TypedDataProviderDefinition::EdrDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::GbifDataProviderDefinition(def) => TypedDataProviderDefinition::GbifDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def) => TypedDataProviderDefinition::GfbioAbcdDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def) => TypedDataProviderDefinition::GfbioCollectionsDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def) => TypedDataProviderDefinition::NetCdfCfDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::PangaeaDataProviderDefinition(def) => TypedDataProviderDefinition::PangaeaDataProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(def) => TypedDataProviderDefinition::SentinelS2L2ACogsProviderDefinition(def.into()),
            crate::layers::external::TypedDataProviderDefinition::WildliveDataConnectorDefinition(def) => TypedDataProviderDefinition::WildliveDataConnectorDefinition(def.into()),
        }
    }
}

impl From<&Volume> for crate::datasets::upload::Volume {
    fn from(value: &Volume) -> Self {
        Self {
            name: VolumeName(value.name.clone()),
            path: value.path.as_ref().map_or_else(PathBuf::new, PathBuf::from),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MlModel {
    pub name: MlModelName,
    pub display_name: String,
    pub description: String,
    pub upload: UploadId,
    pub metadata: MlModelMetadata,
    pub file_name: String,
}

impl From<MlModel> for crate::machine_learning::MlModel {
    fn from(value: MlModel) -> Self {
        crate::machine_learning::MlModel {
            name: value.name.into(),
            display_name: value.display_name,
            description: value.description,
            upload: value.upload,
            metadata: value.metadata.into(),
            file_name: value.file_name,
        }
    }
}

impl From<crate::machine_learning::MlModel> for MlModel {
    fn from(value: crate::machine_learning::MlModel) -> Self {
        MlModel {
            name: value.name.into(),
            display_name: value.display_name,
            description: value.description,
            upload: value.upload,
            metadata: value.metadata.into(),
            file_name: value.file_name,
        }
    }
}
