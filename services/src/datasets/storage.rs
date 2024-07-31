use super::listing::Provenance;
use super::postgres::DatasetMetaData;
use super::{DatasetIdAndName, DatasetName};
use crate::api::model::services::{DataPath, UpdateDataset};
use crate::datasets::listing::{DatasetListing, DatasetProvider};
use crate::datasets::upload::UploadDb;
use crate::datasets::upload::UploadId;
use crate::error;
use crate::error::Result;
use crate::projects::Symbology;
use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_operators::engine::{MetaData, TypedResultDescriptor};
use geoengine_operators::source::{GdalMetaDataList, GdalMetadataNetCdfCf};
use geoengine_operators::{engine::StaticMetaData, source::OgrSourceDataset};
use geoengine_operators::{engine::VectorResultDescriptor, source::GdalMetaDataRegular};
use geoengine_operators::{mock::MockDatasetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::fmt::Debug;
use std::str::FromStr;
use strum_macros;
use strum_macros::{Display, EnumString};
use utoipa::ToSchema;
use uuid::Uuid;
use validator::{Validate, ValidationError};

pub const DATASET_DB_ROOT_COLLECTION_ID: Uuid =
    Uuid::from_u128(0x5460_73b6_d535_4205_b601_9967_5c9f_6dd7);

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub id: DatasetId,
    pub name: DatasetName,
    pub display_name: String,
    pub description: String,
    pub result_descriptor: TypedResultDescriptor,
    pub source_operator: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Vec<Provenance>>,
    pub tags: Option<Vec<String>>,
}

impl Dataset {
    pub fn listing(&self) -> DatasetListing {
        DatasetListing {
            id: self.id,
            name: self.name.clone(),
            display_name: self.display_name.clone(),
            description: self.description.clone(),
            tags: self.tags.clone().unwrap_or_default(), // TODO: figure out if we want to use Option<Vec<String>> everywhere or if Vec<String> is fine
            source_operator: self.source_operator.clone(),
            result_descriptor: self.result_descriptor.clone(),
            symbology: self.symbology.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AddDataset {
    pub name: Option<DatasetName>,
    pub display_name: String,
    pub description: String,
    pub source_operator: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Vec<Provenance>>,
    pub tags: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetDefinition {
    pub properties: AddDataset,
    pub meta_data: MetaDataDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema, Validate)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "upload": "420b06de-0a7e-45cb-9c1c-ea901b46ab69",
    "datasetName": "Germany Border (auto)",
    "datasetDescription": "The Outline of Germany (auto detected format)",
    "mainFile": "germany_polygon.gpkg",
    "tags": ["area"]
}))]
pub struct AutoCreateDataset {
    pub upload: UploadId,
    #[validate(length(min = 1))]
    pub dataset_name: String,
    pub dataset_description: String,
    #[validate(custom = "validate_main_file")]
    pub main_file: String,
    pub layer_name: Option<String>,
    #[validate(custom = "validate_tags")]
    pub tags: Option<Vec<String>>,
}

#[derive(Display, EnumString)]
pub enum ReservedTags {
    #[strum(serialize = "upload")]
    Upload,
    Deleted,
}

fn validate_main_file(main_file: &str) -> Result<(), ValidationError> {
    if main_file.is_empty() || main_file.contains('/') || main_file.contains("..") {
        return Err(ValidationError::new("Invalid upload file name"));
    }

    Ok(())
}

pub fn validate_tags(tags: &Vec<String>) -> Result<(), ValidationError> {
    for tag in tags {
        if tag.is_empty() || tag.contains('/') || tag.contains("..") || tag.contains(' ') {
            // TODO: more validation
            return Err(ValidationError::new("Invalid tag"));
        }
    }

    Ok(())
}

pub fn check_reserved_tags(tags: &Option<Vec<String>>) {
    if let Some(tags) = tags {
        for tag in tags {
            let conversion = ReservedTags::from_str(tag.as_str());
            if let Ok(reserved) = conversion {
                log::warn!(
                    "Adding a new dataset with a reserved tag: {}",
                    reserved.to_string()
                );
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SuggestMetaData {
    pub data_path: DataPath,
    pub main_file: Option<String>,
    pub layer_name: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetaDataSuggestion {
    pub main_file: String,
    pub meta_data: MetaDataDefinition,
}

#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Deserialize, Serialize, Debug, Clone, ToSchema)]
#[serde(tag = "type")]
pub enum MetaDataDefinition {
    MockMetaData(
        StaticMetaData<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    ),
    OgrMetaData(StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>),
    GdalMetaDataRegular(GdalMetaDataRegular),
    GdalStatic(GdalMetaDataStatic),
    GdalMetadataNetCdfCf(GdalMetadataNetCdfCf),
    GdalMetaDataList(GdalMetaDataList),
}

impl From<StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>
    for MetaDataDefinition
{
    fn from(
        meta_data: StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
    ) -> Self {
        MetaDataDefinition::OgrMetaData(meta_data)
    }
}

impl From<GdalMetaDataRegular> for MetaDataDefinition {
    fn from(meta_data: GdalMetaDataRegular) -> Self {
        MetaDataDefinition::GdalMetaDataRegular(meta_data)
    }
}

impl From<GdalMetaDataStatic> for MetaDataDefinition {
    fn from(meta_data: GdalMetaDataStatic) -> Self {
        MetaDataDefinition::GdalStatic(meta_data)
    }
}

impl From<GdalMetadataNetCdfCf> for MetaDataDefinition {
    fn from(meta_data: GdalMetadataNetCdfCf) -> Self {
        MetaDataDefinition::GdalMetadataNetCdfCf(meta_data)
    }
}

impl From<GdalMetaDataList> for MetaDataDefinition {
    fn from(meta_data: GdalMetaDataList) -> Self {
        MetaDataDefinition::GdalMetaDataList(meta_data)
    }
}

impl MetaDataDefinition {
    pub fn source_operator_type(&self) -> &str {
        match self {
            MetaDataDefinition::MockMetaData(_) => "MockDatasetDataSource",
            MetaDataDefinition::OgrMetaData(_) => "OgrSource",
            MetaDataDefinition::GdalMetaDataRegular(_)
            | MetaDataDefinition::GdalStatic(_)
            | MetaDataDefinition::GdalMetadataNetCdfCf(_)
            | MetaDataDefinition::GdalMetaDataList(_) => "GdalSource",
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            MetaDataDefinition::MockMetaData(_) => "MockMetaData",
            MetaDataDefinition::OgrMetaData(_) => "OgrMetaData",
            MetaDataDefinition::GdalMetaDataRegular(_) => "GdalMetaDataRegular",
            MetaDataDefinition::GdalStatic(_) => "GdalStatic",
            MetaDataDefinition::GdalMetadataNetCdfCf(_) => "GdalMetadataNetCdfCf",
            MetaDataDefinition::GdalMetaDataList(_) => "GdalMetaDataList",
        }
    }

    pub async fn result_descriptor(&self) -> Result<TypedResultDescriptor> {
        match self {
            MetaDataDefinition::MockMetaData(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::OgrMetaData(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::GdalMetaDataRegular(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::GdalStatic(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::GdalMetadataNetCdfCf(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
            MetaDataDefinition::GdalMetaDataList(m) => m
                .result_descriptor()
                .await
                .map(Into::into)
                .context(error::Operator),
        }
    }

    pub fn to_typed_metadata(&self) -> DatasetMetaData {
        match self {
            MetaDataDefinition::MockMetaData(d) => DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            },
            MetaDataDefinition::OgrMetaData(d) => DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            },
            MetaDataDefinition::GdalMetaDataRegular(d) => DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            },
            MetaDataDefinition::GdalStatic(d) => DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            },
            MetaDataDefinition::GdalMetadataNetCdfCf(d) => DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            },
            MetaDataDefinition::GdalMetaDataList(d) => DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            },
        }
    }
}

/// Handling of datasets provided by geo engine internally, staged and by external providers
#[async_trait]
pub trait DatasetDb: DatasetStore + DatasetProvider + UploadDb + Send + Sync {}

/// Storage of datasets
#[async_trait]
pub trait DatasetStore {
    async fn add_dataset(
        &self,
        dataset: AddDataset,
        meta_data: MetaDataDefinition,
    ) -> Result<DatasetIdAndName>;

    async fn update_dataset(&self, dataset: DatasetId, update: UpdateDataset) -> Result<()>;

    async fn update_dataset_loading_info(
        &self,
        dataset: DatasetId,
        meta_data: &MetaDataDefinition,
    ) -> Result<()>;

    async fn update_dataset_symbology(
        &self,
        dataset: DatasetId,
        symbology: &Symbology,
    ) -> Result<()>;

    async fn update_dataset_provenance(
        &self,
        dataset: DatasetId,
        provenance: &[Provenance],
    ) -> Result<()>;

    async fn delete_dataset(&self, dataset: DatasetId) -> Result<()>;
}
