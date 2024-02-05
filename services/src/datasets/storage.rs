use super::listing::Provenance;
use super::{DatasetIdAndName, DatasetName};
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
use utoipa::{IntoParams, ToSchema};
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

fn validate_main_file(main_file: &String) -> Result<(), ValidationError> {
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

#[derive(Deserialize, Serialize, Debug, Clone, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct SuggestMetaData {
    #[param(example = "420b06de-0a7e-45cb-9c1c-ea901b46ab69")]
    pub upload: UploadId,
    #[param(example = "germany_polygon.gpkg")]
    pub main_file: Option<String>,
    #[param(example = "test_polygon")]
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
}

/// Handling of datasets provided by geo engine internally, staged and by external providers
#[async_trait]
pub trait DatasetDb: DatasetStore + DatasetProvider + UploadDb + Send + Sync {}

/// Defines the type of meta data a `DatasetDB` is able to store
pub trait DatasetStorer: Send + Sync {
    type StorageType: Send + Sync;
}

/// Allow storage of meta data of a particular storage type, e.g. `HashMapStorable` meta data for
/// `HashMapDatasetDB`
#[async_trait]
pub trait DatasetStore: DatasetStorer {
    async fn add_dataset(
        &self,
        dataset: AddDataset,
        meta_data: Self::StorageType,
    ) -> Result<DatasetIdAndName>;

    async fn delete_dataset(&self, dataset: DatasetId) -> Result<()>;

    /// turn given `meta` data definition into the corresponding `StorageType` for the `DatasetStore`
    /// for use in the `add_dataset` method
    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType;
}
