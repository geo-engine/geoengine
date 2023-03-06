use crate::api::model::datatypes::{DataProviderId, DatasetId};
use crate::api::model::operators::TypedResultDescriptor;
use crate::api::model::services::AddDataset;
use crate::contexts::Session;
use crate::datasets::listing::{DatasetListing, DatasetProvider};
use crate::datasets::upload::UploadDb;
use crate::datasets::upload::UploadId;
use crate::error;
use crate::error::Result;

use crate::projects::Symbology;
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_operators::engine::MetaData;
use geoengine_operators::source::{GdalMetaDataList, GdalMetadataNetCdfCf};
use geoengine_operators::{engine::StaticMetaData, source::OgrSourceDataset};
use geoengine_operators::{engine::VectorResultDescriptor, source::GdalMetaDataRegular};
use geoengine_operators::{mock::MockDatasetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use std::fmt::Debug;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use super::listing::Provenance;

pub const DATASET_DB_LAYER_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0xac50_ed0d_c9a0_41f8_9ce8_35fc_9e38_299b);

pub const DATASET_DB_ROOT_COLLECTION_ID: Uuid =
    Uuid::from_u128(0x5460_73b6_d535_4205_b601_9967_5c9f_6dd7);

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub id: DatasetId,
    pub name: String,
    pub description: String,
    pub result_descriptor: TypedResultDescriptor,
    pub source_operator: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Vec<Provenance>>,
}

impl Dataset {
    pub fn listing(&self) -> DatasetListing {
        DatasetListing {
            id: self.id,
            name: self.name.clone(),
            description: self.description.clone(),
            tags: vec![], // TODO
            source_operator: self.source_operator.clone(),
            result_descriptor: self.result_descriptor.clone(),
            symbology: self.symbology.clone(),
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
#[schema(example = json!({
    "upload": "420b06de-0a7e-45cb-9c1c-ea901b46ab69",
    "datasetName": "Germany Border (auto)",
    "datasetDescription": "The Outline of Germany (auto detected format)",
    "mainFile": "germany_polygon.gpkg"
}))]
pub struct AutoCreateDataset {
    pub upload: UploadId,
    pub dataset_name: String,
    pub dataset_description: String,
    pub main_file: String,
}

impl UserInput for AutoCreateDataset {
    fn validate(&self) -> Result<()> {
        // TODO: more sophisticated input validation
        ensure!(!self.dataset_name.is_empty(), error::InvalidDatasetName);
        ensure!(
            !self.main_file.is_empty()
                && !self.main_file.contains('/')
                && !self.main_file.contains(".."),
            error::InvalidUploadFileName
        );

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct SuggestMetaData {
    #[param(example = "420b06de-0a7e-45cb-9c1c-ea901b46ab69")]
    pub upload: UploadId,
    #[param(example = "germany_polygon.gpkg")]
    pub main_file: Option<String>,
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
        dataset: Validated<AddDataset>,
        meta_data: Self::StorageType,
    ) -> Result<DatasetId>;

    async fn delete_dataset(&self, dataset: DatasetId) -> Result<()>;

    /// turn given `meta` data definition into the corresponding `StorageType` for the `DatasetStore`
    /// for use in the `add_dataset` method
    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType;
}
