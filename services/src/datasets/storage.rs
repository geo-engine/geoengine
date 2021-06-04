use crate::contexts::Session;
use crate::datasets::listing::{DatasetListing, DatasetProvider};
use crate::datasets::upload::UploadDb;
use crate::datasets::upload::UploadId;
use crate::error;
use crate::error::Result;
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, InternalDatasetId};
use geoengine_datatypes::util::Identifier;
use geoengine_operators::{engine::StaticMetaData, source::OgrSourceDataset};
use geoengine_operators::{
    engine::TypedResultDescriptor, mock::MockDatasetDataSourceLoadingInfo,
    source::GdalMetaDataStatic,
};
use geoengine_operators::{engine::VectorResultDescriptor, source::GdalMetaDataRegular};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub id: DatasetId,
    pub name: String,
    pub description: String,
    pub result_descriptor: TypedResultDescriptor,
    pub source_operator: String,
}

impl Dataset {
    pub fn listing(&self) -> DatasetListing {
        DatasetListing {
            id: self.id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            tags: vec![], // TODO
            source_operator: self.source_operator.clone(),
            result_descriptor: self.result_descriptor.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AddDataset {
    pub id: Option<DatasetId>,
    pub name: String,
    pub description: String,
    pub source_operator: String,
}

impl UserInput for AddDataset {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ImportDataset {
    pub name: String,
    pub description: String,
    pub source_operator: String,
    pub result_descriptor: TypedResultDescriptor,
}

impl From<ImportDataset> for Dataset {
    fn from(value: ImportDataset) -> Self {
        Dataset {
            id: DatasetId::Internal(InternalDatasetId::new()),
            name: value.name,
            description: value.description,
            result_descriptor: value.result_descriptor,
            source_operator: value.source_operator,
        }
    }
}

impl UserInput for ImportDataset {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetProviderListing {
    pub id: DatasetProviderId,
    pub name: String,
    pub description: String,
    // more meta data (number of datasets, ...)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AddDatasetProvider {
    AddMockDatasetProvider(AddMockDatasetProvider),
    // TODO: geo catalog, wcs, ...
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddMockDatasetProvider {
    pub datasets: Vec<Dataset>,
}

impl UserInput for AddDatasetProvider {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetProviderListOptions {
    // TODO: filter
    pub offset: u32,
    pub limit: u32,
}

impl UserInput for DatasetProviderListOptions {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}
#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DatasetDefinition {
    pub properties: AddDataset,
    pub meta_data: MetaDataDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateDataset {
    pub upload: UploadId,
    pub definition: DatasetDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
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

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SuggestMetaData {
    pub upload: UploadId,
    pub main_file: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MetaDataSuggestion {
    pub main_file: String,
    pub meta_data: MetaDataDefinition,
}

#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Deserialize, Serialize, Debug, Clone)]
pub enum MetaDataDefinition {
    MockMetaData(StaticMetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>),
    OgrMetaData(StaticMetaData<OgrSourceDataset, VectorResultDescriptor>),
    GdalMetaDataRegular(GdalMetaDataRegular),
    GdalStatic(GdalMetaDataStatic),
}

impl MetaDataDefinition {
    pub fn source_operator_type(&self) -> &str {
        match self {
            MetaDataDefinition::MockMetaData(_) => "MockDatasetDataSource",
            MetaDataDefinition::OgrMetaData(_) => "OgrSource",
            MetaDataDefinition::GdalMetaDataRegular(_) | MetaDataDefinition::GdalStatic(_) => {
                "GdalSource"
            }
        }
    }
}

/// Handling of datasets provided by geo engine internally, staged and by external providers
#[async_trait]
pub trait DatasetDb<S: Session>:
    DatasetStore<S> + DatasetProvider<S> + DatasetProviderDb<S> + UploadDb<S> + Send + Sync
{
}

/// Storage and access of external dataset providers
#[async_trait]
pub trait DatasetProviderDb<S: Session> {
    /// Add an external dataset `provider` by `user`
    // TODO: require special privilege to be able to add external dataset provider and to access external data in general
    async fn add_dataset_provider(
        &mut self,
        session: &S,
        provider: Validated<AddDatasetProvider>,
    ) -> Result<DatasetProviderId>;

    /// List available providers for `user` filtered by `options`
    async fn list_dataset_providers(
        &self,
        session: &S,
        options: Validated<DatasetProviderListOptions>,
    ) -> Result<Vec<DatasetProviderListing>>;

    /// Get dataset `provider` for `user`
    async fn dataset_provider(
        &self,
        session: &S,
        provider: DatasetProviderId,
    ) -> Result<&dyn DatasetProvider<S>>;
}

/// Defines the type of meta data a `DatasetDB` is able to store
pub trait DatasetStorer: Send + Sync {
    type StorageType: Send + Sync;
}

/// Allow storage of meta data of a particular storage type, e.g. `HashMapStorable` meta data for
/// `HashMapDatasetDB`
#[async_trait]
pub trait DatasetStore<S: Session>: DatasetStorer {
    async fn add_dataset(
        &mut self,
        session: &S,
        dataset: Validated<AddDataset>,
        meta_data: Self::StorageType,
    ) -> Result<DatasetId>;

    /// turn given `meta` data definition into the corresponding `StorageType` for the `DatasetStore`
    /// for use in the `add_dataset` method
    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType;
}
