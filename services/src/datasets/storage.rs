use crate::datasets::listing::{DataSetListing, DataSetProvider};
use crate::datasets::upload::UploadDb;
use crate::datasets::upload::UploadId;
use crate::error;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::user_input::{UserInput, Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId, InternalDataSetId};
use geoengine_datatypes::util::Identifier;
use geoengine_operators::{engine::StaticMetaData, source::OgrSourceDataset};
use geoengine_operators::{
    engine::TypedResultDescriptor, mock::MockDataSetDataSourceLoadingInfo,
    source::GdalMetaDataStatic,
};
use geoengine_operators::{engine::VectorResultDescriptor, source::GdalMetaDataRegular};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSet {
    pub id: DataSetId,
    pub name: String,
    pub description: String,
    pub result_descriptor: TypedResultDescriptor,
    pub source_operator: String,
}

impl DataSet {
    pub fn listing(&self) -> DataSetListing {
        DataSetListing {
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
pub struct AddDataSet {
    pub id: Option<DataSetId>,
    pub name: String,
    pub description: String,
    pub source_operator: String,
}

impl UserInput for AddDataSet {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImportDataSet {
    pub name: String,
    pub description: String,
    pub source_operator: String,
    pub result_descriptor: TypedResultDescriptor,
}

impl From<ImportDataSet> for DataSet {
    fn from(value: ImportDataSet) -> Self {
        DataSet {
            id: DataSetId::Internal(InternalDataSetId::new()),
            name: value.name,
            description: value.description,
            result_descriptor: value.result_descriptor,
            source_operator: value.source_operator,
        }
    }
}

impl UserInput for ImportDataSet {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSetProviderListing {
    pub id: DataSetProviderId,
    pub name: String,
    pub description: String,
    // more meta data (number of data sets, ...)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AddDataSetProvider {
    AddMockDataSetProvider(AddMockDataSetProvider),
    // TODO: geo catalog, wcs, ...
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddMockDataSetProvider {
    pub data_sets: Vec<DataSet>,
}

impl UserInput for AddDataSetProvider {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSetProviderListOptions {
    // TODO: filter
    pub offset: u32,
    pub limit: u32,
}

impl UserInput for DataSetProviderListOptions {
    fn validate(&self) -> Result<()> {
        todo!()
    }
}
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataSetDefinition {
    pub properties: AddDataSet,
    pub meta_data: MetaDataDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateDataSet {
    pub upload: UploadId,
    pub definition: DataSetDefinition,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AutoCreateDataSet {
    pub upload: UploadId,
    pub dataset_name: String,
    pub dataset_description: String,
    pub main_file: String,
}

impl UserInput for AutoCreateDataSet {
    fn validate(&self) -> Result<()> {
        // TODO: more sophisticated input validation
        ensure!(
            !self.main_file.contains('/') && !self.main_file.contains(".."),
            error::InvalidUploadFileName
        );

        Ok(())
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Deserialize, Serialize, Debug, Clone)]
pub enum MetaDataDefinition {
    MockMetaData(StaticMetaData<MockDataSetDataSourceLoadingInfo, VectorResultDescriptor>),
    OgrMetaData(StaticMetaData<OgrSourceDataset, VectorResultDescriptor>),
    GdalMetaDataRegular(GdalMetaDataRegular),
    GdalStatic(GdalMetaDataStatic),
}

/// Handling of data sets provided by geo engine internally, staged and by external providers
#[async_trait]
pub trait DataSetDb:
    DataSetStore + DataSetProvider + DataSetProviderDb + UploadDb + Send + Sync
{
}

/// Storage and access of external data set providers
#[async_trait]
pub trait DataSetProviderDb {
    /// Add an external data set `provider` by `user`
    // TODO: require special privilege to be able to add external data set provider and to access external data in general
    async fn add_data_set_provider(
        &mut self,
        user: UserId,
        provider: Validated<AddDataSetProvider>,
    ) -> Result<DataSetProviderId>;

    /// List available providers for `user` filtered by `options`
    async fn list_data_set_providers(
        &self,
        user: UserId,
        options: Validated<DataSetProviderListOptions>,
    ) -> Result<Vec<DataSetProviderListing>>;

    /// Get data set `provider` for `user`
    async fn data_set_provider(
        &self,
        user: UserId,
        provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider>;
}

/// Defines the type of meta data a `DataSetDB` is able to store
pub trait DataSetStorer: Send + Sync {
    type StorageType: Send + Sync;
}

/// Allow storage of meta data of a particular storage type, e.g. `HashMapStorable` meta data for
/// `HashMapDataSetDB`
#[async_trait]
pub trait DataSetStore: DataSetStorer {
    async fn add_data_set(
        &mut self,
        user: UserId,
        data_set: Validated<AddDataSet>,
        meta_data: Self::StorageType,
    ) -> Result<DataSetId>;

    /// turn given `meta` data definition into the corresponding `StorageType` for the `DataSetStore`
    /// for use in the `add_data_set` method
    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType;
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum DataSetPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDataSetPermission {
    pub user: UserId,
    pub data_set: InternalDataSetId,
    pub permission: DataSetPermission,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub enum DataSetProviderPermission {
    Read,
    Write,
    Owner,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Hash)]
pub struct UserDataSetProviderPermission {
    pub user: UserId,
    pub external_provider: DataSetProviderId,
    pub permission: DataSetProviderPermission,
}
