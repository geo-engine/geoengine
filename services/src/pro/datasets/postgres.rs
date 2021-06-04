use crate::datasets::storage::{
    AddDataset, Dataset, DatasetDb, DatasetProviderDb, DatasetProviderDefinition,
    DatasetProviderListOptions, DatasetProviderListing, DatasetStore, DatasetStorer,
    MetaDataDefinition,
};
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::error::Result;
use crate::util::user_input::Validated;
use crate::{
    datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider},
    pro::users::UserSession,
};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId};
use geoengine_operators::engine::{MetaData, MetaDataProvider, ResultDescriptor};

// TODO: implement in separate PR, need placeholder here to satisfy bounds of `Context`
pub struct PostgresDatasetDb {}

impl DatasetDb<UserSession> for PostgresDatasetDb {}

#[async_trait]
impl DatasetProviderDb<UserSession> for PostgresDatasetDb {
    async fn add_dataset_provider(
        &mut self,
        _session: &UserSession,
        _provider: Box<dyn DatasetProviderDefinition<UserSession>>,
    ) -> Result<DatasetProviderId> {
        todo!()
    }

    async fn list_dataset_providers(
        &self,
        _session: &UserSession,
        _options: Validated<DatasetProviderListOptions>,
    ) -> Result<Vec<DatasetProviderListing>> {
        todo!()
    }

    async fn dataset_provider(
        &self,
        _session: &UserSession,
        _provider: DatasetProviderId,
    ) -> Result<Box<dyn DatasetProvider<UserSession>>> {
        todo!()
    }
}

#[async_trait]
impl DatasetProvider<UserSession> for PostgresDatasetDb {
    async fn list(
        &self,
        _session: &UserSession,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        todo!()
    }

    async fn load(&self, _session: &UserSession, _dataset: &DatasetId) -> Result<Dataset> {
        todo!()
    }
}

impl<L, R> MetaDataProvider<L, R> for PostgresDatasetDb
where
    R: ResultDescriptor,
{
    fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> std::result::Result<Box<dyn MetaData<L, R>>, geoengine_operators::error::Error> {
        todo!()
    }
}

impl DatasetStorer for PostgresDatasetDb {
    type StorageType = i32; // placeholder
}

#[async_trait]
impl DatasetStore<UserSession> for PostgresDatasetDb {
    async fn add_dataset(
        &mut self,
        _session: &UserSession,
        _dataset: Validated<AddDataset>,
        _meta_data: i32,
    ) -> Result<DatasetId> {
        todo!()
    }

    fn wrap_meta_data(&self, _meta: MetaDataDefinition) -> Self::StorageType {
        todo!()
    }
}

#[async_trait]
impl UploadDb<UserSession> for PostgresDatasetDb {
    async fn get_upload(&self, _session: &UserSession, _upload: UploadId) -> Result<Upload> {
        todo!()
    }

    async fn create_upload(&mut self, _session: &UserSession, _upload: Upload) -> Result<()> {
        todo!()
    }
}
