use super::{
    storage::{Dataset, DatasetProviderDefinition},
    upload::{Upload, UploadDb, UploadId},
};
use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider};
use crate::datasets::storage::{
    AddDataset, DatasetDb, DatasetProviderDb, DatasetProviderListOptions, DatasetProviderListing,
    DatasetStore, DatasetStorer,
};
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId};
use geoengine_operators::engine::{MetaData, MetaDataProvider, ResultDescriptor};

// TODO: implement in separate PR, need placeholder here to satisfy bounds of `Context`
pub struct PostgresDatasetDb {}

impl DatasetDb for PostgresDatasetDb {}

#[async_trait]
impl DatasetProviderDb for PostgresDatasetDb {
    async fn add_dataset_provider(
        &mut self,
        _user: UserId,
        _provider: Box<dyn DatasetProviderDefinition>,
    ) -> Result<DatasetProviderId> {
        todo!()
    }

    async fn list_dataset_providers(
        &self,
        _user: UserId,
        _options: Validated<DatasetProviderListOptions>,
    ) -> Result<Vec<DatasetProviderListing>> {
        todo!()
    }

    async fn dataset_provider(
        &self,
        _user: UserId,
        _provider: DatasetProviderId,
    ) -> Result<Box<dyn DatasetProvider>> {
        todo!()
    }
}

#[async_trait]
impl DatasetProvider for PostgresDatasetDb {
    async fn list(
        &self,
        _user: UserId,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        todo!()
    }

    async fn load(&self, _user: UserId, _dataset: &DatasetId) -> Result<Dataset> {
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
impl DatasetStore for PostgresDatasetDb {
    async fn add_dataset(
        &mut self,
        _user: UserId,
        _dataset: Validated<AddDataset>,
        _meta_data: i32,
    ) -> Result<DatasetId> {
        todo!()
    }

    fn wrap_meta_data(&self, _meta: super::storage::MetaDataDefinition) -> Self::StorageType {
        todo!()
    }
}

#[async_trait]
impl UploadDb for PostgresDatasetDb {
    async fn get_upload(&self, _user: UserId, _upload: UploadId) -> Result<Upload> {
        todo!()
    }

    async fn create_upload(&mut self, _user: UserId, _upload: Upload) -> Result<()> {
        todo!()
    }
}
