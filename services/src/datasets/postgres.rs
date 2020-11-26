use crate::datasets::listing::{DataSetListOptions, DataSetListing, DataSetProvider};
use crate::datasets::storage::{
    AddDataSet, AddDataSetProvider, DataSetDB, DataSetPermission, DataSetProviderListOptions,
    DataSetProviderListing, ImportDataSet,
};
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use futures::stream::BoxStream;
use geoengine_datatypes::collections::FeatureCollection;
use geoengine_datatypes::dataset::{DataSetId, DataSetProviderId};
use geoengine_datatypes::primitives::Geometry;
use geoengine_datatypes::raster::{Pixel, RasterTile2D};
use geoengine_operators::engine::LoadingInfo;

pub struct PostgresDataSetDB {}

#[async_trait]
impl DataSetDB for PostgresDataSetDB {
    async fn add(&mut self, _user: UserId, _data_set: Validated<AddDataSet>) -> Result<DataSetId> {
        todo!()
    }

    async fn import_raster_data<T: Pixel>(
        &mut self,
        _user: UserId,
        _data_set: Validated<ImportDataSet>,
        _stream: BoxStream<'_, Result<RasterTile2D<T>>>,
    ) -> Result<DataSetId> {
        todo!()
    }

    async fn import_vector_data<G: Geometry>(
        &mut self,
        _user: UserId,
        _data_set: Validated<ImportDataSet>,
        _stream: BoxStream<'_, Result<FeatureCollection<G>>>,
    ) -> Result<DataSetId> {
        todo!()
    }

    async fn add_data_set_permission(
        &mut self,
        _data_set: DataSetId,
        _user: UserId,
        _permission: DataSetPermission,
    ) -> Result<()> {
        todo!()
    }

    async fn add_data_set_provider(
        &mut self,
        _user: UserId,
        _provider: Validated<AddDataSetProvider>,
    ) -> Result<DataSetProviderId> {
        todo!()
    }

    async fn list_data_set_providers(
        &self,
        _user: UserId,
        _options: Validated<DataSetProviderListOptions>,
    ) -> Result<Vec<DataSetProviderListing>> {
        todo!()
    }

    async fn data_set_provider(
        &self,
        _user: UserId,
        _provider: DataSetProviderId,
    ) -> Result<&dyn DataSetProvider> {
        todo!()
    }
}

#[async_trait]
impl DataSetProvider for PostgresDataSetDB {
    async fn list(
        &self,
        _user: UserId,
        _options: Validated<DataSetListOptions>,
    ) -> Result<Vec<DataSetListing>> {
        todo!()
    }

    async fn loading_info(&self, _user: UserId, _data_set: DataSetId) -> Result<LoadingInfo> {
        todo!()
    }
}
