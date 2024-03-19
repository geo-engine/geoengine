use crate::{
    datasets::external::netcdfcf::{
        database::{
            deferred_write_transaction, get_connection, layer, layer_collection, loading_info,
            lock_overview, overviews_exist, readonly_transaction, remove_overviews,
            store_overview_metadata, unlock_overview, NetCdfDatabaseListingConfig,
        },
        loading::LayerCollectionIdFn,
        overviews::LoadingInfoMetadata,
        NetCdfCfProviderDb, NetCdfOverview, Result,
    },
    layers::layer::{Layer, LayerCollection},
};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataProviderId, LayerId};
use geoengine_operators::source::GdalMetaDataList;
use std::sync::Arc;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};

#[async_trait]
impl<Tls> NetCdfCfProviderDb for crate::pro::contexts::ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn lock_overview(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool> {
        lock_overview(get_connection!(self.conn_pool), provider_id, file_name).await
    }

    async fn unlock_overview(&self, provider_id: DataProviderId, file_name: &str) -> Result<()> {
        unlock_overview(get_connection!(self.conn_pool), provider_id, file_name).await
    }

    async fn overviews_exist(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool> {
        overviews_exist(get_connection!(self.conn_pool), provider_id, file_name).await
    }

    async fn store_overview_metadata(
        &self,
        provider_id: DataProviderId,
        metadata: &NetCdfOverview,
        loading_infos: Vec<LoadingInfoMetadata>,
    ) -> Result<()> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = deferred_write_transaction!(connection);

        store_overview_metadata(transaction, provider_id, metadata, loading_infos).await
    }

    async fn remove_overviews(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = deferred_write_transaction!(connection);

        remove_overviews(&transaction, provider_id, file_name).await
    }

    async fn layer_collection<ID: LayerCollectionIdFn>(
        &self,
        config: NetCdfDatabaseListingConfig<'_>,
        id_fn: Arc<ID>,
    ) -> Result<Option<LayerCollection>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction!(connection);

        layer_collection(transaction, config, id_fn).await
    }

    async fn layer(
        &self,
        provider_id: DataProviderId,
        layer_id: &LayerId,
        file_name: &str,
        group_path: &[String],
        entity_id: usize,
    ) -> Result<Option<Layer>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction!(connection);

        layer(
            transaction,
            provider_id,
            layer_id,
            file_name,
            group_path,
            entity_id,
        )
        .await
    }

    async fn loading_info(
        &self,
        provider_id: DataProviderId,
        file_name: &str,
        group_path: &[String],
        entity: usize,
    ) -> Result<Option<GdalMetaDataList>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction!(connection);

        loading_info(transaction, provider_id, file_name, group_path, entity).await
    }

    #[cfg(test)]
    async fn test_execute_with_transaction<T>(
        &self,
        statement: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<(), tokio_postgres::Error>
    where
        T: ?Sized + tokio_postgres::ToStatement + Sync,
    {
        let mut connection = self
            .conn_pool
            .get()
            .await
            .expect("failed to get connection in test");
        let transaction = connection.transaction().await?;

        transaction.execute(statement, params).await?;

        transaction.commit().await
    }
}
