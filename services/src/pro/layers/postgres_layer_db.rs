use bb8_postgres::{
    bb8::Pool,
    tokio_postgres::{
        tls::{MakeTlsConnect, TlsConnect},
        Socket,
    },
    PostgresConnectionManager,
};

use crate::{
    error::Result,
    layers::{
        layer::{
            AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollectionId,
            LayerCollectionListOptions, LayerId,
        },
        storage::LayerDb,
    },
    util::user_input::Validated,
};

pub struct PostgresLayerDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    _conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresLayerDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self {
            _conn_pool: conn_pool,
        }
    }
}

#[async_trait::async_trait]
impl<Tls> LayerDb for PostgresLayerDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_layer(&self, _layer: Validated<AddLayer>) -> Result<LayerId> {
        todo!()
    }
    async fn add_layer_with_id(&self, _id: LayerId, _layer: Validated<AddLayer>) -> Result<()> {
        todo!()
    }

    async fn get_layer(&self, _id: LayerId) -> Result<Layer> {
        todo!()
    }

    async fn add_layer_to_collection(
        &self,
        _layer: LayerId,
        _collection: LayerCollectionId,
    ) -> Result<()> {
        todo!()
    }

    async fn add_collection(
        &self,
        _collection: Validated<AddLayerCollection>,
    ) -> Result<LayerCollectionId> {
        todo!()
    }

    async fn add_collection_with_id(
        &self,
        _id: LayerCollectionId,
        _collection: Validated<AddLayerCollection>,
    ) -> Result<()> {
        todo!()
    }

    async fn add_collection_to_parent(
        &self,
        _collection: LayerCollectionId,
        _parent: LayerCollectionId,
    ) -> Result<()> {
        todo!()
    }

    async fn get_collection_items(
        &self,
        _collection: LayerCollectionId,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        todo!()
    }

    async fn get_root_collection_items(
        &self,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        todo!()
    }
}
