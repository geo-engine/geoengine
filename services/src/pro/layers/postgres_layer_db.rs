use async_trait::async_trait;
use bb8_postgres::{
    bb8::Pool,
    tokio_postgres::{
        tls::{MakeTlsConnect, TlsConnect},
        Socket,
    },
    PostgresConnectionManager,
};
use geoengine_datatypes::util::Identifier;
use snafu::ResultExt;

use crate::{
    error::{self, Result},
    layers::{
        layer::{
            AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollectionId,
            LayerCollectionListOptions, LayerCollectionListing, LayerId, LayerListing,
        },
        listing::LayerCollectionProvider,
        storage::{LayerDb, LayerDbError},
    },
    util::user_input::Validated,
    workflows::workflow::Workflow,
};

pub struct PostgresLayerDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub(crate) conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresLayerDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self { conn_pool }
    }
}

#[async_trait]
impl<Tls> LayerDb for PostgresLayerDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_layer(&self, layer: Validated<AddLayer>) -> Result<LayerId> {
        let conn = self.conn_pool.get().await?;

        let layer = layer.user_input;

        let id = LayerId::new();
        let symbology = serde_json::to_value(&layer.symbology).context(error::SerdeJson)?;

        let stmt = conn
            .prepare(
                "
            INSERT INTO layers (id, name, description, workflow, symbology)
            VALUES ($1, $2, $3, $4, $5);",
            )
            .await?;

        conn.execute(
            &stmt,
            &[
                &id,
                &layer.name,
                &layer.description,
                &serde_json::to_value(&layer.workflow).context(error::SerdeJson)?,
                &symbology,
            ],
        )
        .await?;

        Ok(id)
    }
    async fn add_layer_with_id(&self, id: LayerId, layer: Validated<AddLayer>) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let layer = layer.user_input;

        let symbology = serde_json::to_value(&layer.symbology).context(error::SerdeJson)?;

        let stmt = conn
            .prepare(
                "
            INSERT INTO layers (id, name, description, workflow, symbology)
            VALUES ($1, $2, $3, $4, $5);",
            )
            .await?;

        conn.execute(
            &stmt,
            &[
                &id,
                &layer.name,
                &layer.description,
                &serde_json::to_value(&layer.workflow).context(error::SerdeJson)?,
                &symbology,
            ],
        )
        .await?;

        Ok(())
    }

    async fn get_layer(&self, id: LayerId) -> Result<Layer> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                name,
                description,
                workflow,
                symbology         
            FROM layers l
            WHERE l.id = $1;",
            )
            .await?;

        let row = conn
            .query_one(&stmt, &[&id])
            .await
            .map_err(|_error| LayerDbError::NoLayerForGivenId { id })?;

        Ok(Layer {
            id,
            name: row.get(0),
            description: row.get(1),
            workflow: serde_json::from_value(row.get(2)).context(error::SerdeJson)?,
            symbology: serde_json::from_value(row.get(3)).context(error::SerdeJson)?,
        })
    }

    async fn add_layer_to_collection(
        &self,
        layer: LayerId,
        collection: LayerCollectionId,
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            INSERT INTO collection_layers (collection, layer)
            VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            )
            .await?;

        conn.execute(&stmt, &[&collection, &layer]).await?;

        Ok(())
    }

    async fn add_collection(
        &self,
        collection: Validated<AddLayerCollection>,
    ) -> Result<LayerCollectionId> {
        let conn = self.conn_pool.get().await?;

        let collection = collection.user_input;

        let id = LayerCollectionId::new();

        let stmt = conn
            .prepare(
                "
            INSERT INTO layer_collections (id, name, description)
            VALUES ($1, $2, $3);",
            )
            .await?;

        conn.execute(&stmt, &[&id, &collection.name, &collection.description])
            .await?;

        Ok(id)
    }

    async fn add_collection_with_id(
        &self,
        id: LayerCollectionId,
        collection: Validated<AddLayerCollection>,
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let collection = collection.user_input;

        let stmt = conn
            .prepare(
                "
            INSERT INTO layer_collections (id, name, description)
            VALUES ($1, $2, $3);",
            )
            .await?;

        conn.execute(&stmt, &[&id, &collection.name, &collection.description])
            .await?;

        Ok(())
    }

    async fn add_collection_to_parent(
        &self,
        collection: LayerCollectionId,
        parent: LayerCollectionId,
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            INSERT INTO collection_children (parent, child)
            VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            )
            .await?;

        conn.execute(&stmt, &[&parent, &collection]).await?;

        Ok(())
    }
}

#[async_trait]
impl<Tls> LayerCollectionProvider for PostgresLayerDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn collection_items(
        &self,
        collection: LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let conn = self.conn_pool.get().await?;

        let options = options.user_input;

        let stmt = conn
            .prepare(
                "
        SELECT id, name, description, is_layer
        FROM (
            SELECT 
                id, 
                name, 
                description, 
                FALSE AS is_layer
            FROM layer_collections c JOIN collection_children cc ON (c.id = cc.child)
            WHERE cc.parent = $1
        ) u UNION (
            SELECT 
                id, 
                name, 
                description, 
                TRUE As is_layer
            FROM layers l JOIN collection_layers cl ON (l.id = cl.layer)
            WHERE cl.collection = $1
        )
        ORDER BY is_layer ASC, name ASC
        LIMIT $2 
        OFFSET $3;            
        ",
            )
            .await?;

        let rows = conn
            .query(
                &stmt,
                &[
                    &collection,
                    &i64::from(options.limit),
                    &i64::from(options.offset),
                ],
            )
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let is_layer: bool = row.get(3);

                if is_layer {
                    CollectionItem::Layer(LayerListing {
                        id: row.get(0),
                        name: row.get(1),
                        description: row.get(2),
                    })
                } else {
                    CollectionItem::Collection(LayerCollectionListing {
                        id: row.get(0),
                        name: row.get(1),
                        description: row.get(2),
                    })
                }
            })
            .collect())
    }

    async fn root_collection_items(
        &self,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let conn = self.conn_pool.get().await?;

        let options = options.user_input;

        let stmt = conn
            .prepare(
                "
        SELECT id, name, description, is_layer
        FROM (
            SELECT 
                id, 
                name, 
                description, 
                FALSE AS is_layer
            FROM layer_collections c LEFT JOIN collection_children cc ON (c.id = cc.child)
            WHERE cc.parent IS NULL
        ) a UNION (
            SELECT 
                id, 
                name, 
                description, 
                TRUE AS is_layer
            FROM layers l LEFT JOIN collection_layers cl ON (l.id = cl.layer)
            WHERE cl.collection IS NULL
        )
        ORDER BY is_layer ASC, name ASC
        LIMIT $1 
        OFFSET $2;            
        ",
            )
            .await?;

        let rows = conn
            .query(
                &stmt,
                &[&i64::from(options.limit), &i64::from(options.offset)],
            )
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let is_layer: bool = row.get(3);

                if is_layer {
                    CollectionItem::Layer(LayerListing {
                        id: row.get(0),
                        name: row.get(1),
                        description: row.get(2),
                    })
                } else {
                    CollectionItem::Collection(LayerCollectionListing {
                        id: row.get(0),
                        name: row.get(1),
                        description: row.get(2),
                    })
                }
            })
            .collect())
    }

    async fn workflow(&self, layer: LayerId) -> Result<Workflow> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                workflow,     
            FROM layers l
            WHERE l.id = $1;",
            )
            .await?;

        let row = conn
            .query_one(&stmt, &[&layer])
            .await
            .map_err(|_error| LayerDbError::NoLayerForGivenId { id: layer })?;

        Ok(serde_json::from_value(row.get(0)).context(error::SerdeJson)?)
    }
}
