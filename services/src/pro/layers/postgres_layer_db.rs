use std::str::FromStr;

use async_trait::async_trait;
use bb8_postgres::{
    bb8::Pool,
    tokio_postgres::{
        tls::{MakeTlsConnect, TlsConnect},
        Socket,
    },
    PostgresConnectionManager,
};
use geoengine_datatypes::dataset::{DataProviderId, LayerId};
use snafu::ResultExt;
use uuid::Uuid;

use crate::{
    error::{self, Result},
    layers::{
        external::{DataProvider, DataProviderDefinition},
        layer::{
            AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollectionListOptions,
            LayerCollectionListing, LayerListing, ProviderLayerCollectionId, ProviderLayerId,
        },
        listing::{LayerCollectionId, LayerCollectionProvider},
        storage::{
            LayerDb, LayerDbError, LayerProviderDb, LayerProviderListing,
            LayerProviderListingOptions, INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
            INTERNAL_PROVIDER_ID,
        },
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
    async fn add_layer(
        &self,
        layer: Validated<AddLayer>,
        collection: &LayerCollectionId,
    ) -> Result<LayerId> {
        let collection_id =
            Uuid::from_str(&collection.0).map_err(|_| error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let mut conn = self.conn_pool.get().await?;

        let layer = layer.user_input;

        let layer_id = Uuid::new_v4();
        let symbology = serde_json::to_value(&layer.symbology).context(error::SerdeJson)?;

        let trans = conn.build_transaction().start().await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO layers (id, name, description, workflow, symbology)
            VALUES ($1, $2, $3, $4, $5);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &layer_id,
                    &layer.name,
                    &layer.description,
                    &serde_json::to_value(&layer.workflow).context(error::SerdeJson)?,
                    &symbology,
                ],
            )
            .await?;

        let stmt = trans
            .prepare(
                "
        INSERT INTO collection_layers (collection, layer)
        VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            )
            .await?;

        trans.execute(&stmt, &[&collection_id, &layer_id]).await?;

        trans.commit().await?;

        Ok(LayerId(layer_id.to_string()))
    }

    async fn add_layer_with_id(
        &self,
        id: &LayerId,
        layer: Validated<AddLayer>,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        let layer_id = Uuid::from_str(&id.0).map_err(|_| error::Error::IdStringMustBeUuid {
            found: collection.0.clone(),
        })?;

        let collection_id =
            Uuid::from_str(&collection.0).map_err(|_| error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let mut conn = self.conn_pool.get().await?;

        let layer = layer.user_input;

        let symbology = serde_json::to_value(&layer.symbology).context(error::SerdeJson)?;

        let trans = conn.build_transaction().start().await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO layers (id, name, description, workflow, symbology)
            VALUES ($1, $2, $3, $4, $5);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &layer_id,
                    &layer.name,
                    &layer.description,
                    &serde_json::to_value(&layer.workflow).context(error::SerdeJson)?,
                    &symbology,
                ],
            )
            .await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO collection_layers (collection, layer)
            VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            )
            .await?;

        trans.execute(&stmt, &[&collection_id, &layer_id]).await?;

        trans.commit().await?;

        Ok(())
    }

    async fn add_layer_to_collection(
        &self,
        layer: &LayerId,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        let layer_id = Uuid::from_str(&layer.0).map_err(|_| error::Error::IdStringMustBeUuid {
            found: layer.0.clone(),
        })?;

        let collection_id =
            Uuid::from_str(&collection.0).map_err(|_| error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            INSERT INTO collection_layers (collection, layer)
            VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            )
            .await?;

        conn.execute(&stmt, &[&collection_id, &layer_id]).await?;

        Ok(())
    }

    async fn add_collection(
        &self,
        collection: Validated<AddLayerCollection>,
        parent: &LayerCollectionId,
    ) -> Result<LayerCollectionId> {
        let parent = Uuid::from_str(&parent.0).map_err(|_| error::Error::IdStringMustBeUuid {
            found: parent.0.clone(),
        })?;

        let mut conn = self.conn_pool.get().await?;

        let collection = collection.user_input;

        let collection_id = Uuid::new_v4();

        let trans = conn.build_transaction().start().await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO layer_collections (id, name, description)
            VALUES ($1, $2, $3);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[&collection_id, &collection.name, &collection.description],
            )
            .await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO collection_children (parent, child)
            VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            )
            .await?;

        trans.execute(&stmt, &[&parent, &collection_id]).await?;

        trans.commit().await?;

        Ok(LayerCollectionId(collection_id.to_string()))
    }

    async fn add_collection_with_id(
        &self,
        id: &LayerCollectionId,
        collection: Validated<AddLayerCollection>,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let collection_id =
            Uuid::from_str(&id.0).map_err(|_| error::Error::IdStringMustBeUuid {
                found: id.0.clone(),
            })?;

        let parent = Uuid::from_str(&parent.0).map_err(|_| error::Error::IdStringMustBeUuid {
            found: parent.0.clone(),
        })?;

        let mut conn = self.conn_pool.get().await?;

        let collection = collection.user_input;

        let trans = conn.build_transaction().start().await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO layer_collections (id, name, description)
            VALUES ($1, $2, $3);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[&collection_id, &collection.name, &collection.description],
            )
            .await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO collection_children (parent, child)
            VALUES ($1, $2) ON CONFLICT DO NOTHING;",
            )
            .await?;

        trans.execute(&stmt, &[&parent, &collection_id]).await?;

        trans.commit().await?;

        Ok(())
    }

    async fn add_collection_to_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let collection =
            Uuid::from_str(&collection.0).map_err(|_| error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let parent = Uuid::from_str(&parent.0).map_err(|_| error::Error::IdStringMustBeUuid {
            found: parent.0.clone(),
        })?;

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
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let collection =
            Uuid::from_str(&collection.0).map_err(|_| error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let conn = self.conn_pool.get().await?;

        let options = options.user_input;

        let stmt = conn
            .prepare(
                "
        SELECT id, name, description, is_layer
        FROM (
            SELECT 
                concat(id, '') AS id, 
                name, 
                description, 
                FALSE AS is_layer
            FROM layer_collections c JOIN collection_children cc ON (c.id = cc.child)
            WHERE cc.parent = $1
        ) u UNION (
            SELECT 
                concat(id, '') AS id, 
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
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: LayerId(row.get(0)),
                        },
                        name: row.get(1),
                        description: row.get(2),
                        properties: None,
                    })
                } else {
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            collection_id: LayerCollectionId(row.get(0)),
                        },
                        name: row.get(1),
                        description: row.get(2),
                        entry_label: None,
                        properties: None,
                    })
                }
            })
            .collect())
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(
            INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string(),
        ))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let layer_id = Uuid::from_str(&id.0).map_err(|_| error::Error::IdStringMustBeUuid {
            found: id.0.clone(),
        })?;

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
            .query_one(&stmt, &[&layer_id])
            .await
            .map_err(|_error| LayerDbError::NoLayerForGivenId { id: id.clone() })?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: INTERNAL_PROVIDER_ID,
                layer_id: id.clone(),
            },
            name: row.get(0),
            description: row.get(1),
            workflow: serde_json::from_value(row.get(2)).context(error::SerdeJson)?,
            symbology: serde_json::from_value(row.get(3)).context(error::SerdeJson)?,
            properties: None,
        })
    }
}

pub struct PostgresLayerProviderDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub(crate) conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresLayerProviderDb<Tls>
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
impl<Tls> LayerProviderDb for PostgresLayerProviderDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_layer_provider(
        &self,
        provider: Box<dyn DataProviderDefinition>,
    ) -> Result<DataProviderId> {
        // TODO: permissions
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
              INSERT INTO layer_providers (
                  id, 
                  type_name, 
                  name,
                  definition
              )
              VALUES ($1, $2, $3, $4)",
            )
            .await?;

        let id = provider.id();
        conn.execute(
            &stmt,
            &[
                &id,
                &provider.type_name(),
                &provider.name(),
                &serde_json::to_value(provider)?,
            ],
        )
        .await?;
        Ok(id)
    }

    async fn list_layer_providers(
        &self,
        options: Validated<LayerProviderListingOptions>,
    ) -> Result<Vec<LayerProviderListing>> {
        // TODO: permission
        let conn = self.conn_pool.get().await?;

        let options = options.user_input;

        let stmt = conn
            .prepare(
                "
            SELECT 
                id, 
                name,
                type_name
            FROM 
                layer_providers
            ORDER BY name ASC
            LIMIT $1 
            OFFSET $2;",
            )
            .await?;

        let rows = conn
            .query(
                &stmt,
                &[&i64::from(options.limit), &i64::from(options.offset)],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| LayerProviderListing {
                id: row.get(0),
                name: row.get(1),
                description: row.get(2),
            })
            .collect())
    }

    async fn layer_provider(&self, id: DataProviderId) -> Result<Box<dyn DataProvider>> {
        // TODO: permissions
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
               SELECT 
                   definition
               FROM 
                   layer_providers
               WHERE
                   id = $1",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&id]).await?;

        let definition = serde_json::from_value::<Box<dyn DataProviderDefinition>>(row.get(0))?;

        definition.initialize().await
    }
}
