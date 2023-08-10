use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::api::model::HashMapTextTextDbType;

use crate::contexts::PostgresDb;
use crate::error;
use crate::layers::layer::Property;

use crate::workflows::workflow::WorkflowId;
use crate::{
    error::Result,
    layers::{
        external::{DataProvider, DataProviderDefinition},
        layer::{
            AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollection,
            LayerCollectionListOptions, LayerCollectionListing, LayerListing,
            ProviderLayerCollectionId, ProviderLayerId,
        },
        listing::{LayerCollectionId, LayerCollectionProvider},
        storage::{
            LayerDb, LayerProviderDb, LayerProviderListing, LayerProviderListingOptions,
            INTERNAL_LAYER_DB_ROOT_COLLECTION_ID, INTERNAL_PROVIDER_ID,
        },
        LayerDbError,
    },
};
use async_trait::async_trait;
use bb8_postgres::tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};

use snafu::ResultExt;
use std::str::FromStr;
use uuid::Uuid;

/// delete all collections without parent collection
async fn _remove_collections_without_parent_collection(
    transaction: &tokio_postgres::Transaction<'_>,
) -> Result<()> {
    // HINT: a recursive delete statement seems reasonable, but hard to implement in postgres
    //       because you have a graph with potential loops

    let remove_layer_collections_without_parents_stmt = transaction
        .prepare(
            "DELETE FROM layer_collections
                 WHERE  id <> $1 -- do not delete root collection
                 AND    id NOT IN (
                    SELECT child FROM collection_children
                 );",
        )
        .await?;
    while 0 < transaction
        .execute(
            &remove_layer_collections_without_parents_stmt,
            &[&INTERNAL_LAYER_DB_ROOT_COLLECTION_ID],
        )
        .await?
    {
        // whenever one collection is deleted, we have to check again if there are more
        // collections without parents
    }

    Ok(())
}

/// delete all layers without parent collection
async fn _remove_layers_without_parent_collection(
    transaction: &tokio_postgres::Transaction<'_>,
) -> Result<()> {
    let remove_layers_without_parents_stmt = transaction
        .prepare(
            "DELETE FROM layers
                 WHERE id NOT IN (
                    SELECT layer FROM collection_layers
                 );",
        )
        .await?;
    transaction
        .execute(&remove_layers_without_parents_stmt, &[])
        .await?;

    Ok(())
}

#[async_trait]
impl<Tls> LayerDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_layer(&self, layer: AddLayer, collection: &LayerCollectionId) -> Result<LayerId> {
        let layer_id = Uuid::new_v4();
        let layer_id = LayerId(layer_id.to_string());

        self.add_layer_with_id(&layer_id, layer, collection).await?;

        Ok(layer_id)
    }

    async fn add_layer_with_id(
        &self,
        id: &LayerId,
        layer: AddLayer,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        let layer_id =
            Uuid::from_str(&id.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let collection_id =
            Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let mut conn = self.conn_pool.get().await?;

        let layer = layer;

        let workflow_id = WorkflowId::from_hash(&layer.workflow);

        let trans = conn.build_transaction().start().await?;

        let stmt = trans
            .prepare(
                "INSERT INTO workflows (id, workflow) VALUES ($1, $2) 
            ON CONFLICT DO NOTHING;",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &workflow_id,
                    &serde_json::to_value(&layer.workflow).context(error::SerdeJson)?,
                ],
            )
            .await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO layers (id, name, description, workflow_id, symbology, properties, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &layer_id,
                    &layer.name,
                    &layer.description,
                    &workflow_id,
                    &layer.symbology,
                    &layer.properties,
                    &HashMapTextTextDbType::from(&layer.metadata),
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
        let layer_id =
            Uuid::from_str(&layer.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: layer.0.clone(),
            })?;

        let collection_id =
            Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
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

    async fn add_layer_collection(
        &self,
        collection: AddLayerCollection,
        parent: &LayerCollectionId,
    ) -> Result<LayerCollectionId> {
        let collection_id = Uuid::new_v4();
        let collection_id = LayerCollectionId(collection_id.to_string());

        self.add_layer_collection_with_id(&collection_id, collection, parent)
            .await?;

        Ok(collection_id)
    }

    async fn add_layer_collection_with_id(
        &self,
        id: &LayerCollectionId,
        collection: AddLayerCollection,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let collection_id =
            Uuid::from_str(&id.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: id.0.clone(),
            })?;

        let parent =
            Uuid::from_str(&parent.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: parent.0.clone(),
            })?;

        let mut conn = self.conn_pool.get().await?;

        let trans = conn.build_transaction().start().await?;

        let stmt = trans
            .prepare(
                "
            INSERT INTO layer_collections (id, name, description, properties)
            VALUES ($1, $2, $3, $4);",
            )
            .await?;

        trans
            .execute(
                &stmt,
                &[
                    &collection_id,
                    &collection.name,
                    &collection.description,
                    &collection.properties,
                ],
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
            Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let parent =
            Uuid::from_str(&parent.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
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

    async fn remove_layer_collection(&self, collection: &LayerCollectionId) -> Result<()> {
        let collection =
            Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        if collection == INTERNAL_LAYER_DB_ROOT_COLLECTION_ID {
            return Err(LayerDbError::CannotRemoveRootCollection.into());
        }

        let mut conn = self.conn_pool.get().await?;
        let transaction = conn.transaction().await?;

        // delete the collection!
        // on delete cascade removes all entries from `collection_children` and `collection_layers`

        let remove_layer_collection_stmt = transaction
            .prepare(
                "DELETE FROM layer_collections
                 WHERE id = $1;",
            )
            .await?;
        transaction
            .execute(&remove_layer_collection_stmt, &[&collection])
            .await?;

        _remove_collections_without_parent_collection(&transaction).await?;

        _remove_layers_without_parent_collection(&transaction).await?;

        transaction.commit().await.map_err(Into::into)
    }

    async fn remove_layer_from_collection(
        &self,
        layer: &LayerId,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        let collection_uuid =
            Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let layer_uuid =
            Uuid::from_str(&layer.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: layer.0.clone(),
            })?;

        let mut conn = self.conn_pool.get().await?;
        let transaction = conn.transaction().await?;

        let remove_layer_collection_stmt = transaction
            .prepare(
                "DELETE FROM collection_layers
                 WHERE collection = $1
                 AND layer = $2;",
            )
            .await?;
        let num_results = transaction
            .execute(
                &remove_layer_collection_stmt,
                &[&collection_uuid, &layer_uuid],
            )
            .await?;

        if num_results == 0 {
            return Err(LayerDbError::NoLayerForGivenIdInCollection {
                layer: layer.clone(),
                collection: collection.clone(),
            }
            .into());
        }

        _remove_layers_without_parent_collection(&transaction).await?;

        transaction.commit().await.map_err(Into::into)
    }

    async fn remove_layer_collection_from_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let collection_uuid =
            Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let parent_collection_uuid =
            Uuid::from_str(&parent.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: parent.0.clone(),
            })?;

        let mut conn = self.conn_pool.get().await?;
        let transaction = conn.transaction().await?;

        let remove_layer_collection_stmt = transaction
            .prepare(
                "DELETE FROM collection_children
                 WHERE child = $1
                 AND parent = $2;",
            )
            .await?;
        let num_results = transaction
            .execute(
                &remove_layer_collection_stmt,
                &[&collection_uuid, &parent_collection_uuid],
            )
            .await?;

        if num_results == 0 {
            return Err(LayerDbError::NoCollectionForGivenIdInCollection {
                collection: collection.clone(),
                parent: parent.clone(),
            }
            .into());
        }

        _remove_collections_without_parent_collection(&transaction).await?;

        _remove_layers_without_parent_collection(&transaction).await?;

        transaction.commit().await.map_err(Into::into)
    }
}

#[async_trait]
impl<Tls> LayerCollectionProvider for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    #[allow(clippy::too_many_lines)]
    async fn load_layer_collection(
        &self,
        collection_id: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let collection = Uuid::from_str(&collection_id.0).map_err(|_| {
            crate::error::Error::IdStringMustBeUuid {
                found: collection_id.0.clone(),
            }
        })?;

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
        SELECT DISTINCT name, description, properties
        FROM layer_collections
        WHERE id = $1;",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&collection]).await?;

        let name: String = row.get(0);
        let description: String = row.get(1);
        let properties: Vec<Property> = row.get(2);

        let stmt = conn
            .prepare(
                "
        SELECT DISTINCT id, name, description, properties, is_layer
        FROM (
            SELECT 
                concat(id, '') AS id, 
                name, 
                description, 
                properties, 
                FALSE AS is_layer
            FROM layer_collections
                JOIN collection_children cc ON (id = cc.child)
            WHERE cc.parent = $1
        ) u UNION (
            SELECT 
                concat(id, '') AS id, 
                name, 
                description, 
                properties, 
                TRUE AS is_layer
            FROM layers uc
                JOIN collection_layers cl ON (id = cl.layer)
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

        let items = rows
            .into_iter()
            .map(|row| {
                let is_layer: bool = row.get(4);

                if is_layer {
                    Ok(CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: LayerId(row.get(0)),
                        },
                        name: row.get(1),
                        description: row.get(2),
                        properties: row.get(3),
                    }))
                } else {
                    Ok(CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            collection_id: LayerCollectionId(row.get(0)),
                        },
                        name: row.get(1),
                        description: row.get(2),
                        properties: row.get(3),
                    }))
                }
            })
            .collect::<Result<Vec<CollectionItem>>>()?;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: INTERNAL_PROVIDER_ID,
                collection_id: collection_id.clone(),
            },
            name,
            description,
            items,
            entry_label: None,
            properties,
        })
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(
            INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string(),
        ))
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        let layer_id =
            Uuid::from_str(&id.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
                found: id.0.clone(),
            })?;

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                l.name,
                l.description,
                w.workflow,
                l.symbology,
                l.properties,
                l.metadata
            FROM 
                layers l JOIN workflows w ON (l.workflow_id = w.id)
            WHERE 
                l.id = $1;",
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
            workflow: serde_json::from_value(row.get(2)).context(crate::error::SerdeJson)?,
            symbology: row.get(3),
            properties: row.get(4),
            metadata: row.get::<_, HashMapTextTextDbType>(5).into(),
        })
    }
}

#[async_trait]
impl<Tls> LayerProviderDb for PostgresDb<Tls>
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
        options: LayerProviderListingOptions,
    ) -> Result<Vec<LayerProviderListing>> {
        // TODO: permission
        let conn = self.conn_pool.get().await?;

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

    async fn load_layer_provider(&self, id: DataProviderId) -> Result<Box<dyn DataProvider>> {
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
