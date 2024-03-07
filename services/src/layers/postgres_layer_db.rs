use super::external::TypedDataProviderDefinition;
use super::listing::{ProviderCapabilities, SearchType};
use crate::contexts::PostgresDb;
use crate::error;
use crate::layers::layer::Property;
use crate::layers::listing::{SearchCapabilities, SearchParameters, SearchTypes};
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
use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::dataset::{DataProviderId, LayerId};
use geoengine_datatypes::util::HashMapTextTextDbType;
use snafu::ResultExt;
use std::str::FromStr;
use tokio_postgres::Transaction;
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

pub async fn insert_layer(
    trans: &Transaction<'_>,
    id: &LayerId,
    layer: AddLayer,
    collection: &LayerCollectionId,
) -> Result<Uuid> {
    let layer_id = Uuid::from_str(&id.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
        found: collection.0.clone(),
    })?;

    let collection_id =
        Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
            found: collection.0.clone(),
        })?;

    let workflow_id = WorkflowId::from_hash(&layer.workflow);

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

    Ok(layer_id)
}

pub async fn insert_layer_collection_with_id(
    trans: &Transaction<'_>,
    id: &LayerCollectionId,
    collection: AddLayerCollection,
    parent: &LayerCollectionId,
) -> Result<Uuid> {
    let collection_id =
        Uuid::from_str(&id.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
            found: id.0.clone(),
        })?;

    let parent =
        Uuid::from_str(&parent.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
            found: parent.0.clone(),
        })?;

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

    Ok(collection_id)
}

pub async fn insert_collection_parent<Tls>(
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    collection: &LayerCollectionId,
    parent: &LayerCollectionId,
) -> Result<()>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let collection =
        Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
            found: collection.0.clone(),
        })?;

    let parent =
        Uuid::from_str(&parent.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
            found: parent.0.clone(),
        })?;

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

pub async fn delete_layer_collection(
    transaction: &Transaction<'_>,
    collection: &LayerCollectionId,
) -> Result<()> {
    let collection =
        Uuid::from_str(&collection.0).map_err(|_| crate::error::Error::IdStringMustBeUuid {
            found: collection.0.clone(),
        })?;

    if collection == INTERNAL_LAYER_DB_ROOT_COLLECTION_ID {
        return Err(LayerDbError::CannotRemoveRootCollection.into());
    }

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

    _remove_collections_without_parent_collection(transaction).await?;

    _remove_layers_without_parent_collection(transaction).await?;

    Ok(())
}

pub async fn delete_layer_from_collection(
    transaction: &Transaction<'_>,
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

    _remove_layers_without_parent_collection(transaction).await?;

    Ok(())
}

pub async fn delete_layer_collection_from_parent(
    transaction: &Transaction<'_>,
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

    _remove_collections_without_parent_collection(transaction).await?;

    _remove_layers_without_parent_collection(transaction).await?;

    Ok(())
}

fn create_search_query(full_info: bool) -> String {
    format!("
        WITH RECURSIVE parents AS (
            SELECT $1::uuid as id
            UNION ALL SELECT DISTINCT child FROM collection_children JOIN parents ON (id = parent)
        )
        SELECT DISTINCT *
        FROM (
            SELECT 
                {}
            FROM layer_collections
                JOIN (SELECT DISTINCT child FROM collection_children JOIN parents ON (id = parent)) cc ON (id = cc.child)
            WHERE name ILIKE $4
        ) u UNION (
            SELECT 
                {}
            FROM layers uc
                JOIN (SELECT DISTINCT layer FROM collection_layers JOIN parents ON (collection = id)) cl ON (id = cl.layer)
            WHERE name ILIKE $4
        )
        ORDER BY {}name ASC
        LIMIT $2 
        OFFSET $3;",
        if full_info {
            "concat(id, '') AS id,
            name,
            description,
            properties,
            FALSE AS is_layer"
        } else { "name" },
        if full_info {
            "concat(id, '') AS id,
            name,
            description,
            properties,
            TRUE AS is_layer"
        } else { "name" },
        if full_info { "is_layer ASC," } else { "" })
}

#[async_trait]
impl<Tls> LayerDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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
        let mut conn = self.conn_pool.get().await?;

        let trans = conn.build_transaction().start().await?;

        insert_layer(&trans, id, layer, collection).await?;

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
        let mut conn = self.conn_pool.get().await?;

        let trans = conn.build_transaction().start().await?;

        insert_layer_collection_with_id(&trans, id, collection, parent).await?;

        trans.commit().await?;

        Ok(())
    }

    async fn add_collection_to_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let conn = self.conn_pool.get().await?;
        insert_collection_parent(&conn, collection, parent).await
    }

    async fn remove_layer_collection(&self, collection: &LayerCollectionId) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let transaction = conn.transaction().await?;

        delete_layer_collection(&transaction, collection).await?;

        transaction.commit().await.map_err(Into::into)
    }

    async fn remove_layer_from_collection(
        &self,
        layer: &LayerId,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let transaction = conn.transaction().await?;

        delete_layer_from_collection(&transaction, layer, collection).await?;

        transaction.commit().await.map_err(Into::into)
    }

    async fn remove_layer_collection_from_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let transaction = conn.transaction().await?;

        delete_layer_collection_from_parent(&transaction, collection, parent).await?;

        transaction.commit().await.map_err(Into::into)
    }
}

#[async_trait]
impl<Tls> LayerCollectionProvider for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities {
                search_types: SearchTypes {
                    fulltext: true,
                    prefix: true,
                },
                autocomplete: true,
                filters: None,
            },
        }
    }

    fn name(&self) -> &str {
        "Postgres Layer Database"
    }

    fn description(&self) -> &str {
        "A layer database using Postgres"
    }

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
        SELECT name, description, properties
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

    #[allow(clippy::too_many_lines)]
    async fn search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
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
        SELECT name, description, properties
        FROM layer_collections
        WHERE id = $1;",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&collection]).await?;

        let name: String = row.get(0);
        let description: String = row.get(1);
        let properties: Vec<Property> = row.get(2);

        let pattern = match search.search_type {
            SearchType::Fulltext => {
                format!("%{}%", search.search_string)
            }
            SearchType::Prefix => {
                format!("{}%", search.search_string)
            }
        };

        let stmt = conn.prepare(&create_search_query(true)).await?;

        let rows = conn
            .query(
                &stmt,
                &[
                    &collection,
                    &i64::from(search.limit),
                    &i64::from(search.offset),
                    &pattern,
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

    #[allow(clippy::too_many_lines)]
    async fn autocomplete_search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<Vec<String>> {
        let collection = Uuid::from_str(&collection_id.0).map_err(|_| {
            crate::error::Error::IdStringMustBeUuid {
                found: collection_id.0.clone(),
            }
        })?;

        let conn = self.conn_pool.get().await?;

        let pattern = match search.search_type {
            SearchType::Fulltext => {
                format!("%{}%", search.search_string)
            }
            SearchType::Prefix => {
                format!("{}%", search.search_string)
            }
        };

        let stmt = conn.prepare(&create_search_query(false)).await?;

        let rows = conn
            .query(
                &stmt,
                &[
                    &collection,
                    &i64::from(search.limit),
                    &i64::from(search.offset),
                    &pattern,
                ],
            )
            .await?;

        let items = rows
            .into_iter()
            .map(|row| Ok(row.get::<usize, &str>(0).to_string()))
            .collect::<Result<Vec<String>>>()?;

        Ok(items)
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_layer_provider(
        &self,
        provider: TypedDataProviderDefinition,
    ) -> Result<DataProviderId> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
              INSERT INTO layer_providers (
                  id, 
                  type_name, 
                  name,
                  definition,
                  priority
              )
              VALUES ($1, $2, $3, $4, $5)",
            )
            .await?;

        // clamp the priority to a reasonable range
        let prio = DataProviderDefinition::<Self>::priority(&provider);
        let clamp_prio = prio.clamp(-1000, 1000);

        if prio != clamp_prio {
            log::warn!(
                "The priority of the provider {} is out of range! --> clamped {} to {}",
                DataProviderDefinition::<Self>::name(&provider),
                prio,
                clamp_prio
            );
        }

        let id = DataProviderDefinition::<Self>::id(&provider);
        conn.execute(
            &stmt,
            &[
                &id,
                &DataProviderDefinition::<Self>::type_name(&provider),
                &DataProviderDefinition::<Self>::name(&provider),
                &provider,
                &clamp_prio,
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
                priority
            FROM 
                layer_providers
            WHERE
                priority > -1000
            ORDER BY priority DESC, name ASC
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
                priority: row.get(2),
            })
            .collect())
    }

    async fn load_layer_provider(&self, id: DataProviderId) -> Result<Box<dyn DataProvider>> {
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

        let definition: TypedDataProviderDefinition = row.get(0);

        Box::new(definition)
            .initialize(PostgresDb {
                conn_pool: self.conn_pool.clone(),
            })
            .await
    }
}
