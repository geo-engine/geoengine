use crate::workflows::registry::TxWorkflowRegistry;
use crate::{
    error::Result,
    layers::{
        layer::{AddLayer, AddLayerCollection},
        listing::LayerCollectionId,
        storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
        LayerDbError,
    },
};
use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::dataset::LayerId;
use geoengine_datatypes::util::HashMapTextTextDbType;
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
#[allow(clippy::used_underscore_items)] // TODO: maybe rename?
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

pub async fn insert_layer<W: TxWorkflowRegistry>(
    workflow_registry: &W,
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

    let workflow_id = workflow_registry
        .register_workflow_in_tx(layer.workflow, trans)
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

    #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
    _remove_collections_without_parent_collection(transaction).await?;

    #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
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

    #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
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

    #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
    _remove_collections_without_parent_collection(transaction).await?;

    #[allow(clippy::used_underscore_items)] // TODO: maybe rename?
    _remove_layers_without_parent_collection(transaction).await?;

    Ok(())
}
