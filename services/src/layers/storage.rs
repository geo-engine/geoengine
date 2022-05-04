use std::collections::HashMap;

use super::layer::{
    AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollectionId,
    LayerCollectionListOptions, LayerCollectionListing, LayerId,
};
use crate::error::Result;
use crate::{contexts::Db, util::user_input::Validated};
use async_trait::async_trait;
use geoengine_datatypes::util::Identifier;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(module(error), context(suffix(false)))] // disables default `Snafu` suffix
pub enum LayerDbError {
    #[snafu(display("There is no layer with the given id {id}"))]
    NoLayerForGivenId { id: LayerId },
}

#[async_trait]
pub trait LayerDb: Send + Sync {
    async fn add_layer(&self, layer: Validated<AddLayer>) -> Result<LayerId>;
    async fn add_layer_with_id(&self, id: LayerId, layer: Validated<AddLayer>) -> Result<()>;

    async fn get_layer(&self, id: LayerId) -> Result<Layer>;

    async fn add_layer_to_collection(
        &self,
        layer: LayerId,
        collection: LayerCollectionId,
    ) -> Result<()>;

    async fn add_collection(
        &self,
        collection: Validated<AddLayerCollection>,
    ) -> Result<LayerCollectionId>;

    async fn add_collection_with_id(
        &self,
        id: LayerCollectionId,
        collection: Validated<AddLayerCollection>,
    ) -> Result<()>;

    async fn add_collection_to_parent(
        &self,
        collection: LayerCollectionId,
        parent: LayerCollectionId,
    ) -> Result<()>;

    async fn get_collection_items(
        &self,
        collection: LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>>;

    // all collection items without a parent
    async fn get_root_collection_items(
        &self,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>>;
}

#[derive(Default, Debug)]
pub struct HashMapLayerDb {
    layers: Db<HashMap<LayerId, AddLayer>>,
    collections: Db<HashMap<LayerCollectionId, AddLayerCollection>>,
    collection_children: Db<HashMap<LayerCollectionId, Vec<LayerCollectionId>>>,
    collection_layers: Db<HashMap<LayerCollectionId, Vec<LayerId>>>,
}

#[async_trait]
impl LayerDb for HashMapLayerDb {
    async fn add_layer(&self, layer: Validated<AddLayer>) -> Result<LayerId> {
        let id = LayerId::new();
        self.layers.write().await.insert(id, layer.user_input);
        Ok(id)
    }

    async fn add_layer_with_id(&self, id: LayerId, layer: Validated<AddLayer>) -> Result<()> {
        self.layers.write().await.insert(id, layer.user_input);
        Ok(())
    }

    async fn get_layer(&self, id: LayerId) -> Result<Layer> {
        let layers = self.layers.read().await;

        let layer = layers
            .get(&id)
            .ok_or(LayerDbError::NoLayerForGivenId { id })?;

        Ok(Layer {
            id,
            name: layer.name.clone(),
            description: layer.description.clone(),
            workflow: layer.workflow,
        })
    }

    async fn add_layer_to_collection(
        &self,
        layer: LayerId,
        collection: LayerCollectionId,
    ) -> Result<()> {
        // TODO: check if layer is already in collection
        self.collection_layers
            .write()
            .await
            .entry(collection)
            .or_default()
            .push(layer);

        // TODO: add layers to hashmap

        Ok(())
    }

    async fn add_collection(
        &self,
        collection: Validated<AddLayerCollection>,
    ) -> Result<LayerCollectionId> {
        let id = LayerCollectionId::new();

        self.collections
            .write()
            .await
            .insert(id, collection.user_input);

        Ok(id)
    }

    async fn add_collection_with_id(
        &self,
        id: LayerCollectionId,
        collection: Validated<AddLayerCollection>,
    ) -> Result<()> {
        self.collections
            .write()
            .await
            .insert(id, collection.user_input);
        Ok(())
    }

    async fn add_collection_to_parent(
        &self,
        collection: LayerCollectionId,
        parent: LayerCollectionId,
    ) -> Result<()> {
        // TODO: check if collection is already in collection

        self.collection_children
            .write()
            .await
            .entry(parent)
            .or_default()
            .push(collection);

        Ok(())
    }

    async fn get_collection_items(
        &self,
        collection: LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let options = options.user_input;

        let collections = self.collections.read().await;
        let layers = self.layers.read().await;
        let collection_children = self.collection_children.read().await;
        let collection_layers = self.collection_layers.read().await;

        let empty = vec![];

        let collections = collection_children
            .get(&collection)
            .unwrap_or(&empty)
            .iter()
            .map(|c| {
                let collection = collections
                    .get(c)
                    .expect("collections reference existing collections as children");
                CollectionItem::Collection(LayerCollectionListing {
                    id: *c,
                    name: collection.name.clone(),
                    description: collection.description.clone(),
                })
            });

        let empty = vec![];

        let layers = collection_layers
            .get(&collection)
            .unwrap_or(&empty)
            .iter()
            .map(|l| {
                let layer = layers
                    .get(l)
                    .expect("collections reference existing layers as items");

                CollectionItem::Layer(Layer {
                    id: *l,
                    name: layer.name.clone(),
                    description: layer.description.clone(),
                    workflow: layer.workflow,
                })
            });

        Ok(collections
            .chain(layers)
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect())
    }

    async fn get_root_collection_items(
        &self,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let options = options.user_input;

        let collections = self.collections.read().await;
        let layers = self.layers.read().await;
        let collection_children = self.collection_children.read().await;
        let collection_layers = self.collection_layers.read().await;

        let collections = collections.iter().filter_map(|(id, c)| {
            if collection_children
                .values()
                .any(|collections| collections.contains(id))
            {
                return None;
            }

            Some(CollectionItem::Collection(LayerCollectionListing {
                id: *id,
                name: c.name.clone(),
                description: c.description.clone(),
            }))
        });

        let layers = layers.iter().filter_map(|(id, l)| {
            if collection_layers.values().any(|layers| layers.contains(id)) {
                return None;
            }

            Some(CollectionItem::Layer(Layer {
                id: *id,
                name: l.name.clone(),
                description: l.description.clone(),
                workflow: l.workflow,
            }))
        });

        Ok(collections
            .chain(layers)
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use crate::{util::user_input::UserInput, workflows::workflow::WorkflowId};

    use super::*;

    #[tokio::test]
    async fn it_stores_layers() -> Result<()> {
        let db = HashMapLayerDb::default();

        let workflow_id = WorkflowId::new();

        let layer = AddLayer {
            name: "layer".to_string(),
            description: "description".to_string(),
            workflow: workflow_id,
        }
        .validated()?;

        let l_id = db.add_layer(layer).await?;

        let collection = AddLayerCollection {
            name: "top collection".to_string(),
            description: "description".to_string(),
        }
        .validated()?;

        let top_c_id = db.add_collection(collection).await?;
        db.add_layer_to_collection(l_id, top_c_id).await?;

        let collection = AddLayerCollection {
            name: "empty collection".to_string(),
            description: "description".to_string(),
        }
        .validated()?;

        let empty_c_id = db.add_collection(collection).await?;

        db.add_collection_to_parent(empty_c_id, top_c_id).await?;

        let items = db
            .get_collection_items(
                top_c_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()?,
            )
            .await?;

        assert_eq!(
            items,
            vec![
                CollectionItem::Collection(LayerCollectionListing {
                    id: empty_c_id,
                    name: "empty collection".to_string(),
                    description: "description".to_string()
                }),
                CollectionItem::Layer(Layer {
                    id: l_id,
                    name: "layer".to_string(),
                    description: "description".to_string(),
                    workflow: workflow_id
                })
            ]
        );

        Ok(())
    }
}
