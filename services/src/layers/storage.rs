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
pub struct HashMapLayerDbBackend {
    layers: HashMap<LayerId, AddLayer>,
    collections: HashMap<LayerCollectionId, AddLayerCollection>,
    collection_children: HashMap<LayerCollectionId, Vec<LayerCollectionId>>,
    collection_layers: HashMap<LayerCollectionId, Vec<LayerId>>,
}

#[derive(Default, Debug)]
pub struct HashMapLayerDb {
    backend: Db<HashMapLayerDbBackend>,
}

#[async_trait]
impl LayerDb for HashMapLayerDb {
    async fn add_layer(&self, layer: Validated<AddLayer>) -> Result<LayerId> {
        let id = LayerId::new();
        self.backend
            .write()
            .await
            .layers
            .insert(id, layer.user_input);
        Ok(id)
    }

    async fn add_layer_with_id(&self, id: LayerId, layer: Validated<AddLayer>) -> Result<()> {
        self.backend
            .write()
            .await
            .layers
            .insert(id, layer.user_input);
        Ok(())
    }

    async fn get_layer(&self, id: LayerId) -> Result<Layer> {
        let backend = self.backend.read().await;

        let layer = backend
            .layers
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
        let mut backend = self.backend.write().await;
        let layers = backend.collection_layers.entry(collection).or_default();

        if !layers.contains(&layer) {
            layers.push(layer);
        }

        Ok(())
    }

    async fn add_collection(
        &self,
        collection: Validated<AddLayerCollection>,
    ) -> Result<LayerCollectionId> {
        let id = LayerCollectionId::new();

        self.backend
            .write()
            .await
            .collections
            .insert(id, collection.user_input);

        Ok(id)
    }

    async fn add_collection_with_id(
        &self,
        id: LayerCollectionId,
        collection: Validated<AddLayerCollection>,
    ) -> Result<()> {
        self.backend
            .write()
            .await
            .collections
            .insert(id, collection.user_input);
        Ok(())
    }

    async fn add_collection_to_parent(
        &self,
        collection: LayerCollectionId,
        parent: LayerCollectionId,
    ) -> Result<()> {
        let mut backend = self.backend.write().await;
        let children = backend.collection_children.entry(parent).or_default();

        if !children.contains(&collection) {
            children.push(collection);
        }

        Ok(())
    }

    async fn get_collection_items(
        &self,
        collection: LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let options = options.user_input;

        let backend = self.backend.read().await;

        let empty = vec![];

        let collections = backend
            .collection_children
            .get(&collection)
            .unwrap_or(&empty)
            .iter()
            .map(|c| {
                let collection = backend
                    .collections
                    .get(c)
                    .expect("collections reference existing collections as children");
                CollectionItem::Collection(LayerCollectionListing {
                    id: *c,
                    name: collection.name.clone(),
                    description: collection.description.clone(),
                })
            });

        let empty = vec![];

        let layers = backend
            .collection_layers
            .get(&collection)
            .unwrap_or(&empty)
            .iter()
            .map(|l| {
                let layer = backend
                    .layers
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

        let backend = self.backend.read().await;

        let collections = backend.collections.iter().filter_map(|(id, c)| {
            if backend
                .collection_children
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

        let layers = backend.layers.iter().filter_map(|(id, l)| {
            if backend
                .collection_layers
                .values()
                .any(|layers| layers.contains(id))
            {
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
