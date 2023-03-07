use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use super::add_from_directory::UNSORTED_COLLECTION_ID;
use super::external::{DataProvider, DataProviderDefinition};
use super::layer::{
    AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollection,
    LayerCollectionListOptions, LayerCollectionListing, LayerListing, ProviderLayerCollectionId,
    ProviderLayerId,
};
use super::listing::{LayerCollectionId, LayerCollectionProvider};
use super::LayerDbError;
use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::error::{Error, Result};
use crate::util::user_input::UserInput;
use crate::{contexts::Db, util::user_input::Validated};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, RwLockWriteGuard};
use uuid::Uuid;

pub const INTERNAL_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0xce5e_84db_cbf9_48a2_9a32_d4b7_cc56_ea74);

pub const INTERNAL_LAYER_DB_ROOT_COLLECTION_ID: Uuid =
    Uuid::from_u128(0x0510_2bb3_a855_4a37_8a8a_3002_6a91_fef1);

#[async_trait]
/// Storage for layers and layer collections
pub trait LayerDb: LayerCollectionProvider + Send + Sync {
    /// add new `layer` to the given `collection`
    async fn add_layer(
        &self,
        layer: Validated<AddLayer>,
        collection: &LayerCollectionId,
    ) -> Result<LayerId>;

    /// add new `layer` with fixed `id` to the given `collection`
    /// TODO: remove this method and allow stable names instead
    async fn add_layer_with_id(
        &self,
        id: &LayerId,
        layer: Validated<AddLayer>,
        collection: &LayerCollectionId,
    ) -> Result<()>;

    /// add existing `layer` to the given `collection`
    async fn add_layer_to_collection(
        &self,
        layer: &LayerId,
        collection: &LayerCollectionId,
    ) -> Result<()>;

    /// add new `collection` to the given `parent`
    // TODO: remove once stable names are available
    async fn add_collection(
        &self,
        collection: Validated<AddLayerCollection>,
        parent: &LayerCollectionId,
    ) -> Result<LayerCollectionId>;

    /// add new `collection` with fixex `id` to the given `parent`
    // TODO: remove once stable names are available
    async fn add_collection_with_id(
        &self,
        id: &LayerCollectionId,
        collection: Validated<AddLayerCollection>,
        parent: &LayerCollectionId,
    ) -> Result<()>;

    /// add existing `collection` to given `parent`
    async fn add_collection_to_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()>;

    /// Removes a collection from the database.
    ///
    /// Does not work on the root collection.
    ///
    /// Potentially removes sub-collections if they have no other parent.
    /// Potentially removes layers if they have no other parent.
    async fn remove_collection(&self, collection: &LayerCollectionId) -> Result<()>;

    /// Removes a collection from a parent collection.
    ///
    /// Does not work on the root collection.
    ///
    /// Potentially removes sub-collections if they have no other parent.
    /// Potentially removes layers if they have no other parent.
    async fn remove_collection_from_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()>;

    /// Removes a layer from a collection.
    async fn remove_layer_from_collection(
        &self,
        layer: &LayerId,
        collection: &LayerCollectionId,
    ) -> Result<()>;

    // TODO: share/update
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerProviderListing {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerProviderListingOptions {
    pub offset: u32,
    pub limit: u32,
}

impl UserInput for LayerProviderListingOptions {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[async_trait]
pub trait LayerProviderDb: Send + Sync + 'static {
    async fn add_layer_provider(
        &self,
        provider: Box<dyn DataProviderDefinition>,
    ) -> Result<DataProviderId>;

    async fn list_layer_providers(
        &self,
        options: Validated<LayerProviderListingOptions>,
    ) -> Result<Vec<LayerProviderListing>>;

    async fn layer_provider(&self, id: DataProviderId) -> Result<Box<dyn DataProvider>>;

    // TODO: share/remove/update layer providers
}

#[derive(Default, Debug)]
pub struct HashMapLayerDbBackend {
    layers: HashMap<LayerId, AddLayer>,
    collections: HashMap<LayerCollectionId, AddLayerCollection>,
    collection_children: HashMap<LayerCollectionId, Vec<LayerCollectionId>>,
    collection_layers: HashMap<LayerCollectionId, Vec<LayerId>>,
}

#[derive(Debug)]
pub struct HashMapLayerDb {
    backend: Db<HashMapLayerDbBackend>,
}

impl HashMapLayerDb {
    pub fn new() -> Self {
        let mut backend = HashMapLayerDbBackend::default();

        backend.collections.insert(
            LayerCollectionId(INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string()),
            AddLayerCollection {
                name: "LayerDB".to_string(),
                description: "Root collection for LayerDB".to_string(),
            },
        );

        backend.collections.insert(
            LayerCollectionId(UNSORTED_COLLECTION_ID.to_string()),
            AddLayerCollection {
                name: "Unsorted".to_string(),
                description: "Unsorted Layers".to_string(),
            },
        );

        backend.collection_children.insert(
            LayerCollectionId(INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string()),
            vec![LayerCollectionId(UNSORTED_COLLECTION_ID.to_string())],
        );

        Self {
            backend: Arc::new(RwLock::new(backend)),
        }
    }

    /// Remove collection and output a list of collection ids that need to be removed
    /// because they have no more parent
    fn _remove_collection(
        backend: &mut RwLockWriteGuard<'_, HashMapLayerDbBackend>,
        collection: &LayerCollectionId,
    ) -> Vec<LayerCollectionId> {
        backend.collections.remove(collection);

        // remove collection as sub-collection from other collections
        for children in backend.collection_children.values_mut() {
            children.retain(|c| c != collection);
        }

        let child_collections = backend
            .collection_children
            .remove(collection)
            .unwrap_or_default();

        // check if child collections have another parent
        // if not --> return them (avoids recursion)
        let mut child_collections_to_remove = Vec::new();

        for child_collection in child_collections {
            let has_parent = backend
                .collection_children
                .iter()
                .any(|(_, v)| v.contains(&child_collection));

            if !has_parent {
                child_collections_to_remove.push(child_collection);
            }
        }

        // check if child layers have another parent
        // if not --> remove them
        let child_layers = backend
            .collection_layers
            .remove(collection)
            .unwrap_or_default();
        for child_layer in child_layers {
            let has_parent = backend
                .collection_layers
                .iter()
                .any(|(_, v)| v.contains(&child_layer));

            if !has_parent {
                backend.layers.remove(&child_layer);
            }
        }

        child_collections_to_remove
    }
}

impl Default for HashMapLayerDb {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LayerDb for HashMapLayerDb {
    async fn add_layer(
        &self,
        layer: Validated<AddLayer>,
        collection: &LayerCollectionId,
    ) -> Result<LayerId> {
        let id = LayerId(uuid::Uuid::new_v4().to_string());

        let mut backend = self.backend.write().await;
        backend.layers.insert(id.clone(), layer.user_input);
        backend
            .collection_layers
            .entry(collection.clone())
            .or_default()
            .push(id.clone());
        Ok(id)
    }

    async fn add_layer_with_id(
        &self,
        id: &LayerId,
        layer: Validated<AddLayer>,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        let mut backend = self.backend.write().await;
        backend.layers.insert(id.clone(), layer.user_input);
        backend
            .collection_layers
            .entry(collection.clone())
            .or_default()
            .push(id.clone());
        Ok(())
    }

    async fn add_layer_to_collection(
        &self,
        layer: &LayerId,
        collection: &LayerCollectionId,
    ) -> Result<()> {
        let mut backend = self.backend.write().await;

        // check that layer exists
        if backend.layers.get(layer).is_none() {
            return Err(LayerDbError::NoLayerForGivenId { id: layer.clone() }.into());
        }

        let layers = backend
            .collection_layers
            .entry(collection.clone())
            .or_default();

        if !layers.contains(layer) {
            layers.push(layer.clone());
        }

        Ok(())
    }

    async fn add_collection(
        &self,
        collection: Validated<AddLayerCollection>,
        parent: &LayerCollectionId,
    ) -> Result<LayerCollectionId> {
        let id = LayerCollectionId(uuid::Uuid::new_v4().to_string());

        let mut backend = self.backend.write().await;
        backend
            .collections
            .insert(id.clone(), collection.user_input);
        backend
            .collection_children
            .entry(parent.clone())
            .or_default()
            .push(id.clone());

        Ok(id)
    }

    async fn add_collection_with_id(
        &self,
        id: &LayerCollectionId,
        collection: Validated<AddLayerCollection>,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let mut backend = self.backend.write().await;
        backend
            .collections
            .insert(id.clone(), collection.user_input);
        backend
            .collection_children
            .entry(parent.clone())
            .or_default()
            .push(id.clone());

        Ok(())
    }

    async fn add_collection_to_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let mut backend = self.backend.write().await;
        let children = backend
            .collection_children
            .entry(parent.clone())
            .or_default();

        if !children.contains(collection) {
            children.push(collection.clone());
        }

        Ok(())
    }

    async fn remove_collection(&self, collection: &LayerCollectionId) -> Result<()> {
        if collection == &LayerCollectionId(INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string()) {
            return Err(LayerDbError::CannotRemoveRootCollection.into());
        }

        let mut backend = self.backend.write().await;

        let mut layer_collections_to_remove = Self::_remove_collection(&mut backend, collection);
        while let Some(layer_collection_id) = layer_collections_to_remove.pop() {
            layer_collections_to_remove
                .extend(Self::_remove_collection(&mut backend, &layer_collection_id));
        }

        Ok(())
    }

    async fn remove_collection_from_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()> {
        let mut backend = self.backend.write().await;

        let children = backend
            .collection_children
            .get_mut(parent)
            .ok_or_else(|| LayerDbError::NoLayerCollectionForGivenId { id: parent.clone() })?;

        let length_before = children.len();
        children.retain(|c| c != collection);

        if length_before == children.len() {
            return Err(LayerDbError::NoLayerCollectionForGivenId {
                id: collection.clone(),
            }
            .into());
        }

        // check if collection is still a child of another collection

        let collection_must_be_removed = !backend
            .collection_children
            .iter()
            .any(|(_, v)| v.contains(collection));

        if collection_must_be_removed {
            let mut layer_collections_to_remove = vec![collection.clone()];
            while let Some(layer_collection_id) = layer_collections_to_remove.pop() {
                layer_collections_to_remove
                    .extend(Self::_remove_collection(&mut backend, &layer_collection_id));
            }
        }

        Ok(())
    }

    async fn remove_layer_from_collection(
        &self,
        layer_id: &LayerId,
        collection_id: &LayerCollectionId,
    ) -> Result<()> {
        let mut backend = self.backend.write().await;

        let collection = backend
            .collection_layers
            .get_mut(collection_id)
            .ok_or_else(|| LayerDbError::NoLayerCollectionForGivenId {
                id: collection_id.clone(),
            })?;

        let mut index = None;
        for (i, l) in collection.iter().enumerate() {
            if l == layer_id {
                index = Some(i);
                break;
            }
        }

        let Some(index) = index else {
            return Err(LayerDbError::NoLayerForGivenIdInCollection {
                layer: layer_id.clone(),
                collection: collection_id.clone(),
            }
            .into());
        };

        collection.remove(index);

        // check if layer is in any collection
        // if not --> remove entirely

        let layer_is_in_any_collection = backend
            .collection_layers
            .values()
            .any(|layers| layers.contains(layer_id));

        if !layer_is_in_any_collection {
            backend.layers.remove(layer_id);
        }

        Ok(())
    }
}

#[async_trait]
impl LayerCollectionProvider for HashMapLayerDb {
    async fn collection(
        &self,
        collection_id: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        let options = options.user_input;

        let backend = self.backend.read().await;

        let empty = vec![];

        let collection = backend.collections.get(collection_id).ok_or(
            LayerDbError::NoLayerCollectionForGivenId {
                id: collection_id.clone(),
            },
        )?;

        let collections = backend
            .collection_children
            .get(collection_id)
            .unwrap_or(&empty)
            .iter()
            .map(|c| {
                let collection = backend
                    .collections
                    .get(c)
                    .expect("collections reference existing collections as children");
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        collection_id: c.clone(),
                    },
                    name: collection.name.clone(),
                    description: collection.description.clone(),
                })
            });

        let empty = vec![];

        let layers = backend
            .collection_layers
            .get(collection_id)
            .unwrap_or(&empty)
            .iter()
            .map(|l| {
                let layer = backend
                    .layers
                    .get(l)
                    .expect("collections reference existing layers as items");

                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        layer_id: l.clone(),
                    },
                    name: layer.name.clone(),
                    description: layer.description.clone(),
                    properties: vec![],
                })
            });

        let mut items = collections
            .chain(layers)
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect::<Vec<_>>();

        items.sort_by(|a, b| match (a, b) {
            (CollectionItem::Collection(a), CollectionItem::Collection(b)) => a.name.cmp(&b.name),
            (CollectionItem::Layer(a), CollectionItem::Layer(b)) => a.name.cmp(&b.name),
            (CollectionItem::Collection(_), CollectionItem::Layer(_)) => Ordering::Less,
            (CollectionItem::Layer(_), CollectionItem::Collection(_)) => Ordering::Greater,
        });

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: INTERNAL_PROVIDER_ID,
                collection_id: collection_id.clone(),
            },
            name: collection.name.clone(),
            description: collection.description.clone(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(
            INTERNAL_LAYER_DB_ROOT_COLLECTION_ID.to_string(),
        ))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let backend = self.backend.read().await;

        let layer = backend
            .layers
            .get(id)
            .ok_or(LayerDbError::NoLayerForGivenId { id: id.clone() })?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: INTERNAL_PROVIDER_ID,
                layer_id: id.clone(),
            },
            name: layer.name.clone(),
            description: layer.description.clone(),
            workflow: layer.workflow.clone(),
            symbology: layer.symbology.clone(),
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}

#[derive(Default)]
pub struct HashMapLayerProviderDb {
    external_providers: Db<HashMap<DataProviderId, Box<dyn DataProviderDefinition>>>,
}

#[async_trait]
impl LayerProviderDb for HashMapLayerProviderDb {
    async fn add_layer_provider(
        &self,
        provider: Box<dyn DataProviderDefinition>,
    ) -> Result<DataProviderId> {
        let id = provider.id();

        self.external_providers.write().await.insert(id, provider);

        Ok(id)
    }

    async fn list_layer_providers(
        &self,
        options: Validated<LayerProviderListingOptions>,
    ) -> Result<Vec<LayerProviderListing>> {
        let options = options.user_input;

        let mut listing = self
            .external_providers
            .read()
            .await
            .iter()
            .map(|(id, provider)| LayerProviderListing {
                id: *id,
                name: provider.name(),
                description: provider.type_name().to_string(),
            })
            .collect::<Vec<_>>();

        // TODO: sort option
        listing.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(listing
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect())
    }

    async fn layer_provider(&self, id: DataProviderId) -> Result<Box<dyn DataProvider>> {
        self.external_providers
            .read()
            .await
            .get(&id)
            .cloned()
            .ok_or(Error::UnknownProviderId)?
            .initialize()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{util::user_input::UserInput, workflows::workflow::Workflow};
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_operators::{
        engine::{TypedOperator, VectorOperator},
        mock::{MockPointSource, MockPointSourceParams},
    };

    #[tokio::test]
    async fn it_stores_layers() -> Result<()> {
        let db = HashMapLayerDb::default();

        let layer = AddLayer {
            name: "layer".to_string(),
            description: "description".to_string(),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
        }
        .validated()?;

        let root_collection = &db.root_collection_id().await?;

        let l_id = db.add_layer(layer, root_collection).await?;

        let collection = AddLayerCollection {
            name: "top collection".to_string(),
            description: "description".to_string(),
        }
        .validated()?;

        let top_c_id = db.add_collection(collection, root_collection).await?;
        db.add_layer_to_collection(&l_id, &top_c_id).await?;

        let collection = AddLayerCollection {
            name: "empty collection".to_string(),
            description: "description".to_string(),
        }
        .validated()?;

        let empty_c_id = db.add_collection(collection, &top_c_id).await?;

        let items = db
            .collection(
                &top_c_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()?,
            )
            .await?;

        assert_eq!(
            items,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: top_c_id.clone(),
                },
                name: "top collection".to_string(),
                description: "description".to_string(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            collection_id: empty_c_id,
                        },
                        name: "empty collection".to_string(),
                        description: "description".to_string(),
                    }),
                    CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: l_id,
                        },
                        name: "layer".to_string(),
                        description: "description".to_string(),
                        properties: vec![],
                    })
                ],
                entry_label: None,
                properties: vec![],
            }
        );

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_collections() {
        let db = HashMapLayerDb::default();

        let layer = AddLayer {
            name: "layer".to_string(),
            description: "description".to_string(),
            workflow: Workflow {
                operator: TypedOperator::Vector(
                    MockPointSource {
                        params: MockPointSourceParams {
                            points: vec![Coordinate2D::new(1., 2.); 3],
                        },
                    }
                    .boxed(),
                ),
            },
            symbology: None,
        }
        .validated()
        .unwrap();

        let root_collection = &db.root_collection_id().await.unwrap();

        let collection = AddLayerCollection {
            name: "top collection".to_string(),
            description: "description".to_string(),
        }
        .validated()
        .unwrap();

        let top_c_id = db
            .add_collection(collection, root_collection)
            .await
            .unwrap();

        let l_id = db.add_layer(layer, &top_c_id).await.unwrap();

        let collection = AddLayerCollection {
            name: "empty collection".to_string(),
            description: "description".to_string(),
        }
        .validated()
        .unwrap();

        let empty_c_id = db.add_collection(collection, &top_c_id).await.unwrap();

        let items = db
            .collection(
                &top_c_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            items,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: top_c_id.clone(),
                },
                name: "top collection".to_string(),
                description: "description".to_string(),
                items: vec![
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            collection_id: empty_c_id.clone(),
                        },
                        name: "empty collection".to_string(),
                        description: "description".to_string(),
                    }),
                    CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: INTERNAL_PROVIDER_ID,
                            layer_id: l_id.clone(),
                        },
                        name: "layer".to_string(),
                        description: "description".to_string(),
                        properties: vec![],
                    })
                ],
                entry_label: None,
                properties: vec![],
            }
        );

        // remove empty collection
        db.remove_collection(&empty_c_id).await.unwrap();

        let items = db
            .collection(
                &top_c_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            items,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: INTERNAL_PROVIDER_ID,
                    collection_id: top_c_id.clone(),
                },
                name: "top collection".to_string(),
                description: "description".to_string(),
                items: vec![CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: INTERNAL_PROVIDER_ID,
                        layer_id: l_id.clone(),
                    },
                    name: "layer".to_string(),
                    description: "description".to_string(),
                    properties: vec![],
                })],
                entry_label: None,
                properties: vec![],
            }
        );

        // remove top (not root) collection
        db.remove_collection(&top_c_id).await.unwrap();

        db.collection(
            &top_c_id,
            LayerCollectionListOptions {
                offset: 0,
                limit: 20,
            }
            .validated()
            .unwrap(),
        )
        .await
        .unwrap_err();

        // should be deleted automatically
        db.get_layer(&l_id).await.unwrap_err();

        // it is not allowed to remove the root collection
        db.remove_collection(root_collection).await.unwrap_err();
        db.collection(
            root_collection,
            LayerCollectionListOptions {
                offset: 0,
                limit: 20,
            }
            .validated()
            .unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_collections_from_collections() {
        let db = HashMapLayerDb::default();

        let root_collection_id = &db.root_collection_id().await.unwrap();

        let mid_collection_id = db
            .add_collection(
                AddLayerCollection {
                    name: "mid collection".to_string(),
                    description: "description".to_string(),
                }
                .validated()
                .unwrap(),
                root_collection_id,
            )
            .await
            .unwrap();

        let bottom_collection_id = db
            .add_collection(
                AddLayerCollection {
                    name: "bottom collection".to_string(),
                    description: "description".to_string(),
                }
                .validated()
                .unwrap(),
                &mid_collection_id,
            )
            .await
            .unwrap();

        let layer_id = db
            .add_layer(
                AddLayer {
                    name: "layer".to_string(),
                    description: "description".to_string(),
                    workflow: Workflow {
                        operator: TypedOperator::Vector(
                            MockPointSource {
                                params: MockPointSourceParams {
                                    points: vec![Coordinate2D::new(1., 2.); 3],
                                },
                            }
                            .boxed(),
                        ),
                    },
                    symbology: None,
                }
                .validated()
                .unwrap(),
                &mid_collection_id,
            )
            .await
            .unwrap();

        // removing the mid collection…
        db.remove_collection_from_parent(&mid_collection_id, root_collection_id)
            .await
            .unwrap();

        // …should remove itself
        db.collection(
            &mid_collection_id,
            LayerCollectionListOptions::default().validated().unwrap(),
        )
        .await
        .unwrap_err();

        // …should remove the bottom collection
        db.collection(
            &bottom_collection_id,
            LayerCollectionListOptions::default().validated().unwrap(),
        )
        .await
        .unwrap_err();

        // … and should remove the layer of the bottom collection
        db.get_layer(&layer_id).await.unwrap_err();

        // the root collection is still there
        db.collection(
            root_collection_id,
            LayerCollectionListOptions::default().validated().unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_removes_layers_from_collections() {
        let db = HashMapLayerDb::default();

        let root_collection = &db.root_collection_id().await.unwrap();

        let another_collection = db
            .add_collection(
                AddLayerCollection {
                    name: "top collection".to_string(),
                    description: "description".to_string(),
                }
                .validated()
                .unwrap(),
                root_collection,
            )
            .await
            .unwrap();

        let layer_in_one_collection = db
            .add_layer(
                AddLayer {
                    name: "layer 1".to_string(),
                    description: "description".to_string(),
                    workflow: Workflow {
                        operator: TypedOperator::Vector(
                            MockPointSource {
                                params: MockPointSourceParams {
                                    points: vec![Coordinate2D::new(1., 2.); 3],
                                },
                            }
                            .boxed(),
                        ),
                    },
                    symbology: None,
                }
                .validated()
                .unwrap(),
                &another_collection,
            )
            .await
            .unwrap();

        let layer_in_two_collections = db
            .add_layer(
                AddLayer {
                    name: "layer 2".to_string(),
                    description: "description".to_string(),
                    workflow: Workflow {
                        operator: TypedOperator::Vector(
                            MockPointSource {
                                params: MockPointSourceParams {
                                    points: vec![Coordinate2D::new(1., 2.); 3],
                                },
                            }
                            .boxed(),
                        ),
                    },
                    symbology: None,
                }
                .validated()
                .unwrap(),
                &another_collection,
            )
            .await
            .unwrap();

        db.add_layer_to_collection(&layer_in_two_collections, root_collection)
            .await
            .unwrap();

        // remove first layer --> should be deleted entirely

        db.remove_layer_from_collection(&layer_in_one_collection, &another_collection)
            .await
            .unwrap();

        let number_of_layer_in_collection = db
            .collection(
                &another_collection,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap()
            .items
            .len();
        assert_eq!(
            number_of_layer_in_collection,
            1 /* only the other collection should be here */
        );

        db.get_layer(&layer_in_one_collection).await.unwrap_err();

        // remove second layer --> should only be gone in collection

        db.remove_layer_from_collection(&layer_in_two_collections, &another_collection)
            .await
            .unwrap();

        let number_of_layer_in_collection = db
            .collection(
                &another_collection,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap()
            .items
            .len();
        assert_eq!(
            number_of_layer_in_collection,
            0 /* both layers were deleted */
        );

        db.get_layer(&layer_in_two_collections).await.unwrap();
    }

    #[tokio::test]
    async fn test_missing_layer_dataset_in_collection_listing() {
        let db = HashMapLayerDb::default();

        let root_collection_id = &db.root_collection_id().await.unwrap();

        let top_collection_id = db
            .add_collection(
                AddLayerCollection {
                    name: "top collection".to_string(),
                    description: "description".to_string(),
                }
                .validated()
                .unwrap(),
                root_collection_id,
            )
            .await
            .unwrap();

        let faux_layer = LayerId("faux".to_string());

        // this should fail
        db.add_layer_to_collection(&faux_layer, &top_collection_id)
            .await
            .unwrap_err();

        let root_collection_layers = db
            .collection(
                &top_collection_id,
                LayerCollectionListOptions {
                    offset: 0,
                    limit: 20,
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            root_collection_layers,
            LayerCollection {
                id: ProviderLayerCollectionId {
                    provider_id: DataProviderId(
                        "ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74".try_into().unwrap()
                    ),
                    collection_id: top_collection_id.clone(),
                },
                name: "top collection".to_string(),
                description: "description".to_string(),
                items: vec![],
                entry_label: None,
                properties: vec![],
            }
        );
    }
}
