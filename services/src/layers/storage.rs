use std::collections::HashMap;
use std::sync::Arc;

use super::external::{ExternalLayerProvider, ExternalLayerProviderDefinition};
use super::layer::{
    AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollectionListOptions,
    LayerCollectionListing, LayerListing, ProviderLayerCollectionId, ProviderLayerId,
};
use super::listing::{LayerCollectionId, LayerCollectionProvider, LayerId};
use crate::error::{Error, Result};
use crate::util::user_input::UserInput;
use crate::{contexts::Db, util::user_input::Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::LayerProviderId;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(module(error), context(suffix(false)))] // disables default `Snafu` suffix
pub enum LayerDbError {
    #[snafu(display("There is no layer with the given id {id}"))]
    NoLayerForGivenId { id: LayerId },
}

pub const INTERNAL_PROVIDER_ID: LayerProviderId =
    LayerProviderId::from_u128(0xce5e_84db_cbf9_48a2_9a32_d4b7_cc56_ea74);

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

    // TODO: share/remove/update
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerProviderListing {
    pub id: LayerProviderId,
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
        provider: Box<dyn ExternalLayerProviderDefinition>,
    ) -> Result<LayerProviderId>;

    async fn list_layer_providers(
        &self,
        options: Validated<LayerProviderListingOptions>,
    ) -> Result<Vec<LayerProviderListing>>;

    async fn layer_provider(&self, id: LayerProviderId) -> Result<Box<dyn ExternalLayerProvider>>;

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

        Self {
            backend: Arc::new(RwLock::new(backend)),
        }
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
}

#[async_trait]
impl LayerCollectionProvider for HashMapLayerDb {
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let options = options.user_input;

        let backend = self.backend.read().await;

        let empty = vec![];

        let collections = backend
            .collection_children
            .get(collection)
            .unwrap_or(&empty)
            .iter()
            .map(|c| {
                let collection = backend
                    .collections
                    .get(c)
                    .expect("collections reference existing collections as children");
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider: INTERNAL_PROVIDER_ID,
                        item: c.clone(),
                    },
                    name: collection.name.clone(),
                    description: collection.description.clone(),
                })
            });

        let empty = vec![];

        let layers = backend
            .collection_layers
            .get(collection)
            .unwrap_or(&empty)
            .iter()
            .map(|l| {
                let layer = backend
                    .layers
                    .get(l)
                    .expect("collections reference existing layers as items");

                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider: INTERNAL_PROVIDER_ID,
                        item: l.clone(),
                    },
                    name: layer.name.clone(),
                    description: layer.description.clone(),
                })
            });

        Ok(collections
            .chain(layers)
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect())
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
                provider: INTERNAL_PROVIDER_ID,
                item: id.clone(),
            },
            name: layer.name.clone(),
            description: layer.description.clone(),
            workflow: layer.workflow.clone(),
            symbology: layer.symbology.clone(),
        })
    }
}

#[derive(Default)]
pub struct HashMapLayerProviderDb {
    external_providers: Db<HashMap<LayerProviderId, Box<dyn ExternalLayerProviderDefinition>>>,
}

#[async_trait]
impl LayerProviderDb for HashMapLayerProviderDb {
    async fn add_layer_provider(
        &self,
        provider: Box<dyn ExternalLayerProviderDefinition>,
    ) -> Result<LayerProviderId> {
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
                description: provider.type_name(),
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

    async fn layer_provider(&self, id: LayerProviderId) -> Result<Box<dyn ExternalLayerProvider>> {
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
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_operators::{
        engine::{TypedOperator, VectorOperator},
        mock::{MockPointSource, MockPointSourceParams},
    };

    use crate::{util::user_input::UserInput, workflows::workflow::Workflow};

    use super::*;

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
            .collection_items(
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
            vec![
                CollectionItem::Collection(LayerCollectionListing {
                    id: ProviderLayerCollectionId {
                        provider: INTERNAL_PROVIDER_ID,
                        item: empty_c_id,
                    },
                    name: "empty collection".to_string(),
                    description: "description".to_string()
                }),
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider: INTERNAL_PROVIDER_ID,
                        item: l_id,
                    },
                    name: "layer".to_string(),
                    description: "description".to_string(),
                })
            ]
        );

        Ok(())
    }
}
