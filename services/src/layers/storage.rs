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
use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::error::{Error, Result};
use crate::util::user_input::UserInput;
use crate::{contexts::Db, util::user_input::Validated};
use async_trait::async_trait;
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
    #[snafu(display("There is no layer collection with the given id {id}"))]
    NoLayerCollectionForGivenId { id: LayerCollectionId },
}

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

    // TODO: share/remove/update
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
                    })
                ],
                entry_label: None,
                properties: vec![],
            }
        );

        Ok(())
    }
}
