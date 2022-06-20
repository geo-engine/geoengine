use std::collections::HashMap;

use super::external::{ExternalLayerProvider, ExternalLayerProviderDefinition};
use super::layer::{
    AddLayer, AddLayerCollection, CollectionItem, Layer, LayerCollectionListOptions,
    LayerCollectionListing, LayerListing,
};
use super::listing::{LayerCollectionId, LayerCollectionProvider, LayerId};
use crate::error::Result;
use crate::util::user_input::UserInput;
use crate::workflows::workflow::Workflow;
use crate::{contexts::Db, util::user_input::Validated};
use async_trait::async_trait;
use geoengine_datatypes::dataset::LayerProviderId;
use geoengine_datatypes::identifier;
use geoengine_datatypes::util::Identifier;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(module(error), context(suffix(false)))] // disables default `Snafu` suffix
pub enum LayerDbError {
    #[snafu(display("There is no layer with the given id {id}"))]
    NoLayerForGivenId { id: LayerId },
}

pub const INTERNAL_PROVIDER_ID: LayerProviderId =
    LayerProviderId::from_u128(0xce5e_84db_cbf9_48a2_9a32_d4b7_cc56_ea74);

#[async_trait]
pub trait LayerDb: LayerCollectionProvider + Send + Sync {
    async fn add_layer(&self, layer: Validated<AddLayer>) -> Result<LayerId>;
    async fn add_layer_with_id(&self, id: &LayerId, layer: Validated<AddLayer>) -> Result<()>;

    async fn add_layer_to_collection(
        &self,
        layer: &LayerId,
        collection: &LayerCollectionId,
    ) -> Result<()>;

    async fn add_collection(
        &self,
        collection: Validated<AddLayerCollection>,
    ) -> Result<LayerCollectionId>;

    // TODO: remove once stable names are available
    async fn add_collection_with_id(
        &self,
        id: &LayerCollectionId,
        collection: Validated<AddLayerCollection>,
    ) -> Result<()>;

    async fn add_collection_to_parent(
        &self,
        collection: &LayerCollectionId,
        parent: &LayerCollectionId,
    ) -> Result<()>;

    // TODO: share/remove/update
}

pub struct LayerProviderListing {}

#[derive(Debug, Clone)]
pub struct LayerProviderListingOptions {}

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
    external_providers: Db<HashMap<LayerProviderId, Box<dyn ExternalLayerProviderDefinition>>>,
}

#[derive(Default, Debug)]
pub struct HashMapLayerDb {
    backend: Db<HashMapLayerDbBackend>,
}

#[async_trait]
impl LayerDb for HashMapLayerDb {
    async fn add_layer(&self, layer: Validated<AddLayer>) -> Result<LayerId> {
        let id = LayerId(uuid::Uuid::new_v4().to_string());
        self.backend
            .write()
            .await
            .layers
            .insert(id.clone(), layer.user_input);
        Ok(id)
    }

    async fn add_layer_with_id(&self, id: &LayerId, layer: Validated<AddLayer>) -> Result<()> {
        self.backend
            .write()
            .await
            .layers
            .insert(id.clone(), layer.user_input);
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
    ) -> Result<LayerCollectionId> {
        let id = LayerCollectionId(uuid::Uuid::new_v4().to_string());

        self.backend
            .write()
            .await
            .collections
            .insert(id.clone(), collection.user_input);

        Ok(id)
    }

    async fn add_collection_with_id(
        &self,
        id: &LayerCollectionId,
        collection: Validated<AddLayerCollection>,
    ) -> Result<()> {
        self.backend
            .write()
            .await
            .collections
            .insert(id.clone(), collection.user_input);
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
            .get(&collection)
            .unwrap_or(&empty)
            .iter()
            .map(|c| {
                let collection = backend
                    .collections
                    .get(c)
                    .expect("collections reference existing collections as children");
                CollectionItem::Collection(LayerCollectionListing {
                    id: c.clone(),
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

                CollectionItem::Layer(LayerListing {
                    provider: INTERNAL_PROVIDER_ID,
                    layer: l.clone(),
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

    async fn root_collection_items(
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
                id: id.clone(),
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

            Some(CollectionItem::Layer(LayerListing {
                provider: INTERNAL_PROVIDER_ID,
                layer: id.clone(),
                name: l.name.clone(),
                description: l.description.clone(),
            }))
        });

        Ok(collections
            .chain(layers)
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .collect())
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let backend = self.backend.read().await;

        let layer = backend
            .layers
            .get(&id)
            .ok_or(LayerDbError::NoLayerForGivenId { id: id.clone() })?;

        Ok(Layer {
            id: id.clone(),
            name: layer.name.clone(),
            description: layer.description.clone(),
            workflow: layer.workflow.clone(),
            symbology: layer.symbology.clone(),
        })
    }

    // async fn workflow(&self, layer: LayerId) -> Result<Workflow> {
    //     let backend = self.backend.read().await;

    //     let layer = backend
    //         .layers
    //         .get(&layer)
    //         .ok_or(LayerDbError::NoLayerForGivenId { id: layer })?;

    //     Ok(layer.workflow.clone())
    // }
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
        let id = LayerProviderId::new();

        self.external_providers.write().await.insert(id, provider);

        Ok(id)
    }

    async fn list_layer_providers(
        &self,
        options: Validated<LayerProviderListingOptions>,
    ) -> Result<Vec<LayerProviderListing>> {
        todo!()
    }

    async fn layer_provider(&self, id: LayerProviderId) -> Result<Box<dyn ExternalLayerProvider>> {
        todo!()
    }
}

// #[async_trait]
// impl LayerCollectionProvider for HashMapLayerDb {
//     async fn collection_items(
//         &self,
//         collection: LayerCollectionId,
//         options: Validated<LayerCollectionListOptions>,
//     ) -> Result<Vec<CollectionItem>> {
//         todo!()
//     }

//     async fn root_collection_items(
//         &self,
//         _options: Validated<LayerCollectionListOptions>,
//     ) -> Result<Vec<CollectionItem>> {
//         // TODO: use options

//         // on root level return one collection of every provider
//         let backend = self.backend.read().await;

//         let result = [CollectionItem::Collection(LayerCollectionListing {
//             id: INTERNAL_PROVIDER_ID.clone(),
//             name: "Internal".to_string(),
//             description: "Datasets managed by Geo Engine",
//         })]
//         .into_iter()
//         .chain(backend.iter().map(|(id, provider)| {
//             CollectionItem::Collection(LayerCollectionListing {
//                 id: id.clone(),
//                 name: provider.name(),
//                 description: provider.type_name(),
//             })
//         }))
//         .collect();

//         Ok(result)
//     }

//     async fn get_layer(&self, id: LayerId) -> Result<Layer> {
//         todo!()
//     }
// }

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_operators::{
        engine::{TypedOperator, VectorOperator},
        mock::{MockPointSource, MockPointSourceParams},
    };

    use crate::{util::user_input::UserInput, workflows::workflow::WorkflowId};

    use super::*;

    #[tokio::test]
    async fn it_stores_layers() -> Result<()> {
        let db = HashMapLayerDb::default();

        let _workflow_id = WorkflowId::new();

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

        let l_id = db.add_layer(layer).await?;

        let collection = AddLayerCollection {
            name: "top collection".to_string(),
            description: "description".to_string(),
        }
        .validated()?;

        let top_c_id = db.add_collection(collection).await?;
        db.add_layer_to_collection(&l_id, &top_c_id).await?;

        let collection = AddLayerCollection {
            name: "empty collection".to_string(),
            description: "description".to_string(),
        }
        .validated()?;

        let empty_c_id = db.add_collection(collection).await?;

        db.add_collection_to_parent(&empty_c_id, &top_c_id).await?;

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
                    id: empty_c_id,
                    name: "empty collection".to_string(),
                    description: "description".to_string()
                }),
                CollectionItem::Layer(LayerListing {
                    provider: INTERNAL_PROVIDER_ID,
                    layer: l_id,
                    name: "layer".to_string(),
                    description: "description".to_string(),
                })
            ]
        );

        Ok(())
    }
}
