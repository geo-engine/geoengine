use super::external::{DataProvider, TypedDataProviderDefinition};
use super::layer::{AddLayer, AddLayerCollection};
use super::listing::LayerCollectionId;
use crate::api::model::datatypes::{DataProviderId, LayerId};
use crate::error::Result;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const INTERNAL_PROVIDER_ID: DataProviderId =
    DataProviderId::from_u128(0xce5e_84db_cbf9_48a2_9a32_d4b7_cc56_ea74);

pub const INTERNAL_LAYER_DB_ROOT_COLLECTION_ID: Uuid =
    Uuid::from_u128(0x0510_2bb3_a855_4a37_8a8a_3002_6a91_fef1);

#[async_trait]
/// Storage for layers and layer collections
pub trait LayerDb: Send + Sync {
    /// add new `layer` to the given `collection`
    async fn add_layer(&self, layer: AddLayer, collection: &LayerCollectionId) -> Result<LayerId>;

    /// add new `layer` with fixed `id` to the given `collection`
    /// TODO: remove this method and allow stable names instead
    async fn add_layer_with_id(
        &self,
        id: &LayerId,
        layer: AddLayer,
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
    async fn add_layer_collection(
        &self,
        collection: AddLayerCollection,
        parent: &LayerCollectionId,
    ) -> Result<LayerCollectionId>;

    /// add new `collection` with fixex `id` to the given `parent`
    // TODO: remove once stable names are available
    async fn add_layer_collection_with_id(
        &self,
        id: &LayerCollectionId,
        collection: AddLayerCollection,
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
    async fn remove_layer_collection(&self, collection: &LayerCollectionId) -> Result<()>;

    /// Removes a collection from a parent collection.
    ///
    /// Does not work on the root collection.
    ///
    /// Potentially removes sub-collections if they have no other parent.
    /// Potentially removes layers if they have no other parent.
    async fn remove_layer_collection_from_parent(
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

    // TODO: update
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerProviderListing {
    pub id: DataProviderId,
    pub name: String,
    pub description: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
// TODO: validate user input
pub struct LayerProviderListingOptions {
    pub offset: u32,
    pub limit: u32,
}

#[async_trait]
pub trait LayerProviderDb: Send + Sync + 'static {
    async fn add_layer_provider(
        &self,
        provider: TypedDataProviderDefinition,
    ) -> Result<DataProviderId>;

    async fn list_layer_providers(
        &self,
        options: LayerProviderListingOptions,
    ) -> Result<Vec<LayerProviderListing>>;

    async fn load_layer_provider(&self, id: DataProviderId) -> Result<Box<dyn DataProvider>>;

    // TODO: share/remove/update layer providers
}
