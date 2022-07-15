use std::fmt;

use async_trait::async_trait;
use geoengine_datatypes::dataset::LayerId;

use crate::error::Result;
use crate::util::user_input::Validated;

use super::layer::{CollectionItem, Layer, LayerCollectionListOptions};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct LayerCollectionId(pub String);

impl fmt::Display for LayerCollectionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[async_trait]
/// Listing of layers and layer collections
pub trait LayerCollectionProvider {
    /// list all the items in the given `collection`
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>>;

    /// get the id of the root collection
    async fn root_collection_id(&self) -> Result<LayerCollectionId>;

    /// get the full contents of the layer with the given `id`
    async fn get_layer(&self, id: &LayerId) -> Result<Layer>;
}