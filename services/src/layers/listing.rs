use std::fmt;

use crate::api::model::datatypes::LayerId;
use async_trait::async_trait;

use crate::error::Result;
use crate::util::user_input::Validated;

use super::layer::{Layer, LayerCollection, LayerCollectionListOptions};

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
    /// get the given `collection`
    async fn collection(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection>;

    /// get the id of the root collection
    async fn root_collection_id(&self) -> Result<LayerCollectionId>;

    /// get the full contents of the layer with the given `id`
    async fn get_layer(&self, id: &LayerId) -> Result<Layer>;
}
