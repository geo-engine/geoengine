use std::fmt;

use crate::api::model::datatypes::LayerId;
use async_trait::async_trait;
use utoipa::ToSchema;

use crate::error::Result;

use super::layer::{Layer, LayerCollection, LayerCollectionListOptions};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema)]
#[cfg_attr(
    feature = "postgres",
    derive(postgres_types::ToSql, postgres_types::FromSql)
)]
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
    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection>;

    /// get the id of the root collection
    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId>;

    /// get the full contents of the layer with the given `id`
    async fn load_layer(&self, id: &LayerId) -> Result<Layer>;
}

#[async_trait]
/// Listing of layers and layer collections
pub trait DatasetLayerCollectionProvider {
    /// get the given `collection`
    async fn load_dataset_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection>;

    /// get the id of the root collection
    async fn get_dataset_root_layer_collection_id(&self) -> Result<LayerCollectionId>;

    /// get the full contents of the layer with the given `id`
    async fn load_dataset_layer(&self, id: &LayerId) -> Result<Layer>;
}
