use super::layer::{Layer, LayerCollection, LayerCollectionListOptions};
use crate::error::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::LayerId;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::{IntoParams, ToSchema};

#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, IntoParams, ToSql, FromSql,
)]
#[into_params(names("layer"))]
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
