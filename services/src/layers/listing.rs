use std::fmt;

use async_trait::async_trait;
use geoengine_datatypes::dataset::DatasetId;

use crate::util::user_input::Validated;
use crate::{error::Result, workflows::workflow::Workflow};

use super::layer::{CollectionItem, Layer, LayerCollectionListOptions};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct LayerId(pub String);

impl fmt::Display for LayerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct LayerCollectionId(pub String);

impl fmt::Display for LayerCollectionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[async_trait]
pub trait LayerCollectionProvider {
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>>;

    async fn root_collection_items(
        &self,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>>;

    async fn get_layer(&self, id: &LayerId) -> Result<Layer>;
}
