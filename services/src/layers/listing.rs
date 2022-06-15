use async_trait::async_trait;

use crate::util::user_input::Validated;
use crate::{error::Result, workflows::workflow::Workflow};

use super::layer::{CollectionItem, LayerCollectionId, LayerCollectionListOptions, LayerId};

#[async_trait]
pub trait LayerCollectionProvider {
    async fn collection_items(
        &self,
        collection: LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>>;

    async fn root_collection_items(
        &self,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>>;

    async fn workflow(&self, layer: LayerId) -> Result<Workflow>;
}
