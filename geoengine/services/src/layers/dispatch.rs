use super::external::DataProvider;
use super::layer::{Layer, LayerCollection, LayerCollectionListOptions};
use super::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchParameters,
};
use super::storage::{INTERNAL_PROVIDER_ID, LayerProviderDb, PROJECTS_PROVIDER_ID};
use crate::error::Result;
use crate::projects::ProjectDb;
use crate::projects::layer_provider::ProjectsLayerProvider;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataProviderId, LayerId};

/// The data connector for a provider id: the internal layer database, the projects or an external
/// data connector.
pub enum LayerCollectionProviderRef<'a, D> {
    Internal(&'a D),
    Projects(ProjectsLayerProvider<'a, D>),
    External(Box<dyn DataProvider>),
}

/// Returns the data connector for `provider`.
pub async fn layer_collection_provider<D>(
    db: &D,
    provider: DataProviderId,
) -> Result<LayerCollectionProviderRef<'_, D>>
where
    D: LayerCollectionProvider + LayerProviderDb + ProjectDb,
{
    Ok(match provider {
        INTERNAL_PROVIDER_ID => LayerCollectionProviderRef::Internal(db),
        PROJECTS_PROVIDER_ID => LayerCollectionProviderRef::Projects(ProjectsLayerProvider(db)),
        provider => LayerCollectionProviderRef::External(db.load_layer_provider(provider).await?),
    })
}

/// Loads the layer `layer` of the data connector `provider`.
pub async fn load_layer<D>(db: &D, provider: DataProviderId, layer: &LayerId) -> Result<Layer>
where
    D: LayerCollectionProvider + LayerProviderDb + ProjectDb + Sync,
{
    layer_collection_provider(db, provider)
        .await?
        .load_layer(layer)
        .await
}

#[async_trait]
impl<D> LayerCollectionProvider for LayerCollectionProviderRef<'_, D>
where
    D: LayerCollectionProvider + ProjectDb + Sync,
{
    fn capabilities(&self) -> ProviderCapabilities {
        match self {
            Self::Internal(db) => db.capabilities(),
            Self::Projects(provider) => provider.capabilities(),
            Self::External(provider) => provider.capabilities(),
        }
    }

    fn name(&self) -> &str {
        match self {
            Self::Internal(db) => db.name(),
            Self::Projects(provider) => provider.name(),
            Self::External(provider) => provider.name(),
        }
    }

    fn description(&self) -> &str {
        match self {
            Self::Internal(db) => db.description(),
            Self::Projects(provider) => provider.description(),
            Self::External(provider) => provider.description(),
        }
    }

    async fn search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<LayerCollection> {
        match self {
            Self::Internal(db) => db.search(collection_id, search).await,
            Self::Projects(provider) => provider.search(collection_id, search).await,
            Self::External(provider) => provider.search(collection_id, search).await,
        }
    }

    async fn autocomplete_search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<Vec<String>> {
        match self {
            Self::Internal(db) => db.autocomplete_search(collection_id, search).await,
            Self::Projects(provider) => provider.autocomplete_search(collection_id, search).await,
            Self::External(provider) => provider.autocomplete_search(collection_id, search).await,
        }
    }

    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        match self {
            Self::Internal(db) => db.load_layer_collection(collection, options).await,
            Self::Projects(provider) => provider.load_layer_collection(collection, options).await,
            Self::External(provider) => provider.load_layer_collection(collection, options).await,
        }
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        match self {
            Self::Internal(db) => db.get_root_layer_collection_id().await,
            Self::Projects(provider) => provider.get_root_layer_collection_id().await,
            Self::External(provider) => provider.get_root_layer_collection_id().await,
        }
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        match self {
            Self::Internal(db) => db.load_layer(id).await,
            Self::Projects(provider) => provider.load_layer(id).await,
            Self::External(provider) => provider.load_layer(id).await,
        }
    }
}
