use super::layer::{Layer, LayerCollection, LayerCollectionListOptions};
use crate::error::Error::NotImplemented;
use crate::error::Result;
use async_trait::async_trait;
use geoengine_datatypes::dataset::LayerId;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, IntoParams, ToSql, FromSql,
)]
#[into_params(names("layer"))]
pub struct LayerCollectionId(pub String);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, IntoParams)]
pub struct SearchCapabilities {
    pub(crate) search_types: SearchTypes,
    pub(crate) autocomplete: bool,
    pub(crate) filters: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, IntoParams)]
pub struct SearchTypes {
    pub(crate) fulltext: bool,
    pub(crate) prefix: bool,
}

#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, IntoParams, Validate,
)]
pub struct SearchParameters {
    pub(crate) search_type: String,
    pub(crate) search_string: String,
    pub(crate) limit: u32,
    pub(crate) offset: u32,
}

impl fmt::Display for LayerCollectionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[async_trait]
/// Listing of layers and layer collections
pub trait LayerCollectionProvider {
    /// Get a list of supported search capabilities
    async fn get_search_capabilities(&self) -> Result<SearchCapabilities> {
        Ok(SearchCapabilities {
            search_types: SearchTypes {
                fulltext: false,
                prefix: false,
            },
            autocomplete: false,
            filters: None,
        })
    }

    /// Perform a search
    #[allow(unused_variables)]
    async fn search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<LayerCollection> {
        Err(NotImplemented {
            message: "Layer search is not supported".to_string(),
        })
    }

    /// Perform search term autocomplete
    #[allow(unused_variables)]
    async fn autocomplete_search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<Vec<String>> {
        Err(NotImplemented {
            message: "Layer autocomplete is not supported".to_string(),
        })
    }

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

    /// Get a list of supported search capabilities
    async fn get_search_capabilities(&self) -> Result<SearchCapabilities> {
        Ok(SearchCapabilities {
            search_types: SearchTypes {
                fulltext: false,
                prefix: false,
            },
            autocomplete: false,
            filters: None,
        })
    }

    /// Perform a search
    #[allow(unused_variables)]
    async fn search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<LayerCollection> {
        Err(NotImplemented {
            message: "Dataset search is not supported".to_string(),
        })
    }

    /// Perform search term autocomplete
    #[allow(unused_variables)]
    async fn autocomplete_search(
        &self,
        collection_id: &LayerCollectionId,
        search: SearchParameters,
    ) -> Result<Vec<String>> {
        Err(NotImplemented {
            message: "Dataset autocomplete is not supported".to_string(),
        })
    }
}
