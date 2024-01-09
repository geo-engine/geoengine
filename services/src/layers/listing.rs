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
#[serde(rename_all = "camelCase")]
pub struct ProviderCapabilities {
    pub listing: bool,
    pub search: SearchCapabilities,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct SearchCapabilities {
    pub search_types: SearchTypes,
    pub autocomplete: bool,
    pub filters: Option<Vec<String>>,
}

impl SearchCapabilities {
    pub fn none() -> Self {
        Self {
            search_types: SearchTypes {
                fulltext: false,
                prefix: false,
            },
            autocomplete: false,
            filters: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct SearchTypes {
    pub fulltext: bool,
    pub prefix: bool,
}

#[derive(
    Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, ToSchema, IntoParams, Validate,
)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct SearchParameters {
    #[param(example = "fulltext")]
    pub search_type: SearchType,
    #[param(example = "test")]
    pub search_string: String,
    #[param(example = "20")]
    pub limit: u32,
    #[param(example = "0")]
    pub offset: u32,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum SearchType {
    Fulltext,
    Prefix,
}

impl fmt::Display for SearchType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SearchType::Fulltext => write!(f, "fulltext"),
            SearchType::Prefix => write!(f, "prefix"),
        }
    }
}

impl fmt::Display for LayerCollectionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[async_trait]
/// Listing of layers and layer collections
pub trait LayerCollectionProvider {
    /// Retrieve the provider's capabilities
    fn capabilities(&self) -> ProviderCapabilities;

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
