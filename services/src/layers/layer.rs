use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use geoengine_datatypes::dataset::{DataProviderId, LayerId};

use crate::{
    error::Result, projects::Symbology, util::user_input::UserInput, workflows::workflow::Workflow,
};

use super::listing::LayerCollectionId;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLayerId {
    pub provider_id: DataProviderId,
    pub layer_id: LayerId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLayerCollectionId {
    pub provider_id: DataProviderId,
    pub collection_id: LayerCollectionId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Layer {
    pub id: ProviderLayerId,
    pub name: String,
    pub description: String,
    pub workflow: Workflow,
    pub symbology: Option<Symbology>,
    pub properties: Vec<(String, String)>, // properties to be rendered in the UI
    pub metadata: HashMap<String, String>, // metadata used for loading the data
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LayerListing {
    pub id: ProviderLayerId,
    pub name: String,
    pub description: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddLayer {
    pub name: String,
    pub description: String,
    pub workflow: Workflow,
    pub symbology: Option<Symbology>,
}

impl UserInput for AddLayer {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LayerDefinition {
    pub id: LayerId,
    pub name: String,
    pub description: String,
    pub workflow: Workflow,
    pub symbology: Option<Symbology>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LayerCollection {
    id: LayerCollectionId,
    name: String,
    description: String,
    items: Vec<CollectionItem>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LayerCollectionListing {
    pub id: ProviderLayerCollectionId,
    pub name: String,
    pub description: String,
    pub entry_label: Option<String>, // a common label for the collection's entries, if there is any
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CollectionItem {
    Collection(LayerCollectionListing),
    Layer(LayerListing),
}

impl CollectionItem {
    pub fn name(&self) -> &str {
        match self {
            CollectionItem::Collection(c) => &c.name,
            CollectionItem::Layer(l) => &l.name,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddLayerCollection {
    pub name: String,
    pub description: String,
}

impl UserInput for AddLayerCollection {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LayerCollectionListOptions {
    pub offset: u32,
    pub limit: u32,
}

impl Default for LayerCollectionListOptions {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 20,
        }
    }
}

impl UserInput for LayerCollectionListOptions {
    fn validate(&self) -> Result<()> {
        // TODO
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LayerCollectionDefinition {
    pub id: LayerCollectionId,
    pub name: String,
    pub description: String,
    pub collections: Vec<LayerCollectionId>,
    pub layers: Vec<LayerId>,
}
