use serde::{Deserialize, Serialize};

use geoengine_datatypes::identifier;

use crate::{
    error::Result,
    util::user_input::UserInput,
    workflows::workflow::{Workflow, WorkflowId},
};

identifier!(LayerId);
identifier!(LayerCollectionId);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Layer {
    pub id: LayerId,
    pub name: String,
    pub description: String,
    pub workflow: WorkflowId,
    // TODO: symbology
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddLayer {
    pub name: String,
    pub description: String,
    pub workflow: WorkflowId,
    // TODO: symbology
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LayerCollection {
    id: LayerCollectionId,
    name: String,
    description: String,
    items: Vec<CollectionItem>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LayerCollectionListing {
    pub id: LayerCollectionId,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CollectionItem {
    Collection(LayerCollectionListing),
    Layer(Layer),
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
