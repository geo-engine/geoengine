use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::openapi::{ArrayBuilder, ObjectBuilder, SchemaType};
use utoipa::{IntoParams, ToSchema};

use crate::api::model::datatypes::{DataProviderId, LayerId};

use crate::{
    error::Result, projects::Symbology, util::user_input::UserInput, workflows::workflow::Workflow,
};

use super::listing::LayerCollectionId;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLayerId {
    pub provider_id: DataProviderId,
    pub layer_id: LayerId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLayerCollectionId {
    pub provider_id: DataProviderId,
    pub collection_id: LayerCollectionId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
pub struct Layer {
    pub id: ProviderLayerId,
    pub name: String,
    pub description: String,
    pub workflow: Workflow,
    pub symbology: Option<Symbology>,
    pub properties: Vec<Property>, // properties to be rendered in the UI
    pub metadata: HashMap<String, String>, // metadata used for loading the data
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
pub struct LayerListing {
    pub id: ProviderLayerId,
    pub name: String,
    pub description: String,
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LayerCollection {
    pub id: ProviderLayerCollectionId,
    pub name: String,
    pub description: String,
    pub items: Vec<CollectionItem>,
    pub entry_label: Option<String>, // a common label for the collection's entries, if there is any // TODO: separate labels for collections and layers?
    pub properties: Vec<Property>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Property((String, String));

impl From<(String, String)> for Property {
    fn from(v: (String, String)) -> Self {
        Self(v)
    }
}

// manual implementation because utoipa doesn't support tuples for now
impl ToSchema for Property {
    fn schema() -> utoipa::openapi::schema::Schema {
        ArrayBuilder::new()
            .items(ObjectBuilder::new().schema_type(SchemaType::String))
            .min_items(Some(2))
            .max_items(Some(2))
            .into()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LayerCollectionListing {
    pub id: ProviderLayerCollectionId,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
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

#[derive(Debug, Serialize, Deserialize, Clone, IntoParams)]
pub struct LayerCollectionListOptions {
    #[param(example = 0)]
    pub offset: u32,
    #[param(example = 20)]
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
