use std::collections::HashMap;

use crate::api::model::datatypes::{DataId, DataProviderId, LayerId};
use crate::datasets::listing::{Provenance, ProvenanceOutput};
use crate::error::Result;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerCollectionListing,
    LayerListing, Property, ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::projects::Symbology;
use crate::workflows::workflow::Workflow;
use crate::{datasets::storage::MetaDataDefinition, error, util::user_input::Validated};
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MockExternalLayerProviderDefinition {
    pub id: DataProviderId,
    pub root_collection: MockCollection,
    pub data: HashMap<String, MetaDataDefinition>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MockCollection {
    pub id: LayerCollectionId,
    pub name: String,
    pub description: String,
    pub collections: Vec<MockCollection>,
    pub layers: Vec<MockLayer>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MockLayer {
    pub id: LayerId,
    pub name: String,
    pub description: String,
    pub symbology: Option<Symbology>,
    pub provenance: Option<Provenance>,
    pub workflow: Workflow,
    pub properties: Vec<Property>,
    pub metadata: HashMap<String, String>,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for MockExternalLayerProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(MockExternalDataProvider {
            id: self.id,
            root_collection: self.root_collection,
            data: self.data,
        }))
    }

    fn type_name(&self) -> &'static str {
        "MockType"
    }

    fn name(&self) -> String {
        "MockName".to_owned()
    }

    fn id(&self) -> DataProviderId {
        self.id
    }
}

#[derive(Debug)]
pub struct MockExternalDataProvider {
    id: DataProviderId,
    root_collection: MockCollection,
    data: HashMap<String, MetaDataDefinition>,
}

impl MockExternalDataProvider {
    fn collection(&self, collection_id: &LayerCollectionId) -> Option<&MockCollection> {
        let mut queue = vec![&self.root_collection];

        while !queue.is_empty() {
            let current = queue.pop().unwrap();

            if &current.id == collection_id {
                return Some(current);
            }

            queue.extend(&current.collections);
        }

        None
    }

    fn layer(&self, layer_id: &LayerId) -> Option<&MockLayer> {
        let mut queue = vec![&self.root_collection];

        while !queue.is_empty() {
            let current = queue.pop().unwrap();

            for layer in &current.layers {
                if &layer.id == layer_id {
                    return Some(layer);
                }
            }

            queue.extend(&current.collections);
        }

        None
    }
}

#[async_trait]
impl DataProvider for MockExternalDataProvider {
    async fn provenance(&self, id: &DataId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            data: id.clone(),
            provenance: None,
        })
    }
}

#[async_trait]
impl LayerCollectionProvider for MockExternalDataProvider {
    async fn collection(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        let options = options.user_input;

        let collection =
            self.collection(collection)
                .ok_or(error::Error::UnknownLayerCollectionId {
                    id: collection.clone(),
                })?;

        let out = LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: self.id,
                collection_id: collection.id.clone(),
            },
            name: collection.name.clone(),
            description: collection.description.clone(),
            items: collection
                .collections
                .iter()
                .map(|c| {
                    CollectionItem::Collection(LayerCollectionListing {
                        id: ProviderLayerCollectionId {
                            provider_id: self.id,
                            collection_id: c.id.clone(),
                        },
                        name: c.name.clone(),
                        description: c.description.clone(),
                    })
                })
                .chain(collection.layers.iter().map(|l| {
                    CollectionItem::Layer(LayerListing {
                        id: ProviderLayerId {
                            provider_id: self.id,
                            layer_id: l.id.clone(),
                        },
                        name: l.name.clone(),
                        description: l.description.clone(),
                        properties: vec![],
                    })
                }))
                .skip(options.offset as usize)
                .take(options.limit as usize)
                .collect(),
            entry_label: None,
            properties: vec![],
        };

        Ok(out)
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(self.root_collection.id.to_string()))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let layer = self
            .layer(id)
            .ok_or(error::Error::UnknownLayerId { id: id.clone() })?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: self.id,
                layer_id: layer.id.clone(),
            },
            name: layer.name.clone(),
            description: layer.description.clone(),
            workflow: layer.workflow.clone(),
            symbology: layer.symbology.clone(),
            properties: layer.properties.clone(),
            metadata: layer.metadata.clone(),
        })
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for MockExternalDataProvider
{
    async fn meta_data(
        &self,
        id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
        geoengine_operators::error::Error,
    > {
        let id = match id {
            geoengine_datatypes::dataset::DataId::External(id) => id,
            geoengine_datatypes::dataset::DataId::Internal { dataset_id: _ } => {
                return Err(geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(error::Error::InvalidDataId),
                })
            }
        };

        let metadata = self.data.get(&id.layer_id.0).ok_or(
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(error::Error::UnknownDataId),
            },
        )?;

        if let MetaDataDefinition::MockMetaData(m) = metadata {
            Ok(Box::new(m.clone()))
        } else {
            Err(geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::DataIdTypeMissMatch),
            })
        }
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for MockExternalDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for MockExternalDataProvider
{
    async fn meta_data(
        &self,
        _id: &geoengine_datatypes::dataset::DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}
