use std::collections::HashMap;

use crate::datasets::listing::ProvenanceOutput;
use crate::error::Result;
use crate::layers::external::{DataProvider, DataProviderDefinition};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollectionListOptions, LayerListing, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::workflows::workflow::Workflow;
use crate::{
    datasets::storage::{DatasetDefinition, MetaDataDefinition},
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DataProviderId, LayerId};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::engine::{TypedOperator, VectorOperator};
use geoengine_operators::mock::{MockDatasetDataSource, MockDatasetDataSourceParams};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use uuid::Uuid;

pub const ROOT_COLLECTION_ID: Uuid = Uuid::from_u128(0xd630_e723_63d4_440c_9e15_644c_400f_c7c1);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MockExternalLayerProviderDefinition {
    pub id: DataProviderId,
    pub datasets: Vec<DatasetDefinition>,
}

#[typetag::serde]
#[async_trait]
impl DataProviderDefinition for MockExternalLayerProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn DataProvider>> {
        Ok(Box::new(MockExternalDataProvider {
            id: self.id,
            datasets: self.datasets,
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
    datasets: Vec<DatasetDefinition>,
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
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        ensure!(
            *collection == self.root_collection_id().await?,
            error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        // TODO: use options

        let mut listing = vec![];
        for dataset in &self.datasets {
            listing.push(Ok(CollectionItem::Layer(LayerListing {
                id: ProviderLayerId {
                    provider_id: self.id,
                    layer_id: dataset
                        .properties
                        .id
                        .as_ref()
                        .ok_or(error::Error::MissingDatasetId)
                        .map(|id| LayerId(id.to_string()))?,
                },
                name: dataset.properties.name.clone(),
                description: dataset.properties.description.clone(),
                properties: vec![],
            })));
        }

        Ok(listing
            .into_iter()
            .filter_map(|d: Result<_>| if let Ok(d) = d { Some(d) } else { None })
            .collect())
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(ROOT_COLLECTION_ID.to_string()))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        self.datasets
            .iter()
            .find(|d| {
                d.properties
                    .id
                    .as_ref()
                    .map(|id| LayerId(id.to_string()))
                    .as_ref()
                    == Some(id)
            })
            .ok_or(error::Error::UnknownDataId)
            .and_then(|d| {
                Ok(Layer {
                    id: ProviderLayerId {
                        provider_id: self.id,
                        layer_id: id.clone(),
                    },
                    name: d.properties.name.clone(),
                    description: d.properties.description.clone(),
                    workflow: Workflow {
                        operator: TypedOperator::Vector(
                            MockDatasetDataSource {
                                params: MockDatasetDataSourceParams {
                                    data: d
                                        .properties
                                        .id
                                        .ok_or(error::Error::MissingDatasetId)?
                                        .into(),
                                },
                            }
                            .boxed(),
                        ),
                    },
                    symbology: d.properties.symbology.clone(),
                    properties: vec![],
                    metadata: HashMap::new(),
                })
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
        id: &DataId,
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
        let dataset = id
            .internal()
            .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::DataIdTypeMissMatch),
            })?;
        let dataset_def = self
            .datasets
            .iter()
            .find(|d| d.properties.id.as_ref() == Some(&dataset))
            .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::UnknownDataId),
            })?;

        if let MetaDataDefinition::MockMetaData(m) = &dataset_def.meta_data {
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
        _id: &DataId,
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
        _id: &DataId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}
