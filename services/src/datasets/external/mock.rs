use std::collections::HashMap;

use crate::datasets::listing::ProvenanceOutput;
use crate::layers::external::{ExternalLayerProvider, ExternalLayerProviderDefinition};
use crate::layers::layer::{CollectionItem, Layer, LayerCollectionListOptions, LayerListing};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider, LayerId};
use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::DatasetListing,
        storage::{DatasetDefinition, MetaDataDefinition},
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, LayerProviderId};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MockExternalLayerProviderDefinition {
    pub id: LayerProviderId,
    pub datasets: Vec<DatasetDefinition>,
}

#[typetag::serde]
#[async_trait]
impl ExternalLayerProviderDefinition for MockExternalLayerProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn ExternalLayerProvider>> {
        Ok(Box::new(MockExternalDataProvider {
            id: self.id,
            datasets: self.datasets,
        }))
    }

    fn type_name(&self) -> String {
        "MockType".to_owned()
    }

    fn name(&self) -> String {
        "MockName".to_owned()
    }

    fn id(&self) -> LayerProviderId {
        self.id
    }
}

#[derive(Debug)]
pub struct MockExternalDataProvider {
    id: LayerProviderId,
    datasets: Vec<DatasetDefinition>,
}

// this provider uses dataset and layer ids interchangably
// TODO: remove this when external dataset ids are reworked
fn layer_id_from_dataset_id(id: &DatasetId) -> LayerId {
    match id {
        DatasetId::Internal { dataset_id } => LayerId(dataset_id.to_string()),
        DatasetId::External(s) => LayerId(s.dataset_id.clone()),
    }
}

#[async_trait]
impl ExternalLayerProvider for MockExternalDataProvider {
    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: None,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl LayerCollectionProvider for MockExternalDataProvider {
    async fn collection_items(
        &self,
        _collection: &LayerCollectionId,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        Ok(vec![]) // TODO: throw error instead?
    }

    async fn root_collection_items(
        &self,
        _options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        let mut listing = vec![];
        for dataset in &self.datasets {
            listing.push(Ok(CollectionItem::Layer(LayerListing {
                provider: self.id,
                layer: dataset
                    .properties
                    .id
                    .as_ref()
                    .ok_or(error::Error::MissingDatasetId)
                    .map(layer_id_from_dataset_id)?,
                name: dataset.properties.name.clone(),
                description: dataset.properties.description.clone(),
            })));
        }

        Ok(listing
            .into_iter()
            .filter_map(|d: Result<_>| if let Ok(d) = d { Some(d) } else { None })
            .collect())
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        self.datasets
            .iter()
            .find(|d| {
                d.properties
                    .id
                    .as_ref()
                    .map(layer_id_from_dataset_id)
                    .as_ref()
                    == Some(id)
            })
            .ok_or(error::Error::UnknownDatasetId)
            .map(|d| Layer {
                id: id.clone(),
                name: d.properties.name.clone(),
                description: d.properties.description.clone(),
                workflow: todo!(),
                symbology: d.properties.symbology.clone(),
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
        dataset: &DatasetId,
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
        let dataset_def = self
            .datasets
            .iter()
            .find(|d| d.properties.id.as_ref() == Some(dataset))
            .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::UnknownDatasetId),
            })?;

        if let MetaDataDefinition::MockMetaData(m) = &dataset_def.meta_data {
            Ok(Box::new(m.clone()))
        } else {
            Err(geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::DatasetIdTypeMissMatch),
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
        _dataset: &DatasetId,
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
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}
