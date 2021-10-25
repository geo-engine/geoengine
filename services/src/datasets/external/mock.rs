use std::collections::HashMap;

use crate::datasets::listing::ExternalDatasetProvider;
use crate::datasets::provenance::{ProvenanceOutput, ProvenanceProvider};
use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::DatasetListing,
        storage::{DatasetDefinition, ExternalDatasetProviderDefinition, MetaDataDefinition},
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor,
        VectorQueryRectangle, VectorResultDescriptor,
    },
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MockExternalDataProviderDefinition {
    pub id: DatasetProviderId,
    pub datasets: Vec<DatasetDefinition>,
}

#[typetag::serde]
#[async_trait]
impl ExternalDatasetProviderDefinition for MockExternalDataProviderDefinition {
    async fn initialize(self: Box<Self>) -> crate::error::Result<Box<dyn ExternalDatasetProvider>> {
        Ok(Box::new(MockExternalDataProvider {
            datasets: self.datasets,
        }))
    }

    fn type_name(&self) -> String {
        "MockType".to_owned()
    }

    fn name(&self) -> String {
        "MockName".to_owned()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

pub struct MockExternalDataProvider {
    datasets: Vec<DatasetDefinition>,
}

#[async_trait]
impl ExternalDatasetProvider for MockExternalDataProvider {
    async fn list(
        &self,
        _authorization: &HashMap<String, String>,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        // TODO: user right management
        // TODO: options
        let mut listing = vec![];
        for dataset in &self.datasets {
            listing.push(Ok(DatasetListing {
                id: dataset
                    .properties
                    .id
                    .clone()
                    .ok_or(error::Error::MissingDatasetId)?,
                name: dataset.properties.name.clone(),
                description: dataset.properties.description.clone(),
                tags: vec![],
                source_operator: dataset.properties.source_operator.clone(),
                result_descriptor: dataset.meta_data.result_descriptor().await?,
                symbology: dataset.properties.symbology.clone(),
            }));
        }

        Ok(listing
            .into_iter()
            .filter_map(|d: Result<DatasetListing>| if let Ok(d) = d { Some(d) } else { None })
            .collect())
    }

    async fn load(
        &self,
        _authorization: &HashMap<String, String>,
        _dataset: &geoengine_datatypes::dataset::DatasetId,
    ) -> crate::error::Result<crate::datasets::storage::Dataset> {
        Err(error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl ProvenanceProvider for MockExternalDataProvider {
    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: None,
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
