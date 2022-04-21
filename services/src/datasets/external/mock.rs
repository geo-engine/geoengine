use std::collections::HashMap;

use crate::datasets::listing::{ExternalDatasetProvider, ProvenanceOutput};
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
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_operators::engine::MetaDataLookupResult;
use geoengine_operators::{
    engine::{MetaData, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MockExternalDataProviderDefinition {
    pub id: DatasetProviderId,
    pub datasets: HashMap<DatasetId, DatasetDefinition>,
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

#[derive(Debug)]
pub struct MockExternalDataProvider {
    datasets: HashMap<DatasetId, DatasetDefinition>,
}

#[async_trait]
impl ExternalDatasetProvider for MockExternalDataProvider {
    async fn list(&self, _options: Validated<DatasetListOptions>) -> Result<Vec<DatasetListing>> {
        // TODO: user right management
        // TODO: options
        let mut listing = vec![];
        for (id, dataset) in &self.datasets {
            listing.push(Ok(DatasetListing {
                id: id.clone(),
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

    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: None,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn meta_data(&self, dataset: &DatasetId) -> Result<MetaDataLookupResult> {
        let (_, dataset_def) = self.datasets.iter().find(|(id, _)| *id == dataset).ok_or(
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::UnknownDatasetId),
            },
        )?;

        if let MetaDataDefinition::MockMetaData(m) = &dataset_def.meta_data {
            Ok(MetaDataLookupResult::Mock(Box::new(m.clone())))
        } else {
            Err(error::Error::DatasetIdTypeMissMatch)
        }
    }
}
