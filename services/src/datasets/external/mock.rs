use crate::users::user::UserId;
use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::{DatasetListing, DatasetProvider},
        storage::{DatasetDefinition, DatasetProviderDefinition, MetaDataDefinition},
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId};
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MockExternalDataProviderDefinition {
    id: DatasetProviderId,
    datasets: Vec<DatasetDefinition>,
}

#[typetag::serde]
impl DatasetProviderDefinition for MockExternalDataProviderDefinition {
    fn initialize(
        self: Box<Self>,
    ) -> crate::error::Result<Box<dyn crate::datasets::listing::DatasetProvider>> {
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
impl DatasetProvider for MockExternalDataProvider {
    async fn list(
        &self,
        _user: UserId,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        // TODO: user right management
        // TODO: options
        Ok(self
            .datasets
            .iter()
            .map(|d| {
                Ok(DatasetListing {
                    id: d
                        .properties
                        .id
                        .clone()
                        .ok_or(error::Error::MissingDatasetId)?,
                    name: d.properties.name.clone(),
                    description: d.properties.description.clone(),
                    tags: vec![],
                    source_operator: d.properties.source_operator.clone(),
                    result_descriptor: d.meta_data.result_descriptor()?,
                })
            })
            .filter_map(|d: Result<DatasetListing>| if let Ok(d) = d { Some(d) } else { None })
            .collect())
    }

    async fn load(
        &self,
        _user: crate::users::user::UserId,
        _dataset: &geoengine_datatypes::dataset::DatasetId,
    ) -> crate::error::Result<crate::datasets::storage::Dataset> {
        todo!()
    }
}

impl MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    for MockExternalDataProvider
{
    fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>,
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

impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for MockExternalDataProvider {
    fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        todo!()
    }
}

impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor> for MockExternalDataProvider {
    fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        todo!()
    }
}
