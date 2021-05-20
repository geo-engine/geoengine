use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider, OrderBy};
use crate::datasets::storage::{
    AddDataset, AddDatasetProvider, Dataset, DatasetDb, DatasetProviderDb,
    DatasetProviderListOptions, DatasetProviderListing, DatasetStore, DatasetStorer,
};
use crate::error;
use crate::error::Result;
use crate::users::user::UserId;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DatasetId, DatasetProviderId, InternalDatasetId},
    util::Identifier,
};
use geoengine_operators::source::{GdalLoadingInfo, GdalMetaDataRegular, OgrSourceDataset};
use geoengine_operators::{
    engine::{
        MetaData, MetaDataProvider, RasterResultDescriptor, StaticMetaData, TypedResultDescriptor,
        VectorResultDescriptor,
    },
    source::GdalStacTilesReading,
};
use geoengine_operators::{mock::MockDatasetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use std::collections::HashMap;

use super::{
    storage::MetaDataDefinition,
    upload::{Upload, UploadDb, UploadId},
};

#[derive(Default)]
pub struct HashMapDatasetDb {
    datasets: Vec<Dataset>,
    ogr_datasets:
        HashMap<InternalDatasetId, StaticMetaData<OgrSourceDataset, VectorResultDescriptor>>,
    mock_datasets: HashMap<
        InternalDatasetId,
        StaticMetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>,
    >,
    gdal_datasets:
        HashMap<InternalDatasetId, Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>>,
    uploads: HashMap<UploadId, Upload>,
}

impl DatasetDb for HashMapDatasetDb {}

#[async_trait]
impl DatasetProviderDb for HashMapDatasetDb {
    async fn add_dataset_provider(
        &mut self,
        _user: UserId,
        _provider: Validated<AddDatasetProvider>,
    ) -> Result<DatasetProviderId> {
        todo!()
    }

    async fn list_dataset_providers(
        &self,
        _user: UserId,
        _options: Validated<DatasetProviderListOptions>,
    ) -> Result<Vec<DatasetProviderListing>> {
        todo!()
    }

    async fn dataset_provider(
        &self,
        _user: UserId,
        _provider: DatasetProviderId,
    ) -> Result<&dyn DatasetProvider> {
        todo!()
    }
}

pub trait HashMapStorable: Send + Sync {
    fn store(&self, id: InternalDatasetId, db: &mut HashMapDatasetDb) -> TypedResultDescriptor;
}

impl DatasetStorer for HashMapDatasetDb {
    type StorageType = Box<dyn HashMapStorable>;
}

impl HashMapStorable for MetaDataDefinition {
    fn store(&self, id: InternalDatasetId, db: &mut HashMapDatasetDb) -> TypedResultDescriptor {
        match self {
            MetaDataDefinition::MockMetaData(d) => d.store(id, db),
            MetaDataDefinition::OgrMetaData(d) => d.store(id, db),
            MetaDataDefinition::GdalMetaDataRegular(d) => d.store(id, db),
            MetaDataDefinition::GdalStatic(d) => d.store(id, db),
            MetaDataDefinition::GdalStacTiles(d) => d.store(id, db),
        }
    }
}

impl HashMapStorable for StaticMetaData<OgrSourceDataset, VectorResultDescriptor> {
    fn store(&self, id: InternalDatasetId, db: &mut HashMapDatasetDb) -> TypedResultDescriptor {
        db.ogr_datasets.insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

impl HashMapStorable for StaticMetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor> {
    fn store(&self, id: InternalDatasetId, db: &mut HashMapDatasetDb) -> TypedResultDescriptor {
        db.mock_datasets.insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

impl HashMapStorable for GdalMetaDataRegular {
    fn store(&self, id: InternalDatasetId, db: &mut HashMapDatasetDb) -> TypedResultDescriptor {
        db.gdal_datasets.insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

impl HashMapStorable for GdalMetaDataStatic {
    fn store(&self, id: InternalDatasetId, db: &mut HashMapDatasetDb) -> TypedResultDescriptor {
        db.gdal_datasets.insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

impl HashMapStorable for GdalStacTilesReading {
    fn store(&self, id: InternalDatasetId, db: &mut HashMapDatasetDb) -> TypedResultDescriptor {
        db.gdal_datasets.insert(id, Box::new(self.clone()));
        self.result_descriptor().expect("must not fail").into() //TODO: maybe return result?
    }
}

#[async_trait]
impl DatasetStore for HashMapDatasetDb {
    async fn add_dataset(
        &mut self,
        _user: UserId,
        dataset: Validated<AddDataset>,
        meta_data: Box<dyn HashMapStorable>,
    ) -> Result<DatasetId> {
        let dataset = dataset.user_input;
        let id = dataset
            .id
            .unwrap_or_else(|| InternalDatasetId::new().into());
        let result_descriptor = meta_data.store(id.internal().expect("from AddDataset"), self);

        let d: Dataset = Dataset {
            id: id.clone(),
            name: dataset.name,
            description: dataset.description,
            result_descriptor,
            source_operator: dataset.source_operator,
        };
        self.datasets.push(d);

        Ok(id)
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }
}

#[async_trait]
impl DatasetProvider for HashMapDatasetDb {
    async fn list(
        &self,
        _user: UserId,
        options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        // TODO: permissions

        // TODO: include datasets from external dataset providers
        let options = options.user_input;

        let mut list: Vec<_> = if let Some(filter) = &options.filter {
            self.datasets
                .iter()
                .filter(|d| d.name.contains(filter) || d.description.contains(filter))
                .collect()
        } else {
            self.datasets.iter().collect()
        };

        match options.order {
            OrderBy::NameAsc => list.sort_by(|a, b| a.name.cmp(&b.name)),
            OrderBy::NameDesc => list.sort_by(|a, b| b.name.cmp(&a.name)),
        };

        let list = list
            .into_iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(Dataset::listing)
            .collect();

        Ok(list)
    }

    async fn load(&self, _user: UserId, dataset: &DatasetId) -> Result<Dataset> {
        // TODO: permissions

        self.datasets
            .iter()
            .find(|d| d.id == *dataset)
            .cloned()
            .ok_or(error::Error::UnknownDatasetId)
    }
}

impl MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    for HashMapDatasetDb
{
    fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        Ok(Box::new(
            self.mock_datasets
                .get(&dataset.internal().ok_or(
                    geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(error::Error::DatasetIdTypeMissMatch),
                    },
                )?)
                .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                    source: Box::new(error::Error::UnknownDatasetId),
                })?
                .clone(),
        ))
    }
}

impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for HashMapDatasetDb {
    fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        Ok(Box::new(
            self.ogr_datasets
                .get(&dataset.internal().ok_or(
                    geoengine_operators::error::Error::DatasetMetaData {
                        source: Box::new(error::Error::DatasetIdTypeMissMatch),
                    },
                )?)
                .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                    source: Box::new(error::Error::UnknownDatasetId),
                })?
                .clone(),
        ))
    }
}

impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor> for HashMapDatasetDb {
    fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        let id = dataset
            .internal()
            .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::DatasetIdTypeMissMatch),
            })?;

        Ok(self
            .gdal_datasets
            .get(&id)
            .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::UnknownDatasetId),
            })?
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Context, InMemoryContext};
    use crate::datasets::listing::OrderBy;
    use crate::users::session::Session;
    use crate::util::user_input::UserInput;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_operators::source::OgrSourceErrorSpec;

    #[tokio::test]
    async fn add_ogr_and_list() -> Result<()> {
        let ctx = InMemoryContext::default();

        let session = Session::mock();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
        };

        let ds = AddDataset {
            id: None,
            name: "OgrDataset".to_string(),
            description: "My Ogr dataset".to_string(),
            source_operator: "OgrSource".to_string(),
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: "".to_string(),
                data_type: None,
                time: Default::default(),
                columns: None,
                default_geometry: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Skip,
                provenance: None,
            },
            result_descriptor: descriptor.clone(),
        };

        let id = ctx
            .dataset_db_ref_mut()
            .await
            .add_dataset(session.user.id, ds.validated()?, Box::new(meta))
            .await?;

        let exe_ctx = ctx.execution_context(&session)?;

        let meta: Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>> =
            exe_ctx.meta_data(&id)?;

        assert_eq!(
            meta.result_descriptor()?,
            VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default()
            }
        );

        let ds = ctx
            .dataset_db_ref()
            .await
            .list(
                session.user.id,
                DatasetListOptions {
                    filter: None,
                    order: OrderBy::NameAsc,
                    offset: 0,
                    limit: 1,
                }
                .validated()?,
            )
            .await?;

        assert_eq!(ds.len(), 1);

        assert_eq!(
            ds[0],
            DatasetListing {
                id,
                name: "OgrDataset".to_string(),
                description: "My Ogr dataset".to_string(),
                tags: vec![],
                source_operator: "OgrSource".to_string(),
                result_descriptor: descriptor.into()
            }
        );

        Ok(())
    }
}

#[async_trait]
impl UploadDb for HashMapDatasetDb {
    async fn get_upload(&self, _user: UserId, upload: UploadId) -> Result<Upload> {
        // TODO: user permission
        self.uploads
            .get(&upload)
            .map(Clone::clone)
            .ok_or(error::Error::UnknownUploadId)
    }

    async fn create_upload(&mut self, _user: UserId, upload: Upload) -> Result<()> {
        // TODO: user permission
        self.uploads.insert(upload.id, upload);
        Ok(())
    }
}
