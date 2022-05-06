use crate::contexts::{Db, SimpleSession};
use crate::datasets::listing::{
    DatasetListOptions, DatasetListing, DatasetProvider, ExternalDatasetProvider, OrderBy,
};
use crate::datasets::storage::{
    AddDataset, Dataset, DatasetDb, DatasetProviderDb, DatasetProviderListOptions,
    DatasetProviderListing, DatasetStore, DatasetStorer,
};
use crate::error;
use crate::error::Result;
use crate::util::user_input::Validated;
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::{
    dataset::{DatasetId, DatasetProviderId, InternalDatasetId},
    util::Identifier,
};
use geoengine_operators::engine::{
    MetaData, RasterResultDescriptor, StaticMetaData, TypedResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::source::{
    GdalLoadingInfo, GdalMetaDataList, GdalMetaDataRegular, GdalMetadataNetCdfCf, OgrSourceDataset,
};
use geoengine_operators::{mock::MockDatasetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use std::collections::HashMap;

use super::listing::ProvenanceOutput;
use super::{
    listing::SessionMetaDataProvider,
    storage::{ExternalDatasetProviderDefinition, MetaDataDefinition},
    upload::{Upload, UploadDb, UploadId},
};

#[derive(Default)]
struct HashMapDatasetDbBackend {
    datasets: Vec<Dataset>,
    ogr_datasets: HashMap<
        InternalDatasetId,
        StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
    >,
    mock_datasets: HashMap<
        InternalDatasetId,
        StaticMetaData<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    >,
    gdal_datasets: HashMap<
        InternalDatasetId,
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    >,
    uploads: HashMap<UploadId, Upload>,
    external_providers: HashMap<DatasetProviderId, Box<dyn ExternalDatasetProviderDefinition>>,
}

#[derive(Default)]
pub struct HashMapDatasetDb {
    backend: Db<HashMapDatasetDbBackend>,
}

impl DatasetDb<SimpleSession> for HashMapDatasetDb {}

#[async_trait]
impl DatasetProviderDb<SimpleSession> for HashMapDatasetDb {
    async fn add_dataset_provider(
        &self,
        _session: &SimpleSession,
        provider: Box<dyn ExternalDatasetProviderDefinition>,
    ) -> Result<DatasetProviderId> {
        let id = provider.id();
        self.backend
            .write()
            .await
            .external_providers
            .insert(id, provider);
        Ok(id)
    }

    async fn list_dataset_providers(
        &self,
        _session: &SimpleSession,
        _options: Validated<DatasetProviderListOptions>,
    ) -> Result<Vec<DatasetProviderListing>> {
        // TODO: use options
        Ok(self
            .backend
            .read()
            .await
            .external_providers
            .iter()
            .map(|(id, d)| DatasetProviderListing {
                id: *id,
                type_name: d.type_name(),
                name: d.name(),
            })
            .collect())
    }

    async fn dataset_provider(
        &self,
        _session: &SimpleSession,
        provider: DatasetProviderId,
    ) -> Result<Box<dyn ExternalDatasetProvider>> {
        self.backend
            .read()
            .await
            .external_providers
            .get(&provider)
            .cloned()
            .ok_or(error::Error::UnknownProviderId)?
            .initialize()
            .await
    }
}

#[async_trait]
pub trait HashMapStorable: Send + Sync {
    async fn store(&self, id: InternalDatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor;
}

impl DatasetStorer for HashMapDatasetDb {
    type StorageType = Box<dyn HashMapStorable>;
}

#[async_trait]
impl HashMapStorable for MetaDataDefinition {
    async fn store(&self, id: InternalDatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
        match self {
            MetaDataDefinition::MockMetaData(d) => d.store(id, db).await,
            MetaDataDefinition::OgrMetaData(d) => d.store(id, db).await,
            MetaDataDefinition::GdalMetaDataRegular(d) => d.store(id, db).await,
            MetaDataDefinition::GdalStatic(d) => d.store(id, db).await,
            MetaDataDefinition::GdalMetadataNetCdfCf(d) => d.store(id, db).await,
            MetaDataDefinition::GdalMetaDataList(d) => d.store(id, db).await,
        }
    }
}

#[async_trait]
impl HashMapStorable
    for StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
{
    async fn store(&self, id: InternalDatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .ogr_datasets
            .insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable
    for StaticMetaData<
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    >
{
    async fn store(&self, id: InternalDatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .mock_datasets
            .insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable for GdalMetaDataRegular {
    async fn store(&self, id: InternalDatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable for GdalMetaDataStatic {
    async fn store(&self, id: InternalDatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable for GdalMetadataNetCdfCf {
    async fn store(&self, id: InternalDatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable for GdalMetaDataList {
    async fn store(&self, id: InternalDatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl DatasetStore<SimpleSession> for HashMapDatasetDb {
    async fn add_dataset(
        &self,
        _session: &SimpleSession,
        dataset: Validated<AddDataset>,
        meta_data: Box<dyn HashMapStorable>,
    ) -> Result<DatasetId> {
        let dataset = dataset.user_input;
        let id = dataset
            .id
            .unwrap_or_else(|| InternalDatasetId::new().into());
        let result_descriptor = meta_data
            .store(id.internal().expect("from AddDataset"), self)
            .await;

        let d: Dataset = Dataset {
            id: id.clone(),
            name: dataset.name,
            description: dataset.description,
            result_descriptor,
            source_operator: dataset.source_operator,
            symbology: dataset.symbology,
            provenance: dataset.provenance,
        };
        self.backend.write().await.datasets.push(d);

        Ok(id)
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }
}

#[async_trait]
impl DatasetProvider<SimpleSession> for HashMapDatasetDb {
    async fn list(
        &self,
        _session: &SimpleSession,
        options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        // TODO: permissions

        // TODO: include datasets from external dataset providers
        let options = options.user_input;

        let backend = self.backend.read().await;

        let mut list: Vec<_> = if let Some(filter) = &options.filter {
            backend
                .datasets
                .iter()
                .filter(|d| d.name.contains(filter) || d.description.contains(filter))
                .collect()
        } else {
            backend.datasets.iter().collect()
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

    async fn load(&self, _session: &SimpleSession, dataset: &DatasetId) -> Result<Dataset> {
        // TODO: permissions

        self.backend
            .read()
            .await
            .datasets
            .iter()
            .find(|d| d.id == *dataset)
            .cloned()
            .ok_or(error::Error::UnknownDatasetId)
    }

    async fn provenance(
        &self,
        session: &SimpleSession,
        dataset: &DatasetId,
    ) -> Result<ProvenanceOutput> {
        match dataset {
            DatasetId::Internal { dataset_id: _ } => self
                .backend
                .read()
                .await
                .datasets
                .iter()
                .find(|d| d.id == *dataset)
                .map(|d| ProvenanceOutput {
                    dataset: d.id.clone(),
                    provenance: d.provenance.clone(),
                })
                .ok_or(error::Error::UnknownDatasetId),
            DatasetId::External(id) => {
                self.dataset_provider(session, id.provider_id)
                    .await?
                    .provenance(dataset)
                    .await
            }
        }
    }
}

#[async_trait]
impl
    SessionMetaDataProvider<
        SimpleSession,
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for HashMapDatasetDb
{
    async fn session_meta_data(
        &self,
        _session: &SimpleSession,
        dataset: &DatasetId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
    > {
        Ok(Box::new(
            self.backend
                .read()
                .await
                .mock_datasets
                .get(
                    &dataset
                        .internal()
                        .ok_or(error::Error::DatasetIdTypeMissMatch)?,
                )
                .ok_or(error::Error::UnknownDatasetId)?
                .clone(),
        ))
    }
}

#[async_trait]
impl
    SessionMetaDataProvider<
        SimpleSession,
        OgrSourceDataset,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for HashMapDatasetDb
{
    async fn session_meta_data(
        &self,
        _session: &SimpleSession,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        Ok(Box::new(
            self.backend
                .read()
                .await
                .ogr_datasets
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

#[async_trait]
impl
    SessionMetaDataProvider<
        SimpleSession,
        GdalLoadingInfo,
        RasterResultDescriptor,
        RasterQueryRectangle,
    > for HashMapDatasetDb
{
    async fn session_meta_data(
        &self,
        _session: &SimpleSession,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let id = dataset
            .internal()
            .ok_or(error::Error::DatasetIdTypeMissMatch)?;

        Ok(self
            .backend
            .read()
            .await
            .gdal_datasets
            .get(&id)
            .ok_or(error::Error::UnknownDatasetId)?
            .clone())
    }
}

#[async_trait]
impl UploadDb<SimpleSession> for HashMapDatasetDb {
    async fn get_upload(&self, _session: &SimpleSession, upload: UploadId) -> Result<Upload> {
        self.backend
            .read()
            .await
            .uploads
            .get(&upload)
            .map(Clone::clone)
            .ok_or(error::Error::UnknownUploadId)
    }

    async fn create_upload(&self, _session: &SimpleSession, upload: Upload) -> Result<()> {
        self.backend.write().await.uploads.insert(upload.id, upload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Context, InMemoryContext};
    use crate::datasets::listing::OrderBy;
    use crate::util::user_input::UserInput;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::MetaDataProvider;
    use geoengine_operators::source::OgrSourceErrorSpec;

    #[tokio::test]
    async fn add_ogr_and_list() -> Result<()> {
        let ctx = InMemoryContext::test_default();

        let session = SimpleSession::default();

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
            symbology: None,
            provenance: None,
        };

        let meta = StaticMetaData {
            loading_info: OgrSourceDataset {
                file_name: Default::default(),
                layer_name: "".to_string(),
                data_type: None,
                time: Default::default(),
                default_geometry: None,
                columns: None,
                force_ogr_time_filter: false,
                force_ogr_spatial_filter: false,
                on_error: OgrSourceErrorSpec::Ignore,
                sql_query: None,
                attribute_query: None,
            },
            result_descriptor: descriptor.clone(),
            phantom: Default::default(),
        };

        let id = ctx
            .dataset_db_ref()
            .add_dataset(&session, ds.validated()?, Box::new(meta))
            .await?;

        let exe_ctx = ctx.execution_context(session.clone())?;

        let meta: Box<
            dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
        > = exe_ctx.meta_data(&id).await?;

        assert_eq!(
            meta.result_descriptor().await?,
            VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default()
            }
        );

        let ds = ctx
            .dataset_db_ref()
            .list(
                &session,
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
                result_descriptor: descriptor.into(),
                symbology: None,
            }
        );

        Ok(())
    }
}
