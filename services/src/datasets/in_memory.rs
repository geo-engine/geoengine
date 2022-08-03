use crate::contexts::{Db, SimpleSession};
use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider, OrderBy};
use crate::datasets::storage::{AddDataset, Dataset, DatasetDb, DatasetStore, DatasetStorer};
use crate::error;
use crate::error::Result;
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollectionListOptions, LayerListing, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::util::operators::source_operator_from_dataset;
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::dataset::{DataId, DatasetId, LayerId};
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{
    MetaData, RasterResultDescriptor, StaticMetaData, TypedResultDescriptor, VectorResultDescriptor,
};

use geoengine_operators::source::{
    GdalLoadingInfo, GdalMetaDataList, GdalMetaDataRegular, GdalMetadataNetCdfCf, OgrSourceDataset,
};
use geoengine_operators::{mock::MockDatasetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use snafu::ensure;
use std::collections::HashMap;
use std::str::FromStr;

use super::listing::ProvenanceOutput;
use super::storage::{DATASET_DB_LAYER_PROVIDER_ID, DATASET_DB_ROOT_COLLECTION_ID};
use super::{
    listing::SessionMetaDataProvider,
    storage::MetaDataDefinition,
    upload::{Upload, UploadDb, UploadId},
};

#[derive(Default)]
struct HashMapDatasetDbBackend {
    datasets: Vec<Dataset>,
    ogr_datasets: HashMap<
        DatasetId,
        StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>,
    >,
    mock_datasets: HashMap<
        DatasetId,
        StaticMetaData<
            MockDatasetDataSourceLoadingInfo,
            VectorResultDescriptor,
            VectorQueryRectangle,
        >,
    >,
    gdal_datasets: HashMap<
        DatasetId,
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    >,
    uploads: HashMap<UploadId, Upload>,
}

#[derive(Default)]
pub struct HashMapDatasetDb {
    backend: Db<HashMapDatasetDbBackend>,
}

impl DatasetDb<SimpleSession> for HashMapDatasetDb {}

#[async_trait]
pub trait HashMapStorable: Send + Sync {
    async fn store(&self, id: DatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor;
}

impl DatasetStorer for HashMapDatasetDb {
    type StorageType = Box<dyn HashMapStorable>;
}

#[async_trait]
impl HashMapStorable for MetaDataDefinition {
    async fn store(&self, id: DatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
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
    async fn store(&self, id: DatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
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
    async fn store(&self, id: DatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
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
    async fn store(&self, id: DatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
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
    async fn store(&self, id: DatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
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
    async fn store(&self, id: DatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
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
    async fn store(&self, id: DatasetId, db: &HashMapDatasetDb) -> TypedResultDescriptor {
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
        let id = dataset.id.unwrap_or_else(DatasetId::new);
        let result_descriptor = meta_data.store(id, self).await;

        let d: Dataset = Dataset {
            id,
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
        _session: &SimpleSession,
        dataset: &DatasetId,
    ) -> Result<ProvenanceOutput> {
        self.backend
            .read()
            .await
            .datasets
            .iter()
            .find(|d| d.id == *dataset)
            .map(|d| ProvenanceOutput {
                data: d.id.into(),
                provenance: d.provenance.clone(),
            })
            .ok_or(error::Error::UnknownDatasetId)
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
        id: &DataId,
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
                .get(&id.internal().ok_or(error::Error::DataIdTypeMissMatch)?)
                .ok_or(error::Error::UnknownDataId)?
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
        id: &DataId,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        Ok(Box::new(
            self.backend
                .read()
                .await
                .ogr_datasets
                .get(
                    &id.internal()
                        .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                            source: Box::new(error::Error::DataIdTypeMissMatch),
                        })?,
                )
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
        id: &DataId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let id = id.internal().ok_or(error::Error::DataIdTypeMissMatch)?;

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

#[async_trait]
impl LayerCollectionProvider for HashMapDatasetDb {
    async fn collection_items(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<Vec<CollectionItem>> {
        ensure!(
            *collection == self.root_collection_id().await?,
            error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        let options = options.user_input;

        let backend = self.backend.read().await;

        let listing = backend
            .datasets
            .iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|d| {
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DATASET_DB_LAYER_PROVIDER_ID,
                        // use the dataset id also as layer id, TODO: maybe prefix it?
                        layer_id: LayerId(d.id.to_string()),
                    },
                    name: d.name.clone(),
                    description: d.description.clone(),
                    properties: None,
                })
            })
            .collect();

        Ok(listing)
    }

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(DATASET_DB_ROOT_COLLECTION_ID.to_string()))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let dataset_id = DatasetId::from_str(&id.0)?;

        let backend = self.backend.read().await;

        let dataset = backend
            .datasets
            .iter()
            .find(|d| d.id == dataset_id)
            .ok_or(error::Error::UnknownDatasetId)?;

        let operator = source_operator_from_dataset(&dataset.source_operator, &dataset.id.into())?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: DATASET_DB_LAYER_PROVIDER_ID,
                layer_id: id.clone(),
            },
            name: dataset.name.clone(),
            description: dataset.description.clone(),
            workflow: Workflow { operator },
            symbology: dataset.symbology.clone(),
            properties: None,
        })
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
            time: None,
            bbox: None,
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
        > = exe_ctx.meta_data(&id.into()).await?;

        assert_eq!(
            meta.result_descriptor().await?,
            VectorResultDescriptor {
                data_type: VectorDataType::Data,
                spatial_reference: SpatialReferenceOption::Unreferenced,
                columns: Default::default(),
                time: None,
                bbox: None,
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
