use crate::api::model::datatypes::{DatasetId, DatasetName, LayerId};
use crate::api::model::services::AddDataset;
use crate::contexts::InMemoryDb;
use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider, OrderBy};
use crate::datasets::storage::{Dataset, DatasetDb, DatasetStore, DatasetStorer};
use crate::error::Result;
use crate::error::{self, Error};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{DatasetLayerCollectionProvider, LayerCollectionId};
use crate::util::operators::source_operator_from_dataset;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::dataset::DataId;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, OperatorName, RasterResultDescriptor, StaticMetaData,
    TypedResultDescriptor, VectorResultDescriptor,
};

use geoengine_operators::mock::MockDatasetDataSource;
use geoengine_operators::source::{
    GdalLoadingInfo, GdalMetaDataList, GdalMetaDataRegular, GdalMetadataNetCdfCf, GdalSource,
    OgrSource, OgrSourceDataset,
};
use geoengine_operators::{mock::MockDatasetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use snafu::ensure;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use super::listing::ProvenanceOutput;
use super::storage::{DATASET_DB_LAYER_PROVIDER_ID, DATASET_DB_ROOT_COLLECTION_ID};
use super::{
    storage::MetaDataDefinition,
    upload::{Upload, UploadDb, UploadId},
};

#[derive(Default)]
pub struct HashMapDatasetDbBackend {
    datasets_by_id: BTreeMap<DatasetName, Dataset>,
    datasets_by_internal_id: HashMap<DatasetId, Dataset>,
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

impl DatasetDb for InMemoryDb {}

#[async_trait]
pub trait HashMapStorable: Send + Sync {
    async fn store(&self, id: DatasetId, db: &InMemoryDb) -> TypedResultDescriptor;
}

impl DatasetStorer for InMemoryDb {
    type StorageType = Box<dyn HashMapStorable>;
}

#[async_trait]
impl HashMapStorable for MetaDataDefinition {
    async fn store(&self, id: DatasetId, db: &InMemoryDb) -> TypedResultDescriptor {
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
    async fn store(&self, id: DatasetId, db: &InMemoryDb) -> TypedResultDescriptor {
        db.backend
            .dataset_db
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
    async fn store(&self, id: DatasetId, db: &InMemoryDb) -> TypedResultDescriptor {
        db.backend
            .dataset_db
            .write()
            .await
            .mock_datasets
            .insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable for GdalMetaDataRegular {
    async fn store(&self, id: DatasetId, db: &InMemoryDb) -> TypedResultDescriptor {
        db.backend
            .dataset_db
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable for GdalMetaDataStatic {
    async fn store(&self, id: DatasetId, db: &InMemoryDb) -> TypedResultDescriptor {
        db.backend
            .dataset_db
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable for GdalMetadataNetCdfCf {
    async fn store(&self, id: DatasetId, db: &InMemoryDb) -> TypedResultDescriptor {
        db.backend
            .dataset_db
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl HashMapStorable for GdalMetaDataList {
    async fn store(&self, id: DatasetId, db: &InMemoryDb) -> TypedResultDescriptor {
        db.backend
            .dataset_db
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl DatasetStore for InMemoryDb {
    async fn add_dataset(
        &self,
        dataset: AddDataset,
        meta_data: Box<dyn HashMapStorable>,
    ) -> Result<DatasetId> {
        let internal_id = DatasetId::new();
        let id = dataset.id.unwrap_or_else(|| DatasetName {
            namespace: geoengine_datatypes::dataset::SYSTEM_NAMESPACE.to_string(),
            name: internal_id.to_string(),
        });

        self.check_namespace(&id).await?;

        // check if dataset with same id exists

        let backend = self.backend.dataset_db.write().await;

        if backend.datasets_by_id.contains_key(&id)
            || backend.datasets_by_internal_id.contains_key(&internal_id)
        {
            return Err(Error::DuplicateDatasetId);
        }

        // TODO: we should keep the lock for the whole function to avoid race conditions hereafter.
        drop(backend);

        let result_descriptor = meta_data.store(internal_id, self).await;

        let d: Dataset = Dataset {
            internal_id,
            id: id.clone(),
            name: dataset.name,
            description: dataset.description,
            result_descriptor: result_descriptor.into(),
            source_operator: dataset.source_operator,
            symbology: dataset.symbology,
            provenance: dataset.provenance,
        };

        let mut backend = self.backend.dataset_db.write().await;

        backend.datasets_by_id.insert(id, d.clone());
        backend.datasets_by_internal_id.insert(internal_id, d);

        Ok(internal_id)
    }

    async fn delete_dataset(&self, dataset_id: DatasetId) -> Result<()> {
        let mut backend = self.backend.dataset_db.write().await;

        let Some(dataset) = backend
            .datasets_by_internal_id
            .remove(&dataset_id) else {
                return Err(Error::UnknownDatasetId);
            };

        backend.datasets_by_id.remove(&dataset.id);

        match dataset.source_operator.as_str() {
            GdalSource::TYPE_NAME => {
                backend.gdal_datasets.remove(&dataset_id);
            }
            OgrSource::TYPE_NAME => {
                backend.ogr_datasets.remove(&dataset_id);
            }
            MockDatasetDataSource::TYPE_NAME => {
                backend.mock_datasets.remove(&dataset_id);
            }
            _ => {
                return Err(Error::UnknownOperator {
                    operator: dataset.source_operator.clone(),
                })
            }
        }

        Ok(())
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }

    async fn check_namespace(&self, id: &DatasetName) -> Result<()> {
        // due to a lack of users, etc., we only allow one namespace for now
        if id.namespace == geoengine_datatypes::dataset::SYSTEM_NAMESPACE {
            Ok(())
        } else {
            Err(Error::InvalidDatasetIdNamespace)
        }
    }
}

#[async_trait]
impl DatasetProvider for InMemoryDb {
    async fn list_datasets(&self, options: DatasetListOptions) -> Result<Vec<DatasetListing>> {
        let backend = self.backend.dataset_db.read().await;

        let mut list: Vec<_> = if let Some(filter) = &options.filter {
            backend
                .datasets_by_id
                .values()
                .filter(|d| d.name.contains(filter) || d.description.contains(filter))
                .collect()
        } else {
            backend.datasets_by_id.values().collect()
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

    async fn load_dataset(&self, dataset: &DatasetId) -> Result<Dataset> {
        self.backend
            .dataset_db
            .read()
            .await
            .datasets_by_internal_id
            .get(dataset)
            .cloned()
            .ok_or(error::Error::UnknownDatasetId)
    }

    async fn load_provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        self.backend
            .dataset_db
            .read()
            .await
            .datasets_by_internal_id
            .get(dataset)
            .map(|d| ProvenanceOutput {
                data: d.internal_id.into(),
                provenance: d.provenance.clone(),
            })
            .ok_or(error::Error::UnknownDatasetId)
    }

    async fn resolve_dataset(
        &self,
        dataset: &geoengine_datatypes::dataset::InternalDataset,
    ) -> Result<geoengine_datatypes::dataset::DatasetId> {
        let dataset_name = match dataset {
            geoengine_datatypes::dataset::InternalDataset::DatasetId { dataset_id } => {
                // early return if we already have an id
                return Ok(*dataset_id);
            }
            geoengine_datatypes::dataset::InternalDataset::Name { name } => name,
        };

        if dataset_name.namespace != geoengine_datatypes::dataset::SYSTEM_NAMESPACE {
            return Err(error::Error::InvalidDatasetIdNamespace);
        }

        let dataset_db = self.backend.dataset_db.read().await;

        let dataset = dataset_db
            .datasets_by_id
            .get(&dataset_name.into())
            .ok_or(error::Error::UnknownDatasetId)?;

        Ok(dataset.internal_id.into())
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for InMemoryDb
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
    > {
        let id = id
            .internal()
            .ok_or(geoengine_operators::error::Error::DataIdTypeMissMatch)?;
        let id = self
            .resolve_dataset(&id)
            .await
            .map_err(|_| geoengine_operators::error::Error::UnknownDataId)?;

        Ok(Box::new(
            self.backend
                .dataset_db
                .read()
                .await
                .mock_datasets
                .get(&id.into())
                .ok_or(geoengine_operators::error::Error::UnknownDataId)?
                .clone(),
        ))
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for InMemoryDb
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
    > {
        let id = id
            .internal()
            .ok_or(geoengine_operators::error::Error::DataIdTypeMissMatch)?;
        let id = self
            .resolve_dataset(&id)
            .await
            .map_err(|_| geoengine_operators::error::Error::UnknownDataId)?;

        Ok(Box::new(
            self.backend
                .dataset_db
                .read()
                .await
                .ogr_datasets
                .get(&id.into())
                .ok_or(geoengine_operators::error::Error::DatasetMetaData {
                    source: Box::new(error::Error::UnknownDatasetId),
                })?
                .clone(),
        ))
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for InMemoryDb
{
    async fn meta_data(
        &self,
        id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
    > {
        let id = id
            .internal()
            .ok_or(geoengine_operators::error::Error::DataIdTypeMissMatch)?;
        let id = self
            .resolve_dataset(&id)
            .await
            .map_err(|_| geoengine_operators::error::Error::UnknownDataId)?;

        Ok(self
            .backend
            .dataset_db
            .read()
            .await
            .gdal_datasets
            .get(&id.into())
            .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?
            .clone())
    }
}

#[async_trait]
impl UploadDb for InMemoryDb {
    async fn load_upload(&self, upload: UploadId) -> Result<Upload> {
        self.backend
            .dataset_db
            .read()
            .await
            .uploads
            .get(&upload)
            .map(Clone::clone)
            .ok_or(error::Error::UnknownUploadId)
    }

    async fn create_upload(&self, upload: Upload) -> Result<()> {
        self.backend
            .dataset_db
            .write()
            .await
            .uploads
            .insert(upload.id, upload);
        Ok(())
    }
}

#[async_trait]
impl DatasetLayerCollectionProvider for InMemoryDb {
    async fn load_dataset_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        ensure!(
            *collection == self.get_dataset_root_layer_collection_id().await?,
            error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        let backend = self.backend.dataset_db.read().await;

        let items = backend
            .datasets_by_id
            .values()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|d| {
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DATASET_DB_LAYER_PROVIDER_ID,
                        // use the dataset id also as layer id, TODO: maybe prefix it?
                        layer_id: LayerId(d.internal_id.to_string()),
                    },
                    name: d.name.clone(),
                    description: d.description.clone(),
                    properties: vec![],
                })
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: DATASET_DB_LAYER_PROVIDER_ID,
                collection_id: collection.clone(),
            },
            name: "Datasets".to_string(),
            description: "Basic Layers for all Datasets".to_string(),
            items,
            entry_label: None,
            properties: vec![],
        })
    }

    async fn get_dataset_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(DATASET_DB_ROOT_COLLECTION_ID.to_string()))
    }

    async fn load_dataset_layer(&self, id: &LayerId) -> Result<Layer> {
        let dataset_id = DatasetId::from_str(&id.0)?;

        let backend = self.backend.dataset_db.read().await;

        let dataset = backend
            .datasets_by_internal_id
            .get(&dataset_id)
            .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?;

        let operator =
            source_operator_from_dataset(&dataset.source_operator, &dataset.internal_id.into())?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: DATASET_DB_LAYER_PROVIDER_ID,
                layer_id: id.clone(),
            },
            name: dataset.name.clone(),
            description: dataset.description.clone(),
            workflow: Workflow { operator },
            symbology: dataset.symbology.clone(),
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{InMemoryContext, SessionContext, SimpleApplicationContext};
    use crate::datasets::listing::OrderBy;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::MetaDataProvider;
    use geoengine_operators::source::OgrSourceErrorSpec;

    #[tokio::test]
    async fn add_ogr_and_list() -> Result<()> {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

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
                layer_name: String::new(),
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

        let db = ctx.db();
        let id = db.add_dataset(ds, Box::new(meta)).await?;

        let exe_ctx = ctx.execution_context()?;

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

        let ds = db
            .list_datasets(DatasetListOptions {
                filter: None,
                order: OrderBy::NameAsc,
                offset: 0,
                limit: 1,
            })
            .await?;

        assert_eq!(ds.len(), 1);

        assert_eq!(
            ds[0],
            DatasetListing {
                id: DatasetName {
                    namespace: geoengine_datatypes::dataset::SYSTEM_NAMESPACE.to_string(),
                    name: id.to_string()
                },
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
