use crate::api::model::datatypes::{DatasetId, DatasetName, LayerId};
use crate::api::model::responses::datasets::DatasetIdAndName;
use crate::api::model::services::AddDataset;

use crate::datasets::listing::{
    DatasetListOptions, DatasetListing, DatasetProvider, OrderBy, ProvenanceOutput,
};
use crate::datasets::storage::{
    Dataset, DatasetDb, DatasetStore, DatasetStorer, MetaDataDefinition,
    DATASET_DB_LAYER_PROVIDER_ID, DATASET_DB_ROOT_COLLECTION_ID,
};
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::error::Result;
use crate::error::{self, Error};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{DatasetLayerCollectionProvider, LayerCollectionId};
use crate::layers::storage::INTERNAL_PROVIDER_ID;
use crate::pro::contexts::ProInMemoryDb;
use crate::pro::permissions::{Permission, PermissionDb};
use crate::pro::users::UserId;
use crate::util::operators::source_operator_from_dataset;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use futures::{stream, StreamExt};
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
use log::info;
use snafu::ensure;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

#[derive(Default)]
pub struct ProHashMapDatasetDbBackend {
    datasets_by_id: HashMap<DatasetId, Dataset>,
    datasets_by_name: BTreeMap<DatasetName, Dataset>,
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
    uploads: HashMap<UserId, HashMap<UploadId, Upload>>,
}

impl DatasetDb for ProInMemoryDb {}

#[async_trait]
pub trait ProHashMapStorable: Send + Sync {
    async fn store(&self, id: DatasetId, db: &ProInMemoryDb) -> TypedResultDescriptor;
}

impl DatasetStorer for ProInMemoryDb {
    type StorageType = Box<dyn ProHashMapStorable>;
}

#[async_trait]
impl ProHashMapStorable for MetaDataDefinition {
    async fn store(&self, id: DatasetId, db: &ProInMemoryDb) -> TypedResultDescriptor {
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
impl ProHashMapStorable
    for StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
{
    async fn store(&self, id: DatasetId, db: &ProInMemoryDb) -> TypedResultDescriptor {
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
impl ProHashMapStorable
    for StaticMetaData<
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    >
{
    async fn store(&self, id: DatasetId, db: &ProInMemoryDb) -> TypedResultDescriptor {
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
impl ProHashMapStorable for GdalMetaDataRegular {
    async fn store(&self, id: DatasetId, db: &ProInMemoryDb) -> TypedResultDescriptor {
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
impl ProHashMapStorable for GdalMetaDataStatic {
    async fn store(&self, id: DatasetId, db: &ProInMemoryDb) -> TypedResultDescriptor {
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
impl ProHashMapStorable for GdalMetadataNetCdfCf {
    async fn store(&self, id: DatasetId, db: &ProInMemoryDb) -> TypedResultDescriptor {
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
impl ProHashMapStorable for GdalMetaDataList {
    async fn store(&self, id: DatasetId, db: &ProInMemoryDb) -> TypedResultDescriptor {
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
impl DatasetStore for ProInMemoryDb {
    async fn add_dataset(
        &self,
        dataset: AddDataset,
        meta_data: Box<dyn ProHashMapStorable>,
    ) -> Result<DatasetIdAndName> {
        info!("Add dataset {:?}", dataset.name);

        let id = DatasetId::new();
        let name = dataset.name.unwrap_or_else(|| {
            DatasetName::new(Some(self.session.user.id.to_string()), id.to_string())
        });

        self.check_namespace(&name)?;

        // check if dataset with same id exists

        let backend = self.backend.dataset_db.write().await;

        if backend.datasets_by_name.contains_key(&name) || backend.datasets_by_id.contains_key(&id)
        {
            return Err(Error::DuplicateDatasetId);
        }

        // TODO: we should keep the lock for the whole function to avoid race conditions hereafter.
        drop(backend);

        let result_descriptor = meta_data.store(id, self).await;

        let d: Dataset = Dataset {
            id,
            name: name.clone(),
            display_name: dataset.display_name,
            description: dataset.description,
            result_descriptor: result_descriptor.into(),
            source_operator: dataset.source_operator,
            symbology: dataset.symbology,
            provenance: dataset.provenance,
        };

        let mut backend = self.backend.dataset_db.write().await;

        backend.datasets_by_name.insert(name.clone(), d.clone());
        backend.datasets_by_id.insert(id, d);

        self.create_resource(id).await?;

        Ok(DatasetIdAndName { id, name })
    }

    async fn delete_dataset(&self, dataset_id: DatasetId) -> Result<()> {
        ensure!(
            self.has_permission(dataset_id, Permission::Owner).await?,
            error::PermissionDenied
        );

        let mut backend = self.backend.dataset_db.write().await;

        let Some(dataset) = backend
            .datasets_by_id
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

        self.remove_permissions(dataset_id).await?;

        Ok(())
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }
}

#[async_trait]
impl DatasetProvider for ProInMemoryDb {
    async fn list_datasets(&self, options: DatasetListOptions) -> Result<Vec<DatasetListing>> {
        let backend = self.backend.dataset_db.read().await;

        let mut list = stream::iter(backend.datasets_by_id.values())
            .filter(|d| async {
                if let Some(filter) = &options.filter {
                    if !(d.name.name.contains(filter)
                        || d.display_name.contains(filter)
                        || d.description.contains(filter))
                    {
                        return false;
                    }
                }

                self.has_permission(d.id, Permission::Read)
                    .await
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>()
            .await;

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
        ensure!(
            self.has_permission(*dataset, Permission::Read).await?,
            error::PermissionDenied
        );

        let backend = self.backend.dataset_db.read().await;

        backend
            .datasets_by_id
            .get(dataset)
            .map(Clone::clone)
            .ok_or(error::Error::UnknownDatasetId)
    }

    async fn load_provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        ensure!(
            self.has_permission(*dataset, Permission::Read).await?,
            error::PermissionDenied
        );

        let backend = self.backend.dataset_db.read().await;

        backend
            .datasets_by_id
            .get(dataset)
            .map(|d| ProvenanceOutput {
                data: d.id.into(),
                provenance: d.provenance.clone(),
            })
            .ok_or(error::Error::UnknownDatasetId)
    }

    async fn resolve_dataset_name_to_id(&self, dataset_name: &DatasetName) -> Result<DatasetId> {
        let dataset_db = self.backend.dataset_db.read().await;

        let dataset = dataset_db
            .datasets_by_name
            .get(dataset_name)
            .ok_or(error::Error::UnknownDatasetId)?;

        Ok(dataset.id)
    }
}

#[async_trait]
impl
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for ProInMemoryDb
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

        if !self
            .has_permission(DatasetId::from(id), Permission::Read)
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?
        {
            return Err(geoengine_operators::error::Error::PermissionDenied);
        };

        let backend = self.backend.dataset_db.read().await;

        Ok(Box::new(
            backend
                .mock_datasets
                .get(&id.into())
                .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?
                .clone(),
        ))
    }
}

#[async_trait]
impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for ProInMemoryDb
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

        if !self
            .has_permission(DatasetId::from(id), Permission::Read)
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?
        {
            return Err(geoengine_operators::error::Error::PermissionDenied);
        };

        let backend = self.backend.dataset_db.read().await;

        Ok(Box::new(
            backend
                .ogr_datasets
                .get(&id.into())
                .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?
                .clone(),
        ))
    }
}

#[async_trait]
impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for ProInMemoryDb
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

        if !self
            .has_permission(DatasetId::from(id), Permission::Read)
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?
        {
            return Err(geoengine_operators::error::Error::PermissionDenied);
        };

        let backend = self.backend.dataset_db.read().await;

        Ok(backend
            .gdal_datasets
            .get(&id.into())
            .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?
            .clone())
    }
}

#[async_trait]
impl UploadDb for ProInMemoryDb {
    async fn load_upload(&self, upload: UploadId) -> Result<Upload> {
        let backend = self.backend.dataset_db.read().await;

        backend
            .uploads
            .get(&self.session.user.id)
            .and_then(|u| u.get(&upload).map(Clone::clone))
            .ok_or(error::Error::UnknownUploadId)
    }

    async fn create_upload(&self, upload: Upload) -> Result<()> {
        let mut backend = self.backend.dataset_db.write().await;

        backend
            .uploads
            .entry(self.session.user.id)
            .or_insert_with(HashMap::new)
            .insert(upload.id, upload);
        Ok(())
    }
}

#[async_trait]
impl DatasetLayerCollectionProvider for ProInMemoryDb {
    async fn load_dataset_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let backend = self.backend.dataset_db.read().await;

        let items = stream::iter(backend.datasets_by_id.values())
            .filter(|d| async {
                self.has_permission(d.id, Permission::Read)
                    .await
                    .unwrap_or(false)
            })
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|d| {
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DATASET_DB_LAYER_PROVIDER_ID,
                        // use the dataset id also as layer id
                        layer_id: LayerId(d.id.to_string()),
                    },
                    name: d.display_name.clone(),
                    description: d.description.clone(),
                    properties: vec![],
                })
            })
            .collect()
            .await;

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: INTERNAL_PROVIDER_ID,
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

        ensure!(
            self.has_permission(dataset_id, Permission::Read).await?,
            error::PermissionDenied
        );

        let backend = self.backend.dataset_db.read().await;

        let dataset = backend
            .datasets_by_id
            .get(&dataset_id)
            .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?;

        let operator =
            source_operator_from_dataset(&dataset.source_operator, &dataset.name.clone().into())?;

        Ok(Layer {
            id: ProviderLayerId {
                provider_id: DATASET_DB_LAYER_PROVIDER_ID,
                layer_id: id.clone(),
            },
            name: dataset.display_name.clone(),
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
    use crate::contexts::{ApplicationContext, MockableSession, SessionContext};
    use crate::datasets::listing::OrderBy;
    use crate::datasets::upload::{FileId, FileUpload};
    use crate::pro::contexts::ProInMemoryContext;
    use crate::pro::permissions::Role;
    use crate::pro::users::UserSession;

    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::MetaDataProvider;
    use geoengine_operators::source::OgrSourceErrorSpec;

    #[tokio::test]
    async fn add_ogr_and_list() -> Result<()> {
        let app_ctx = ProInMemoryContext::test_default();

        let session = UserSession::mock(); // TODO: find suitable way for public data

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
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

        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        let id = db.add_dataset(ds, Box::new(meta)).await?.id;

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
                id,
                name: DatasetName::new(Some(session.user.id.to_string()), id.to_string()),
                display_name: "OgrDataset".to_string(),
                description: "My Ogr dataset".to_string(),
                tags: vec![],
                source_operator: "OgrSource".to_string(),
                result_descriptor: descriptor.into(),
                symbology: None,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn it_lists_only_permitted_datasets() -> Result<()> {
        let app_ctx = ProInMemoryContext::test_default();

        let session1 = UserSession::mock();
        let session2 = UserSession::mock();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
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

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let _id = db1.add_dataset(ds, Box::new(meta)).await?;

        let list1 = db1
            .list_datasets(DatasetListOptions {
                filter: None,
                order: OrderBy::NameAsc,
                offset: 0,
                limit: 1,
            })
            .await?;

        assert_eq!(list1.len(), 1);

        let list2 = db2
            .list_datasets(DatasetListOptions {
                filter: None,
                order: OrderBy::NameAsc,
                offset: 0,
                limit: 1,
            })
            .await?;

        assert_eq!(list2.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn it_shows_only_permitted_provenance() -> Result<()> {
        let app_ctx = ProInMemoryContext::test_default();

        let session1 = UserSession::mock();
        let session2 = UserSession::mock();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
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

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let id = db1.add_dataset(ds, Box::new(meta)).await?.id;

        assert!(db1.load_provenance(&id).await.is_ok());

        assert!(db2.load_provenance(&id).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn it_updates_permissions() -> Result<()> {
        let app_ctx = ProInMemoryContext::test_default();

        let session1 = UserSession::mock();
        let session2 = UserSession::mock();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
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

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let id = db1.add_dataset(ds, Box::new(meta)).await?.id;

        assert!(db1.load_dataset(&id).await.is_ok());

        assert!(db2.load_dataset(&id).await.is_err());

        db1.add_permission(session2.user.id.into(), id, Permission::Read)
            .await?;

        assert!(db2.load_dataset(&id).await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn it_uses_roles_for_permissions() -> Result<()> {
        let app_ctx = ProInMemoryContext::test_default();

        let session1 = UserSession::mock();
        let session2 = UserSession::mock();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
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

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let id = db1.add_dataset(ds, Box::new(meta)).await?.id;

        assert!(db1.load_dataset(&id).await.is_ok());

        assert!(db2.load_dataset(&id).await.is_err());

        db1.add_permission(Role::registered_user_role_id(), id, Permission::Read)
            .await?;

        assert!(db2.load_dataset(&id).await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn it_secures_meta_data() -> Result<()> {
        let app_ctx = ProInMemoryContext::test_default();

        let session1 = UserSession::mock();
        let session2 = UserSession::mock();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: None,
            display_name: "OgrDataset".to_string(),
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

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let id = db1.add_dataset(ds, Box::new(meta)).await?.id;

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db1.meta_data(&id.into()).await;

        assert!(meta.is_ok());

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db2.meta_data(&id.into()).await;

        assert!(meta.is_err());

        db1.add_permission(Role::registered_user_role_id(), id, Permission::Read)
            .await?;

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db2.meta_data(&id.into()).await;

        assert!(meta.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn it_secures_meta_data_but_name_is_not_none() -> Result<()> {
        let app_ctx = ProInMemoryContext::test_default();

        let session1 = UserSession::mock();
        let session2 = UserSession::mock();

        let descriptor = VectorResultDescriptor {
            data_type: VectorDataType::Data,
            spatial_reference: SpatialReferenceOption::Unreferenced,
            columns: Default::default(),
            time: None,
            bbox: None,
        };

        let ds = AddDataset {
            name: Some(DatasetName::new(
                Some(session1.user.id.to_string()),
                "my_ogr_dataset",
            )),
            display_name: "OgrDataset".to_string(),
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

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        let id = db1.add_dataset(ds, Box::new(meta)).await?.id;

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db1.meta_data(&id.into()).await;

        assert!(meta.is_ok());

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db2.meta_data(&id.into()).await;

        assert!(meta.is_err());

        db1.add_permission(Role::registered_user_role_id(), id, Permission::Read)
            .await?;

        let meta: geoengine_operators::util::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = db2.meta_data(&id.into()).await;

        assert!(meta.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn it_secures_uploads() -> Result<()> {
        let app_ctx = ProInMemoryContext::test_default();

        let session1 = UserSession::mock();
        let session2 = UserSession::mock();

        let upload_id = UploadId::new();

        let upload = Upload {
            id: upload_id,
            files: vec![FileUpload {
                id: FileId::new(),
                name: "test.bin".to_owned(),
                byte_size: 1024,
            }],
        };

        let db1 = app_ctx.session_context(session1.clone()).db();
        let db2 = app_ctx.session_context(session2.clone()).db();

        db1.create_upload(upload).await?;

        assert!(db1.load_upload(upload_id).await.is_ok());

        assert!(db2.load_upload(upload_id).await.is_err());

        Ok(())
    }
}
