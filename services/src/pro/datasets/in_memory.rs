use crate::api::model::datatypes::{DataId, DatasetId, LayerId};
use crate::api::model::services::AddDataset;
use crate::contexts::Db;
use crate::datasets::listing::SessionMetaDataProvider;
use crate::datasets::listing::{
    DatasetListOptions, DatasetListing, DatasetProvider, OrderBy, ProvenanceOutput,
};
use crate::datasets::storage::{
    Dataset, DatasetDb, DatasetStore, DatasetStorer, MetaDataDefinition,
    DATASET_DB_LAYER_PROVIDER_ID, DATASET_DB_ROOT_COLLECTION_ID,
};
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::error;
use crate::error::Result;
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerListing,
    ProviderLayerCollectionId, ProviderLayerId,
};
use crate::layers::listing::{LayerCollectionId, LayerCollectionProvider};
use crate::layers::storage::INTERNAL_PROVIDER_ID;
use crate::pro::datasets::Permission;
use crate::pro::users::{UserId, UserSession};
use crate::util::operators::source_operator_from_dataset;
use crate::util::user_input::Validated;
use crate::workflows::workflow::Workflow;
use async_trait::async_trait;
use geoengine_datatypes::primitives::{RasterQueryRectangle, VectorQueryRectangle};
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{
    MetaData, RasterResultDescriptor, StaticMetaData, TypedResultDescriptor, VectorResultDescriptor,
};

use geoengine_operators::source::{
    GdalLoadingInfo, GdalMetaDataList, GdalMetaDataRegular, GdalMetadataNetCdfCf, OgrSourceDataset,
};
use geoengine_operators::{mock::MockDatasetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use log::{info, warn};
use snafu::ensure;
use std::collections::HashMap;
use std::str::FromStr;

use super::storage::UpdateDatasetPermissions;
use super::DatasetPermission;

#[derive(Default)]
pub struct ProHashMapDatasetDbBackend {
    datasets: HashMap<DatasetId, Dataset>,
    dataset_permissions: Vec<DatasetPermission>,
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

#[derive(Default)]
pub struct ProHashMapDatasetDb {
    pub backend: Db<ProHashMapDatasetDbBackend>,
}

impl DatasetDb<UserSession> for ProHashMapDatasetDb {}

#[async_trait]
pub trait ProHashMapStorable: Send + Sync {
    async fn store(&self, id: DatasetId, db: &ProHashMapDatasetDb) -> TypedResultDescriptor;
}

impl DatasetStorer for ProHashMapDatasetDb {
    type StorageType = Box<dyn ProHashMapStorable>;
}

#[async_trait]
impl ProHashMapStorable for MetaDataDefinition {
    async fn store(&self, id: DatasetId, db: &ProHashMapDatasetDb) -> TypedResultDescriptor {
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
    async fn store(&self, id: DatasetId, db: &ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
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
    async fn store(&self, id: DatasetId, db: &ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .mock_datasets
            .insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl ProHashMapStorable for GdalMetaDataRegular {
    async fn store(&self, id: DatasetId, db: &ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl ProHashMapStorable for GdalMetaDataStatic {
    async fn store(&self, id: DatasetId, db: &ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl ProHashMapStorable for GdalMetadataNetCdfCf {
    async fn store(&self, id: DatasetId, db: &ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl ProHashMapStorable for GdalMetaDataList {
    async fn store(&self, id: DatasetId, db: &ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.backend
            .write()
            .await
            .gdal_datasets
            .insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl DatasetStore<UserSession> for ProHashMapDatasetDb {
    async fn add_dataset(
        &self,
        session: &UserSession,
        dataset: Validated<AddDataset>,
        meta_data: Box<dyn ProHashMapStorable>,
    ) -> Result<DatasetId> {
        info!("Add dataset {:?}", dataset.user_input.name);

        let dataset = dataset.user_input;
        let id = dataset.id.unwrap_or_else(DatasetId::new);
        let result_descriptor = meta_data.store(id, self).await;

        let d: Dataset = Dataset {
            id,
            name: dataset.name,
            description: dataset.description,
            result_descriptor: result_descriptor.into(),
            source_operator: dataset.source_operator,
            symbology: dataset.symbology,
            provenance: dataset.provenance,
        };
        self.backend.write().await.datasets.insert(id, d);

        self.backend
            .write()
            .await
            .dataset_permissions
            .push(DatasetPermission {
                role: session.user.id.into(),
                dataset: id,
                permission: Permission::Owner,
            });

        Ok(id)
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }
}

#[async_trait]
impl DatasetProvider<UserSession> for ProHashMapDatasetDb {
    async fn list(
        &self,
        session: &UserSession,
        options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        let options = options.user_input;

        let backend = self.backend.read().await;

        let iter = backend
            .dataset_permissions
            .iter()
            .filter(|p| session.roles.contains(&p.role))
            .filter_map(|p| {
                let matching_dataset = backend.datasets.get(&p.dataset);

                if matching_dataset.is_none() {
                    warn!("Permission {:?} without a matching dataset", p);
                }

                matching_dataset
            });

        let mut list: Vec<_> = if let Some(filter) = &options.filter {
            iter.filter(|d| d.name.contains(filter) || d.description.contains(filter))
                .collect()
        } else {
            iter.collect()
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

    async fn load(&self, session: &UserSession, dataset: &DatasetId) -> Result<Dataset> {
        let backend = self.backend.read().await;
        ensure!(
            backend
                .dataset_permissions
                .iter()
                .any(|p| session.roles.contains(&p.role)),
            error::DatasetPermissionDenied { dataset: *dataset }
        );

        backend
            .datasets
            .get(dataset)
            .map(Clone::clone)
            .ok_or(error::Error::UnknownDatasetId)
    }

    async fn provenance(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<ProvenanceOutput> {
        let backend = self.backend.read().await;

        ensure!(
            backend
                .dataset_permissions
                .iter()
                .any(|p| session.roles.contains(&p.role)),
            error::DatasetPermissionDenied { dataset: *dataset }
        );

        backend
            .datasets
            .get(dataset)
            .map(|d| ProvenanceOutput {
                data: d.id.into(),
                provenance: d.provenance.clone(),
            })
            .ok_or(error::Error::UnknownDatasetId)
    }
}

#[async_trait]
impl UpdateDatasetPermissions for ProHashMapDatasetDb {
    async fn add_dataset_permission(
        &self,
        session: &UserSession,
        permission: DatasetPermission,
    ) -> Result<()> {
        info!("Add dataset permission {:?}", permission);

        let mut backend = self.backend.write().await;

        ensure!(
            backend
                .dataset_permissions
                .iter()
                .any(|p| session.roles.contains(&p.role) && p.permission == Permission::Owner),
            error::UpdateDatasetPermission {
                role: session.user.id.to_string(),
                dataset: permission.dataset,
                permission: format!("{:?}", permission.permission),
            }
        );

        ensure!(
            !backend.dataset_permissions.contains(&permission),
            error::DuplicateDatasetPermission {
                role: session.user.id.to_string(),
                dataset: permission.dataset,
                permission: format!("{:?}", permission.permission),
            }
        );

        backend.dataset_permissions.push(permission);

        Ok(())
    }
}

#[async_trait]
impl
    SessionMetaDataProvider<
        UserSession,
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for ProHashMapDatasetDb
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
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
        let backend = self.backend.read().await;
        let id = id.internal().ok_or(error::Error::DataIdTypeMissMatch)?;
        ensure!(
            backend
                .dataset_permissions
                .iter()
                .any(|p| p.dataset == id && session.roles.contains(&p.role)),
            error::DatasetPermissionDenied { dataset: id }
        );

        Ok(Box::new(
            backend
                .mock_datasets
                .get(&id)
                .ok_or(error::Error::UnknownDatasetId)?
                .clone(),
        ))
    }
}

#[async_trait]
impl
    SessionMetaDataProvider<
        UserSession,
        OgrSourceDataset,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for ProHashMapDatasetDb
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
        id: &DataId,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        let backend = self.backend.read().await;

        let id = id.internal().ok_or(error::Error::DataIdTypeMissMatch)?;
        ensure!(
            backend
                .dataset_permissions
                .iter()
                .any(|p| p.dataset == id && session.roles.contains(&p.role)),
            error::DatasetPermissionDenied { dataset: id }
        );

        Ok(Box::new(
            backend
                .ogr_datasets
                .get(&id)
                .ok_or(error::Error::UnknownDatasetId)?
                .clone(),
        ))
    }
}

#[async_trait]
impl
    SessionMetaDataProvider<
        UserSession,
        GdalLoadingInfo,
        RasterResultDescriptor,
        RasterQueryRectangle,
    > for ProHashMapDatasetDb
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
        id: &DataId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let backend = self.backend.read().await;

        let id = id.internal().ok_or(error::Error::DataIdTypeMissMatch)?;
        ensure!(
            backend
                .dataset_permissions
                .iter()
                .any(|p| p.dataset == id && session.roles.contains(&p.role)),
            error::DatasetPermissionDenied { dataset: id }
        );

        Ok(backend
            .gdal_datasets
            .get(&id)
            .ok_or(error::Error::UnknownDatasetId)?
            .clone())
    }
}

#[async_trait]
impl UploadDb<UserSession> for ProHashMapDatasetDb {
    async fn get_upload(&self, session: &UserSession, upload: UploadId) -> Result<Upload> {
        self.backend
            .read()
            .await
            .uploads
            .get(&session.user.id)
            .and_then(|u| u.get(&upload).map(Clone::clone))
            .ok_or(error::Error::UnknownUploadId)
    }

    async fn create_upload(&self, session: &UserSession, upload: Upload) -> Result<()> {
        self.backend
            .write()
            .await
            .uploads
            .entry(session.user.id)
            .or_insert_with(HashMap::new)
            .insert(upload.id, upload);
        Ok(())
    }
}

#[async_trait]
impl LayerCollectionProvider for ProHashMapDatasetDb {
    async fn collection(
        &self,
        collection: &LayerCollectionId,
        options: Validated<LayerCollectionListOptions>,
    ) -> Result<LayerCollection> {
        ensure!(
            *collection == self.root_collection_id().await?,
            error::UnknownLayerCollectionId {
                id: collection.clone()
            }
        );

        let options = options.user_input;

        let backend = self.backend.read().await;

        let items = backend
            .datasets
            .iter()
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|(_id, d)| {
                CollectionItem::Layer(LayerListing {
                    id: ProviderLayerId {
                        provider_id: DATASET_DB_LAYER_PROVIDER_ID,
                        // use the dataset id also as layer id
                        layer_id: LayerId(d.id.to_string()),
                    },
                    name: d.name.clone(),
                    description: d.description.clone(),
                    properties: vec![],
                })
            })
            .collect();

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

    async fn root_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(DATASET_DB_ROOT_COLLECTION_ID.to_string()))
    }

    async fn get_layer(&self, id: &LayerId) -> Result<Layer> {
        let dataset_id = DatasetId::from_str(&id.0)?;

        let backend = self.backend.read().await;

        let (_id, dataset) = backend
            .datasets
            .iter()
            .find(|(_id, d)| d.id == dataset_id)
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
            properties: vec![],
            metadata: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{Context, MockableSession};
    use crate::datasets::listing::OrderBy;
    use crate::datasets::upload::{FileId, FileUpload};
    use crate::pro::contexts::ProInMemoryContext;
    use crate::pro::datasets::Role;
    use crate::util::user_input::UserInput;
    use geoengine_datatypes::collections::VectorDataType;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::MetaDataProvider;
    use geoengine_operators::source::OgrSourceErrorSpec;

    #[tokio::test]
    async fn add_ogr_and_list() -> Result<()> {
        let ctx = ProInMemoryContext::test_default();

        let session = UserSession::mock(); // TODO: find suitable way for public data

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

    #[tokio::test]
    async fn it_lists_only_permitted_datasets() -> Result<()> {
        let ctx = ProInMemoryContext::test_default();

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

        let _id = ctx
            .dataset_db_ref()
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        let list1 = ctx
            .dataset_db_ref()
            .list(
                &session1,
                DatasetListOptions {
                    filter: None,
                    order: OrderBy::NameAsc,
                    offset: 0,
                    limit: 1,
                }
                .validated()?,
            )
            .await?;

        assert_eq!(list1.len(), 1);

        let list2 = ctx
            .dataset_db_ref()
            .list(
                &session2,
                DatasetListOptions {
                    filter: None,
                    order: OrderBy::NameAsc,
                    offset: 0,
                    limit: 1,
                }
                .validated()?,
            )
            .await?;

        assert_eq!(list2.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn it_shows_only_permitted_provenance() -> Result<()> {
        let ctx = ProInMemoryContext::test_default();

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

        let id = ctx
            .dataset_db_ref()
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        assert!(ctx
            .dataset_db_ref()
            .provenance(&session1, &id)
            .await
            .is_ok());

        assert!(ctx
            .dataset_db_ref()
            .provenance(&session2, &id)
            .await
            .is_err());

        Ok(())
    }

    #[tokio::test]
    async fn it_updates_permissions() -> Result<()> {
        let ctx = ProInMemoryContext::test_default();

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

        let id = ctx
            .dataset_db_ref()
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        assert!(ctx.dataset_db_ref().load(&session1, &id).await.is_ok());

        assert!(ctx.dataset_db_ref().load(&session2, &id).await.is_err());

        ctx.dataset_db_ref()
            .add_dataset_permission(
                &session1,
                DatasetPermission {
                    role: session2.user.id.into(),
                    dataset: id,
                    permission: Permission::Read,
                },
            )
            .await?;

        assert!(ctx.dataset_db_ref().load(&session2, &id).await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn it_uses_roles_for_permissions() -> Result<()> {
        let ctx = ProInMemoryContext::test_default();

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

        let id = ctx
            .dataset_db_ref()
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        assert!(ctx.dataset_db_ref().load(&session1, &id).await.is_ok());

        assert!(ctx.dataset_db_ref().load(&session2, &id).await.is_err());

        ctx.dataset_db_ref()
            .add_dataset_permission(
                &session1,
                DatasetPermission {
                    role: Role::user_role_id(),
                    dataset: id,
                    permission: Permission::Read,
                },
            )
            .await?;

        assert!(ctx.dataset_db_ref().load(&session2, &id).await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn it_secures_meta_data() -> Result<()> {
        let ctx = ProInMemoryContext::test_default();

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

        let id = ctx
            .dataset_db_ref()
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        let meta: Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = ctx
            .dataset_db_ref()
            .session_meta_data(&session1, &id.into())
            .await;

        assert!(meta.is_ok());

        let meta: Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = ctx
            .dataset_db_ref()
            .session_meta_data(&session2, &id.into())
            .await;

        assert!(meta.is_err());

        ctx.dataset_db_ref()
            .add_dataset_permission(
                &session1,
                DatasetPermission {
                    role: Role::user_role_id(),
                    dataset: id,
                    permission: Permission::Read,
                },
            )
            .await?;

        let meta: Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = ctx
            .dataset_db_ref()
            .session_meta_data(&session2, &id.into())
            .await;

        assert!(meta.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn it_secures_uploads() -> Result<()> {
        let ctx = ProInMemoryContext::test_default();

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

        ctx.dataset_db_ref()
            .create_upload(&session1, upload)
            .await?;

        assert!(ctx
            .dataset_db_ref()
            .get_upload(&session1, upload_id)
            .await
            .is_ok());

        assert!(ctx
            .dataset_db_ref()
            .get_upload(&session2, upload_id)
            .await
            .is_err());

        Ok(())
    }
}
