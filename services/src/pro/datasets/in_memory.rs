use crate::contexts::MockableSession;
use crate::datasets::listing::SessionMetaDataProvider;
use crate::datasets::listing::{
    DatasetListOptions, DatasetListing, DatasetProvider, ExternalDatasetProvider, OrderBy,
    ProvenanceOutput,
};
use crate::datasets::storage::{
    AddDataset, Dataset, DatasetDb, DatasetProviderDb, DatasetProviderListOptions,
    DatasetProviderListing, DatasetStore, DatasetStorer, ExternalDatasetProviderDefinition,
    MetaDataDefinition,
};
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::error;
use crate::error::Result;
use crate::pro::datasets::Permission;
use crate::pro::users::{UserId, UserSession};
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
    GdalLoadingInfo, GdalMetaDataRegular, GdalMetadataNetCdfCf, OgrSourceDataset,
};
use geoengine_operators::{mock::MockDatasetDataSourceLoadingInfo, source::GdalMetaDataStatic};
use log::{info, warn};
use snafu::ensure;
use std::collections::HashMap;

use super::storage::UpdateDatasetPermissions;
use super::DatasetPermission;

#[derive(Default)]
pub struct ProHashMapDatasetDb {
    datasets: HashMap<DatasetId, Dataset>,
    dataset_permissions: Vec<DatasetPermission>,
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
    uploads: HashMap<UserId, HashMap<UploadId, Upload>>,
    external_providers: HashMap<DatasetProviderId, Box<dyn ExternalDatasetProviderDefinition>>,
}

impl DatasetDb<UserSession> for ProHashMapDatasetDb {}

#[async_trait]
impl DatasetProviderDb<UserSession> for ProHashMapDatasetDb {
    async fn add_dataset_provider(
        &mut self,
        _session: &UserSession,
        provider: Box<dyn ExternalDatasetProviderDefinition>,
    ) -> Result<DatasetProviderId> {
        // TODO: authorization
        let id = provider.id();
        self.external_providers.insert(id, provider);
        Ok(id)
    }

    async fn list_dataset_providers(
        &self,
        _session: &UserSession,
        _options: Validated<DatasetProviderListOptions>,
    ) -> Result<Vec<DatasetProviderListing>> {
        // TODO: authorization
        // TODO: use options
        Ok(self
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
        _session: &UserSession,
        provider: DatasetProviderId,
    ) -> Result<Box<dyn ExternalDatasetProvider>> {
        // TODO: authorization
        self.external_providers
            .get(&provider)
            .cloned()
            .ok_or(error::Error::UnknownProviderId)?
            .initialize()
            .await
    }
}

pub trait ProHashMapStorable: Send + Sync {
    fn store(&self, id: InternalDatasetId, db: &mut ProHashMapDatasetDb) -> TypedResultDescriptor;
}

impl DatasetStorer for ProHashMapDatasetDb {
    type StorageType = Box<dyn ProHashMapStorable>;
}

impl ProHashMapStorable for MetaDataDefinition {
    fn store(&self, id: InternalDatasetId, db: &mut ProHashMapDatasetDb) -> TypedResultDescriptor {
        match self {
            MetaDataDefinition::MockMetaData(d) => d.store(id, db),
            MetaDataDefinition::OgrMetaData(d) => d.store(id, db),
            MetaDataDefinition::GdalMetaDataRegular(d) => d.store(id, db),
            MetaDataDefinition::GdalStatic(d) => d.store(id, db),
            MetaDataDefinition::GdalMetadataNetCdfCf(d) => d.store(id, db),
        }
    }
}

impl ProHashMapStorable
    for StaticMetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
{
    fn store(&self, id: InternalDatasetId, db: &mut ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.ogr_datasets.insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

impl ProHashMapStorable
    for StaticMetaData<
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    >
{
    fn store(&self, id: InternalDatasetId, db: &mut ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.mock_datasets.insert(id, self.clone());
        self.result_descriptor.clone().into()
    }
}

impl ProHashMapStorable for GdalMetaDataRegular {
    fn store(&self, id: InternalDatasetId, db: &mut ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.gdal_datasets.insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

impl ProHashMapStorable for GdalMetaDataStatic {
    fn store(&self, id: InternalDatasetId, db: &mut ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.gdal_datasets.insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

impl ProHashMapStorable for GdalMetadataNetCdfCf {
    fn store(&self, id: InternalDatasetId, db: &mut ProHashMapDatasetDb) -> TypedResultDescriptor {
        db.gdal_datasets.insert(id, Box::new(self.clone()));
        self.result_descriptor.clone().into()
    }
}

#[async_trait]
impl DatasetStore<UserSession> for ProHashMapDatasetDb {
    async fn add_dataset(
        &mut self,
        session: &UserSession,
        dataset: Validated<AddDataset>,
        meta_data: Box<dyn ProHashMapStorable>,
    ) -> Result<DatasetId> {
        info!("Add dataset {:?}", dataset.user_input.name);

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
            symbology: dataset.symbology,
            provenance: dataset.provenance,
        };
        self.datasets.insert(id.clone(), d);

        self.dataset_permissions.push(DatasetPermission {
            role: session.user.id.into(),
            dataset: id.clone(),
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

        let iter = self
            .dataset_permissions
            .iter()
            .filter(|p| session.roles.contains(&p.role))
            .filter_map(|p| {
                let matching_dataset = self.datasets.get(&p.dataset);

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
        ensure!(
            self.dataset_permissions
                .iter()
                .any(|p| session.roles.contains(&p.role)),
            error::DatasetPermissionDenied {
                dataset: dataset.clone(),
            }
        );

        self.datasets
            .get(dataset)
            .map(Clone::clone)
            .ok_or(error::Error::UnknownDatasetId)
    }

    async fn provenance(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<ProvenanceOutput> {
        match dataset {
            DatasetId::Internal { dataset_id: _ } => {
                ensure!(
                    self.dataset_permissions
                        .iter()
                        .any(|p| session.roles.contains(&p.role)),
                    error::DatasetPermissionDenied {
                        dataset: dataset.clone(),
                    }
                );

                self.datasets
                    .get(dataset)
                    .map(|d| ProvenanceOutput {
                        dataset: d.id.clone(),
                        provenance: d.provenance.clone(),
                    })
                    .ok_or(error::Error::UnknownDatasetId)
            }
            DatasetId::External(id) => {
                self.dataset_provider(&UserSession::mock(), id.provider_id)
                    .await?
                    .provenance(dataset)
                    .await
            }
        }
    }
}

#[async_trait]
impl UpdateDatasetPermissions for ProHashMapDatasetDb {
    async fn add_dataset_permission(
        &mut self,
        session: &UserSession,
        permission: DatasetPermission,
    ) -> Result<()> {
        info!("Add dataset permission {:?}", permission);

        ensure!(
            self.dataset_permissions
                .iter()
                .any(|p| session.roles.contains(&p.role) && p.permission == Permission::Owner),
            error::UpateDatasetPermission {
                role: session.user.id.to_string(),
                dataset: permission.dataset,
                permission: format!("{:?}", permission.permission),
            }
        );

        ensure!(
            !self.dataset_permissions.contains(&permission),
            error::DuplicateDatasetPermission {
                role: session.user.id.to_string(),
                dataset: permission.dataset,
                permission: format!("{:?}", permission.permission),
            }
        );

        self.dataset_permissions.push(permission);

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
        ensure!(
            self.dataset_permissions
                .iter()
                .any(|p| p.dataset == *dataset && session.roles.contains(&p.role)),
            error::DatasetPermissionDenied {
                dataset: dataset.clone(),
            }
        );

        Ok(Box::new(
            self.mock_datasets
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
        UserSession,
        OgrSourceDataset,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for ProHashMapDatasetDb
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        ensure!(
            self.dataset_permissions
                .iter()
                .any(|p| p.dataset == *dataset && session.roles.contains(&p.role)),
            error::DatasetPermissionDenied {
                dataset: dataset.clone(),
            }
        );

        Ok(Box::new(
            self.ogr_datasets
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
        UserSession,
        GdalLoadingInfo,
        RasterResultDescriptor,
        RasterQueryRectangle,
    > for ProHashMapDatasetDb
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        ensure!(
            self.dataset_permissions
                .iter()
                .any(|p| p.dataset == *dataset && session.roles.contains(&p.role)),
            error::DatasetPermissionDenied {
                dataset: dataset.clone(),
            }
        );

        let id = dataset
            .internal()
            .ok_or(error::Error::DatasetIdTypeMissMatch)?;

        Ok(self
            .gdal_datasets
            .get(&id)
            .ok_or(error::Error::UnknownDatasetId)?
            .clone())
    }
}

#[async_trait]
impl UploadDb<UserSession> for ProHashMapDatasetDb {
    async fn get_upload(&self, session: &UserSession, upload: UploadId) -> Result<Upload> {
        self.uploads
            .get(&session.user.id)
            .and_then(|u| u.get(&upload).map(Clone::clone))
            .ok_or(error::Error::UnknownUploadId)
    }

    async fn create_upload(&mut self, session: &UserSession, upload: Upload) -> Result<()> {
        self.uploads
            .entry(session.user.id)
            .or_insert_with(HashMap::new)
            .insert(upload.id, upload);
        Ok(())
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
            .dataset_db_ref_mut()
            .await
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
            .await
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

        let _id = ctx
            .dataset_db_ref_mut()
            .await
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        let list1 = ctx
            .dataset_db_ref()
            .await
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
            .await
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
            .dataset_db_ref_mut()
            .await
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        assert!(ctx
            .dataset_db_ref()
            .await
            .provenance(&session1, &id)
            .await
            .is_ok());

        assert!(ctx
            .dataset_db_ref()
            .await
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
            .dataset_db_ref_mut()
            .await
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        assert!(ctx
            .dataset_db_ref()
            .await
            .load(&session1, &id)
            .await
            .is_ok());

        assert!(ctx
            .dataset_db_ref()
            .await
            .load(&session2, &id)
            .await
            .is_err());

        ctx.dataset_db_ref_mut()
            .await
            .add_dataset_permission(
                &session1,
                DatasetPermission {
                    role: session2.user.id.into(),
                    dataset: id.clone(),
                    permission: Permission::Read,
                },
            )
            .await?;

        assert!(ctx
            .dataset_db_ref()
            .await
            .load(&session2, &id)
            .await
            .is_ok());

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
            .dataset_db_ref_mut()
            .await
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        assert!(ctx
            .dataset_db_ref()
            .await
            .load(&session1, &id)
            .await
            .is_ok());

        assert!(ctx
            .dataset_db_ref()
            .await
            .load(&session2, &id)
            .await
            .is_err());

        ctx.dataset_db_ref_mut()
            .await
            .add_dataset_permission(
                &session1,
                DatasetPermission {
                    role: Role::user_role_id(),
                    dataset: id.clone(),
                    permission: Permission::Read,
                },
            )
            .await?;

        assert!(ctx
            .dataset_db_ref()
            .await
            .load(&session2, &id)
            .await
            .is_ok());

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
            .dataset_db_ref_mut()
            .await
            .add_dataset(&session1, ds.validated()?, Box::new(meta))
            .await?;

        let meta: Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = ctx
            .dataset_db_ref()
            .await
            .session_meta_data(&session1, &id)
            .await;

        assert!(meta.is_ok());

        let meta: Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = ctx
            .dataset_db_ref()
            .await
            .session_meta_data(&session2, &id)
            .await;

        assert!(meta.is_err());

        ctx.dataset_db_ref_mut()
            .await
            .add_dataset_permission(
                &session1,
                DatasetPermission {
                    role: Role::user_role_id(),
                    dataset: id.clone(),
                    permission: Permission::Read,
                },
            )
            .await?;

        let meta: Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        > = ctx
            .dataset_db_ref()
            .await
            .session_meta_data(&session2, &id)
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

        ctx.dataset_db_ref_mut()
            .await
            .create_upload(&session1, upload)
            .await?;

        assert!(ctx
            .dataset_db_ref()
            .await
            .get_upload(&session1, upload_id)
            .await
            .is_ok());

        assert!(ctx
            .dataset_db_ref()
            .await
            .get_upload(&session2, upload_id)
            .await
            .is_err());

        Ok(())
    }
}
