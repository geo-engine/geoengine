use crate::datasets::listing::ProvenanceOutput;
use crate::datasets::listing::SessionMetaDataProvider;
use crate::datasets::storage::{
    AddDataset, Dataset, DatasetDb, DatasetProviderDb, DatasetProviderListOptions,
    DatasetProviderListing, DatasetStore, DatasetStorer, ExternalDatasetProviderDefinition,
    MetaDataDefinition,
};
use crate::datasets::upload::FileId;
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::error::{self, Error, Result};
use crate::pro::datasets::storage::UpdateDatasetPermissions;
use crate::pro::datasets::RoleId;
use crate::util::user_input::Validated;
use crate::{
    datasets::listing::{
        DatasetListOptions, DatasetListing, DatasetProvider, ExternalDatasetProvider,
    },
    pro::users::UserSession,
};
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, InternalDatasetId};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{
    MetaData, RasterResultDescriptor, StaticMetaData, TypedResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use log::info;
use postgres_types::{FromSql, ToSql};
use snafu::{ensure, ResultExt};

use super::{DatasetPermission, Permission};

pub struct PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: Pool<PostgresConnectionManager<Tls>>,
}

impl<Tls> PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(conn_pool: Pool<PostgresConnectionManager<Tls>>) -> Self {
        Self { conn_pool }
    }
}

impl<Tls> DatasetDb<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

#[async_trait]
impl<Tls> DatasetProviderDb<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_dataset_provider(
        &mut self,
        _session: &UserSession,
        provider: Box<dyn ExternalDatasetProviderDefinition>,
    ) -> Result<DatasetProviderId> {
        // TODO: permissions
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            INSERT INTO dataset_providers (
                id, 
                type_name, 
                name,
                definition
            )
            VALUES ($1, $2, $3, $4)",
            )
            .await?;

        let id = provider.id();
        conn.execute(
            &stmt,
            &[
                &id,
                &provider.type_name(),
                &provider.name(),
                &serde_json::to_value(provider)?,
            ],
        )
        .await?;
        Ok(id)
    }

    async fn list_dataset_providers(
        &self,
        _session: &UserSession,
        _options: Validated<DatasetProviderListOptions>,
    ) -> Result<Vec<DatasetProviderListing>> {
        // TODO: options
        // TODO: permission
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                id, 
                type_name, 
                name
            FROM 
                dataset_providers",
            )
            .await?;

        let rows = conn.query(&stmt, &[]).await?;

        Ok(rows
            .iter()
            .map(|row| DatasetProviderListing {
                id: row.get(0),
                type_name: row.get(1),
                name: row.get(2),
            })
            .collect())
    }

    async fn dataset_provider(
        &self,
        _session: &UserSession,
        provider: DatasetProviderId,
    ) -> Result<Box<dyn ExternalDatasetProvider>> {
        // TODO: permissions
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                definition
            FROM 
                dataset_providers
            WHERE
                id = $1",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&provider]).await?;

        let definition =
            serde_json::from_value::<Box<dyn ExternalDatasetProviderDefinition>>(row.get(0))?;

        definition.initialize().await
    }
}

#[async_trait]
impl<Tls> DatasetProvider<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list(
        &self,
        session: &UserSession,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        // TODO: use options

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT 
                d.id, 
                d.name, 
                d.description,
                d.tags,
                d.source_operator,
                d.result_descriptor,
                d.symbology
            FROM 
                user_permitted_datasets p JOIN datasets d 
                    ON (p.dataset_id = d.id)
            WHERE 
                p.user_id = $1",
            )
            .await?;

        let rows = conn.query(&stmt, &[&session.user.id]).await?;

        Ok(rows
            .iter()
            .map(|row| {
                Result::<DatasetListing>::Ok(DatasetListing {
                    id: DatasetId::Internal {
                        dataset_id: row.get(0),
                    },
                    name: row.get(1),
                    description: row.get(2),
                    tags: row.get::<_, Option<_>>(3).unwrap_or_default(),
                    source_operator: row.get(4),
                    result_descriptor: serde_json::from_value(row.get(5))?,
                    symbology: serde_json::from_value(row.get(6))?,
                })
            })
            .filter_map(Result::ok)
            .collect())
    }

    async fn load(&self, session: &UserSession, dataset: &DatasetId) -> Result<Dataset> {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT
                d.id,
                d.name,
                d.description,
                d.result_descriptor,
                d.source_operator,
                d.symbology,
                d.provenance
            FROM 
                user_permitted_datasets p JOIN datasets d 
                    ON (p.dataset_id = d.id)
            WHERE 
                p.user_id = $1 AND d.id = $2
            LIMIT 
                1",
            )
            .await?;

        // TODO: throw proper dataset does not exist/no permission error
        let row = conn.query_one(&stmt, &[&session.user.id, &id]).await?;

        Ok(Dataset {
            id: DatasetId::Internal {
                dataset_id: row.get(0),
            },
            name: row.get(1),
            description: row.get(2),
            result_descriptor: serde_json::from_value(row.get(3))?,
            source_operator: row.get(4),
            symbology: serde_json::from_value(row.get(5))?,
            provenance: serde_json::from_value(row.get(6))?,
        })
    }

    async fn provenance(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<ProvenanceOutput> {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                d.provenance 
            FROM 
                user_permitted_datasets p JOIN datasets d
                    ON(p.dataset_id = d.id)
            WHERE 
                p.user_id = $1 AND d.id = $2",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&session.user.id, &id]).await?;

        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: serde_json::from_value(row.get(0)).context(error::SerdeJson)?,
        })
    }
}

#[async_trait]
impl<Tls>
    SessionMetaDataProvider<
        UserSession,
        MockDatasetDataSourceLoadingInfo,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn session_meta_data(
        &self,
        _session: &UserSession,
        _dataset: &DatasetId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
    > {
        Err(Error::NotYetImplemented)
    }
}

#[async_trait]
impl<Tls>
    SessionMetaDataProvider<
        UserSession,
        OgrSourceDataset,
        VectorResultDescriptor,
        VectorQueryRectangle,
    > for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>>
    {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
        SELECT 
            d.meta_data
        FROM 
            user_permitted_datasets p JOIN datasets d 
                ON (p.dataset_id = d.id)
        WHERE 
            d.id = $1 AND p.user_id = $2",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&id, &session.user.id]).await?;

        let meta_data: StaticMetaData<
            OgrSourceDataset,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > = serde_json::from_value(row.get(0))?;

        Ok(Box::new(meta_data))
    }
}

#[async_trait]
impl<Tls>
    SessionMetaDataProvider<
        UserSession,
        GdalLoadingInfo,
        RasterResultDescriptor,
        RasterQueryRectangle,
    > for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn session_meta_data(
        &self,
        session: &UserSession,
        dataset: &DatasetId,
    ) -> Result<Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>>
    {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
        SELECT 
            d.meta_data
        FROM 
            user_permitted_datasets p JOIN datasets d 
                ON (p.dataset_id = d.id)
        WHERE 
            d.id = $1 AND p.user_id = $2",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&id, &session.user.id]).await?;

        let meta_data: MetaDataDefinition = serde_json::from_value(row.get(0))?;

        Ok(match meta_data {
            MetaDataDefinition::GdalMetaDataRegular(m) => Box::new(m),
            MetaDataDefinition::GdalStatic(m) => Box::new(m),
            _ => return Err(Error::DatasetIdTypeMissMatch),
        })
    }
}

#[async_trait]
pub trait PostgresStorable<Tls>: Send + Sync
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn to_json(&self) -> Result<DatasetMetaDataJson>;
}

pub struct DatasetMetaDataJson {
    meta_data: serde_json::Value,
    result_descriptor: serde_json::Value,
}

impl<Tls> DatasetStorer for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type StorageType = Box<dyn PostgresStorable<Tls>>;
}

impl<Tls> PostgresStorable<Tls> for MetaDataDefinition
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn to_json(&self) -> Result<DatasetMetaDataJson> {
        match self {
            MetaDataDefinition::MockMetaData(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
            MetaDataDefinition::OgrMetaData(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
            MetaDataDefinition::GdalMetaDataRegular(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
            MetaDataDefinition::GdalStatic(d) => Ok(DatasetMetaDataJson {
                meta_data: serde_json::to_value(self)?,
                result_descriptor: serde_json::to_value(&TypedResultDescriptor::from(
                    d.result_descriptor.clone(),
                ))?,
            }),
        }
    }
}

#[async_trait]
impl<Tls> DatasetStore<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_dataset(
        &mut self,
        session: &UserSession,
        dataset: Validated<AddDataset>,
        meta_data: Box<dyn PostgresStorable<Tls>>,
    ) -> Result<DatasetId> {
        let dataset = dataset.user_input;
        let id = dataset
            .id
            .unwrap_or_else(|| InternalDatasetId::new().into());
        let internal_id = id.internal().ok_or(Error::InvalidDatasetId)?;

        let meta_data_json = meta_data.to_json()?;

        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare(
                "
                INSERT INTO datasets (
                    id,
                    name,
                    description,
                    source_operator,
                    result_descriptor,
                    meta_data,
                    symbology,
                    provenance
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &internal_id,
                &dataset.name,
                &dataset.description,
                &dataset.source_operator,
                &meta_data_json.result_descriptor,
                &meta_data_json.meta_data,
                &serde_json::to_value(&dataset.symbology)?,
                &serde_json::to_value(&dataset.provenance)?,
            ],
        )
        .await?;

        let stmt = tx
            .prepare(
                "
            INSERT INTO dataset_permissions (
                role_id,
                dataset_id,
                permission
            )
            VALUES ($1, $2, $3)",
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &RoleId::from(session.user.id),
                &internal_id,
                &Permission::Owner,
            ],
        )
        .await?;

        tx.commit().await?;

        Ok(id)
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }
}

#[async_trait]
impl<Tls> UpdateDatasetPermissions for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_dataset_permission(
        &mut self,
        session: &UserSession,
        permission: DatasetPermission,
    ) -> Result<()> {
        info!(
            "Add dataset permission session: {:?} permission: {:?}",
            session, permission
        );

        let internal_id = permission.dataset.internal().ok_or(
            geoengine_operators::error::Error::DatasetMetaData {
                source: Box::new(error::Error::DatasetIdTypeMissMatch),
            },
        )?;

        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare(
                "
            SELECT
                user_id 
            FROM 
                user_permitted_datasets 
            WHERE
                user_id = $1 AND dataset_id = $2 AND permission = $3",
            )
            .await?;

        let auth = tx
            .query_one(
                &stmt,
                &[
                    &RoleId::from(session.user.id),
                    &internal_id,
                    &Permission::Owner,
                ],
            )
            .await;

        ensure!(
            auth.is_ok(),
            error::UpateDatasetPermission {
                role: session.user.id.to_string(),
                dataset: permission.dataset,
                permission: format!("{:?}", permission.permission),
            }
        );

        let stmt = tx
            .prepare(
                "
            SELECT 
                COUNT(role_id) 
            FROM 
                dataset_permissions 
            WHERE 
                role_id = $1 AND dataset_id = $2 and permission = $3",
            )
            .await?;

        let duplicate = tx
            .query_one(
                &stmt,
                &[&permission.role, &internal_id, &permission.permission],
            )
            .await?;

        ensure!(
            duplicate.get::<usize, i64>(0) == 0,
            error::DuplicateDatasetPermission {
                role: session.user.id.to_string(),
                dataset: permission.dataset,
                permission: format!("{:?}", permission.permission),
            }
        );

        let stmt = tx
            .prepare(
                "
            INSERT INTO dataset_permissions (
                role_id,
                dataset_id,
                permission
            )
            VALUES ($1, $2, $3)",
            )
            .await?;

        tx.execute(
            &stmt,
            &[&permission.role, &internal_id, &permission.permission],
        )
        .await?;

        tx.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl<Tls> UploadDb<UserSession> for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn get_upload(&self, session: &UserSession, upload: UploadId) -> Result<Upload> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare("SELECT id, files FROM uploads WHERE id = $1 AND user_id = $2")
            .await?;

        let row = conn.query_one(&stmt, &[&upload, &session.user.id]).await?;

        Ok(Upload {
            id: row.get(0),
            files: row
                .get::<_, Vec<FileUpload>>(1)
                .into_iter()
                .map(Into::into)
                .collect(),
        })
    }

    async fn create_upload(&mut self, session: &UserSession, upload: Upload) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare("INSERT INTO uploads (id, user_id, files) VALUES ($1, $2, $3)")
            .await?;

        conn.execute(
            &stmt,
            &[
                &upload.id,
                &session.user.id,
                &upload
                    .files
                    .iter()
                    .map(FileUpload::from)
                    .collect::<Vec<_>>(),
            ],
        )
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, ToSql, FromSql)]
pub struct FileUpload {
    pub id: FileId,
    pub name: String,
    pub byte_size: i64,
}

impl From<crate::datasets::upload::FileUpload> for FileUpload {
    fn from(upload: crate::datasets::upload::FileUpload) -> Self {
        Self {
            id: upload.id,
            name: upload.name,
            byte_size: upload.byte_size as i64,
        }
    }
}

impl From<&crate::datasets::upload::FileUpload> for FileUpload {
    fn from(upload: &crate::datasets::upload::FileUpload) -> Self {
        Self {
            id: upload.id,
            name: upload.name.clone(),
            byte_size: upload.byte_size as i64,
        }
    }
}

impl From<FileUpload> for crate::datasets::upload::FileUpload {
    fn from(upload: FileUpload) -> Self {
        Self {
            id: upload.id,
            name: upload.name,
            byte_size: upload.byte_size as u64,
        }
    }
}
