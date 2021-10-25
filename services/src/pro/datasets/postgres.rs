use crate::datasets::provenance::{ProvenanceOutput, ProvenanceProvider};
use crate::datasets::storage::{
    AddDataset, Dataset, DatasetDb, DatasetProviderDb, DatasetProviderListOptions,
    DatasetProviderListing, DatasetStore, DatasetStorer, ExternalDatasetProviderDefinition,
    MetaDataDefinition,
};
use crate::datasets::upload::FileId;
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::error::{self, Error, Result};
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
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterQueryRectangle, RasterResultDescriptor, StaticMetaData,
    TypedResultDescriptor, VectorQueryRectangle, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use postgres_types::{FromSql, ToSql};
use snafu::ResultExt;

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
        _session: &UserSession,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        // TODO: permission
        // TODO: use options

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT 
                id, 
                name, 
                description,
                tags,
                source_operator,
                result_descriptor,
                symbology
            FROM 
                datasets",
            )
            .await?;

        let rows = conn.query(&stmt, &[]).await?;

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

    async fn load(&self, _session: &UserSession, dataset: &DatasetId) -> Result<Dataset> {
        // TODO: permissions

        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT 
                id, 
                name, 
                description,
                result_descriptor,
                source_operator,
                symbology,
                provenance
            FROM 
                datasets
            WHERE 
                id = $1",
            )
            .await?;

        let row = conn.query_one(&stmt, &[&id]).await?;

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
}

#[async_trait]
impl<Tls>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::LoadingInfo {
            source: Box::new(Error::NotYetImplemented),
        })
    }
}

#[async_trait]
impl<Tls> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let id = dataset
            .internal()
            .ok_or(geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(Error::InvalidDatasetId),
            })?;

        let conn = self.conn_pool.get().await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;
        let stmt = conn
            .prepare(
                "
        SELECT 
            meta_data
        FROM 
            datasets 
        WHERE 
            id = $1",
            )
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        let row = conn.query_one(&stmt, &[&id]).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;

        let meta_data: StaticMetaData<
            OgrSourceDataset,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > = serde_json::from_value(row.get(0))?;

        Ok(Box::new(meta_data))
    }
}

#[async_trait]
impl<Tls> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>>,
        geoengine_operators::error::Error,
    > {
        let id = dataset
            .internal()
            .ok_or(geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(Error::InvalidDatasetId),
            })?;

        let conn = self.conn_pool.get().await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;
        let stmt = conn
            .prepare(
                "
        SELECT 
            meta_data
        FROM 
            datasets 
        WHERE 
            id = $1",
            )
            .await
            .map_err(|e| geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            })?;

        let row = conn.query_one(&stmt, &[&id]).await.map_err(|e| {
            geoengine_operators::error::Error::LoadingInfo {
                source: Box::new(e),
            }
        })?;

        let meta_data: MetaDataDefinition = serde_json::from_value(row.get(0))?;

        Ok(match meta_data {
            MetaDataDefinition::GdalMetaDataRegular(m) => Box::new(m),
            MetaDataDefinition::GdalStatic(m) => Box::new(m),
            _ => {
                return Err(geoengine_operators::error::Error::LoadingInfo {
                    source: Box::new(Error::DatasetIdTypeMissMatch),
                })
            }
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
        _session: &UserSession,
        dataset: Validated<AddDataset>,
        meta_data: Box<dyn PostgresStorable<Tls>>,
    ) -> Result<DatasetId> {
        // TODO: permissions
        let dataset = dataset.user_input;
        let id = dataset
            .id
            .unwrap_or_else(|| InternalDatasetId::new().into());
        let internal_id = id.internal().ok_or(Error::InvalidDatasetId)?;

        let meta_data_json = meta_data.to_json()?;

        let conn = self.conn_pool.get().await?;

        let stmt = conn
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

        conn.execute(
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

        Ok(id)
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
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
    async fn get_upload(&self, _session: &UserSession, upload: UploadId) -> Result<Upload> {
        // TODO: permissions
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare("SELECT id, files FROM uploads WHERE id = $1")
            .await?;

        let row = conn.query_one(&stmt, &[&upload]).await?;

        Ok(Upload {
            id: row.get(0),
            files: row
                .get::<_, Vec<FileUpload>>(1)
                .into_iter()
                .map(Into::into)
                .collect(),
        })
    }

    async fn create_upload(&mut self, _session: &UserSession, upload: Upload) -> Result<()> {
        // TODO permission, user
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare("INSERT INTO uploads (id, files) VALUES ($1, $2)")
            .await?;

        conn.execute(
            &stmt,
            &[
                &upload.id,
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

#[async_trait]
impl<Tls> ProvenanceProvider for PostgresDatasetDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        let id = dataset.internal().ok_or(Error::InvalidDatasetId)?;

        // TODO: permissions
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare("SELECT provenance FROM datasets WHERE id = $1")
            .await?;

        let row = conn.query_one(&stmt, &[&id]).await?;

        Ok(ProvenanceOutput {
            dataset: dataset.clone(),
            provenance: serde_json::from_value(row.get(0)).context(error::SerdeJson)?,
        })
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
