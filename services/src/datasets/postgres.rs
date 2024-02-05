use super::listing::{OrderBy, Provenance};
use super::{AddDataset, DatasetIdAndName, DatasetName};
use crate::contexts::PostgresDb;
use crate::datasets::listing::ProvenanceOutput;
use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider};
use crate::datasets::storage::{
    Dataset, DatasetDb, DatasetStore, DatasetStorer, MetaDataDefinition,
};
use crate::datasets::upload::FileId;
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::error::{self, Result};
use async_trait::async_trait;
use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::PostgresConnectionManager;
use geoengine_datatypes::dataset::{DataId, DatasetId};
use geoengine_datatypes::primitives::RasterQueryRectangle;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, TypedResultDescriptor,
    VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{GdalLoadingInfo, OgrSourceDataset};
use postgres_types::{FromSql, ToSql};

impl<Tls> DatasetDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

pub async fn resolve_dataset_name_to_id<Tls>(
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    dataset_name: &DatasetName,
) -> Result<Option<DatasetId>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let stmt = conn
        .prepare(
            "SELECT id
        FROM datasets
        WHERE name = $1::\"DatasetName\"",
        )
        .await?;

    let row_option = conn.query_opt(&stmt, &[&dataset_name]).await?;

    Ok(row_option.map(|row| row.get(0)))
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl<Tls> DatasetProvider for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_datasets(&self, options: DatasetListOptions) -> Result<Vec<DatasetListing>> {
        let conn = self.conn_pool.get().await?;

        let order_sql = if options.order == OrderBy::NameAsc {
            "name ASC"
        } else {
            "name DESC"
        };

        let mut pos = 2;

        let filter_sql = if options.filter.is_some() {
            pos += 1;
            format!("AND (name).name ILIKE ${pos} ESCAPE '\\'")
        } else {
            String::new()
        };

        let (filter_tags_sql, filter_tags_list) = if let Some(filter_tags) = &options.tags {
            pos += 1;
            (format!("AND d.tags @> ${pos}::text[]"), filter_tags.clone())
        } else {
            (String::new(), vec![])
        };

        let stmt = conn
            .prepare(&format!(
                "
            SELECT 
                id,
                name,
                display_name,
                description,
                tags,
                source_operator,
                result_descriptor,
                symbology
            FROM 
                datasets
            {filter_sql}
            {filter_tags_sql}
            ORDER BY {order_sql}
            LIMIT $1
            OFFSET $2;"
            ))
            .await?;

        let rows = match (options.filter, options.tags) {
            (Some(filter), Some(_)) => {
                conn.query(
                    &stmt,
                    &[
                        &i64::from(options.limit),
                        &i64::from(options.offset),
                        &format!("%{}%", filter.replace('%', "\\%").replace('_', "\\_")),
                        &filter_tags_list,
                    ],
                )
                .await?
            }
            (Some(filter), None) => {
                conn.query(
                    &stmt,
                    &[
                        &i64::from(options.limit),
                        &i64::from(options.offset),
                        &format!("%{}%", filter.replace('%', "\\%").replace('_', "\\_")),
                    ],
                )
                .await?
            }
            (None, Some(_)) => {
                conn.query(
                    &stmt,
                    &[
                        &i64::from(options.limit),
                        &i64::from(options.offset),
                        &filter_tags_list,
                    ],
                )
                .await?
            }
            (None, None) => {
                conn.query(
                    &stmt,
                    &[&i64::from(options.limit), &i64::from(options.offset)],
                )
                .await?
            }
        };

        Ok(rows
            .iter()
            .map(|row| {
                Result::<DatasetListing>::Ok(DatasetListing {
                    id: row.get(0),
                    name: row.get(1),
                    display_name: row.get(2),
                    description: row.get(3),
                    tags: row.get::<_, Option<_>>(4).unwrap_or_default(),
                    source_operator: row.get(5),
                    result_descriptor: row.get(6),
                    symbology: row.get(7),
                })
            })
            .filter_map(Result::ok)
            .collect())
    }

    async fn load_dataset(&self, dataset: &DatasetId) -> Result<Dataset> {
        let conn = self.conn_pool.get().await?;
        let stmt = conn
            .prepare(
                "
            SELECT
                id,
                name,
                display_name,
                description,
                result_descriptor,
                source_operator,
                symbology,
                provenance,
                tags
            FROM 
                datasets
            WHERE 
                id = $1
            LIMIT 
                1",
            )
            .await?;

        let row = conn.query_opt(&stmt, &[dataset]).await?;

        let row = row.ok_or(error::Error::UnknownDatasetId)?;

        Ok(Dataset {
            id: row.get(0),
            name: row.get(1),
            display_name: row.get(2),
            description: row.get(3),
            result_descriptor: row.get(4),
            source_operator: row.get(5),
            symbology: row.get(6),
            provenance: row.get(7),
            tags: row.get(8),
        })
    }

    async fn load_provenance(&self, dataset: &DatasetId) -> Result<ProvenanceOutput> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                provenance 
            FROM 
                datasets
            WHERE
                id = $1;",
            )
            .await?;

        let row = conn.query_one(&stmt, &[dataset]).await?;

        let provenances: Vec<Provenance> = row.get(0);

        Ok(ProvenanceOutput {
            data: (*dataset).into(),
            provenance: Some(provenances),
        })
    }

    async fn resolve_dataset_name_to_id(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<Option<DatasetId>> {
        let conn = self.conn_pool.get().await?;
        resolve_dataset_name_to_id(&conn, dataset_name).await
    }
}

#[async_trait]
impl<Tls>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn meta_data(
        &self,
        _id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<
            dyn MetaData<
                MockDatasetDataSourceLoadingInfo,
                VectorResultDescriptor,
                VectorQueryRectangle,
            >,
        >,
    > {
        Err(geoengine_operators::error::Error::NotYetImplemented)
    }
}

#[async_trait]
impl<Tls> MetaDataProvider<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>
    for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
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

        let conn = self.conn_pool.get().await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
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
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?;

        let row = conn.query_one(&stmt, &[&id]).await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            }
        })?;

        let meta_data: MetaDataDefinition = row.get("meta_data");

        let MetaDataDefinition::OgrMetaData(meta_data) = meta_data else {
            return Err(geoengine_operators::error::Error::MetaData {
                source: Box::new(geoengine_operators::error::Error::InvalidType {
                    expected: "OgrMetaData".to_string(),
                    found: meta_data.type_name().to_string(),
                }),
            });
        };

        Ok(Box::new(meta_data))
    }
}

#[async_trait]
impl<Tls> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
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

        let conn = self.conn_pool.get().await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
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
                id = $1;",
            )
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?;

        let row = conn.query_one(&stmt, &[&id]).await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            }
        })?;

        let meta_data: MetaDataDefinition = row.get(0);

        Ok(match meta_data {
            MetaDataDefinition::GdalMetaDataRegular(m) => Box::new(m),
            MetaDataDefinition::GdalStatic(m) => Box::new(m),
            MetaDataDefinition::GdalMetaDataList(m) => Box::new(m),
            MetaDataDefinition::GdalMetadataNetCdfCf(m) => Box::new(m),
            _ => return Err(geoengine_operators::error::Error::DataIdTypeMissMatch),
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
    fn to_typed_metadata(&self) -> Result<DatasetMetaData>;
}

pub struct DatasetMetaData<'m> {
    meta_data: &'m MetaDataDefinition,
    result_descriptor: TypedResultDescriptor,
}

impl<Tls> DatasetStorer for PostgresDb<Tls>
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
    fn to_typed_metadata(&self) -> Result<DatasetMetaData> {
        match self {
            MetaDataDefinition::MockMetaData(d) => Ok(DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            }),
            MetaDataDefinition::OgrMetaData(d) => Ok(DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            }),
            MetaDataDefinition::GdalMetaDataRegular(d) => Ok(DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            }),
            MetaDataDefinition::GdalStatic(d) => Ok(DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            }),
            MetaDataDefinition::GdalMetadataNetCdfCf(d) => Ok(DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            }),
            MetaDataDefinition::GdalMetaDataList(d) => Ok(DatasetMetaData {
                meta_data: self,
                result_descriptor: TypedResultDescriptor::from(d.result_descriptor.clone()),
            }),
        }
    }
}

#[async_trait]
impl<Tls> DatasetStore for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_dataset(
        &self,
        dataset: AddDataset,
        meta_data: Box<dyn PostgresStorable<Tls>>,
    ) -> Result<DatasetIdAndName> {
        let id = DatasetId::new();
        let name = dataset.name.unwrap_or_else(|| DatasetName {
            namespace: None,
            name: id.to_string(),
        });

        Self::check_namespace(&name)?;

        let typed_meta_data = meta_data.to_typed_metadata()?;

        let conn = self.conn_pool.get().await?;

        // unique constraint on `id` checks if dataset with same id exists

        let stmt = conn
            .prepare(
                "
                INSERT INTO datasets (
                    id,
                    name,
                    display_name,
                    description,
                    source_operator,
                    result_descriptor,
                    meta_data,
                    symbology,
                    provenance,
                    tags
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::text[])",
            )
            .await?;

        conn.execute(
            &stmt,
            &[
                &id,
                &name,
                &dataset.display_name,
                &dataset.description,
                &dataset.source_operator,
                &typed_meta_data.result_descriptor,
                typed_meta_data.meta_data,
                &dataset.symbology,
                &dataset.provenance,
                &dataset.tags,
            ],
        )
        .await?;

        Ok(DatasetIdAndName { id, name })
    }

    async fn delete_dataset(&self, dataset_id: DatasetId) -> Result<()> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn.prepare("DELETE FROM datasets WHERE id = $1;").await?;

        conn.execute(&stmt, &[&dataset_id]).await?;

        Ok(())
    }

    fn wrap_meta_data(&self, meta: MetaDataDefinition) -> Self::StorageType {
        Box::new(meta)
    }
}

#[async_trait]
impl<Tls> UploadDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn load_upload(&self, upload: UploadId) -> Result<Upload> {
        // TODO: check permissions

        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare("SELECT id, files FROM uploads WHERE id = $1;")
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

    async fn create_upload(&self, upload: Upload) -> Result<()> {
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
