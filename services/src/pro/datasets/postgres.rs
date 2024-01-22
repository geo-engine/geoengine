use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider};
use crate::datasets::listing::{OrderBy, ProvenanceOutput};
use crate::datasets::postgres::resolve_dataset_name_to_id;
use crate::datasets::storage::{Dataset, DatasetDb, DatasetStore, MetaDataDefinition};
use crate::datasets::upload::FileId;
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::datasets::{AddDataset, DatasetIdAndName, DatasetName};
use crate::error::{self, Error, Result};
use crate::pro::contexts::ProPostgresDb;
use crate::pro::permissions::postgres_permissiondb::TxPermissionDb;
use crate::pro::permissions::{Permission, RoleId};
use async_trait::async_trait;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::tokio_postgres::Socket;
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
use snafu::ensure;

impl<Tls> DatasetDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl<Tls> DatasetProvider for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_datasets(&self, options: DatasetListOptions) -> Result<Vec<DatasetListing>> {
        let conn = self.conn_pool.get().await?;

        let mut pos = 3;
        let order_sql = if options.order == OrderBy::NameAsc {
            "name ASC"
        } else {
            "name DESC"
        };

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
                d.id,
                d.name,
                d.display_name,
                d.description,
                d.tags,
                d.source_operator,
                d.result_descriptor,
                d.symbology
            FROM 
                user_permitted_datasets p JOIN datasets d 
                    ON (p.dataset_id = d.id)
            WHERE 
                p.user_id = $1
                {filter_sql}
                {filter_tags_sql}
            ORDER BY {order_sql}
            LIMIT $2
            OFFSET $3;  
            ",
            ))
            .await?;

        let rows = match (options.filter, options.tags) {
            (Some(filter), Some(_)) => {
                conn.query(
                    &stmt,
                    &[
                        &self.session.user.id,
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
                        &self.session.user.id,
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
                        &self.session.user.id,
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
                    &[
                        &self.session.user.id,
                        &i64::from(options.limit),
                        &i64::from(options.offset),
                    ],
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
                d.id,
                d.name,
                d.display_name,
                d.description,
                d.result_descriptor,
                d.source_operator,
                d.symbology,
                d.provenance,
                d.tags
            FROM 
                user_permitted_datasets p JOIN datasets d 
                    ON (p.dataset_id = d.id)
            WHERE 
                p.user_id = $1 AND d.id = $2
            LIMIT 
                1",
            )
            .await?;

        let row = conn
            .query_opt(&stmt, &[&self.session.user.id, dataset])
            .await?;

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
                d.provenance 
            FROM 
                user_permitted_datasets p JOIN datasets d
                    ON(p.dataset_id = d.id)
            WHERE 
                p.user_id = $1 AND d.id = $2",
            )
            .await?;

        let row = conn
            .query_opt(&stmt, &[&self.session.user.id, dataset])
            .await?;

        let row = row.ok_or(error::Error::UnknownDatasetId)?;

        Ok(ProvenanceOutput {
            data: (*dataset).into(),
            provenance: row.get(0),
        })
    }

    async fn resolve_dataset_name_to_id(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<Option<DatasetId>> {
        let conn = self.conn_pool.get().await?;
        resolve_dataset_name_to_id(&conn, dataset_name).await
    }

    async fn dataset_autocomplete_search(
        &self,
        tags: Option<Vec<String>>,
        search_string: String,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<String>> {
        let connection = self.conn_pool.get().await?;

        let limit = i64::from(limit);
        let offset = i64::from(offset);
        let search_string = format!(
            "%{}%",
            search_string.replace('%', "\\%").replace('_', "\\_")
        );

        let mut query_params: Vec<&(dyn ToSql + Sync)> =
            vec![&self.session.user.id, &limit, &offset, &search_string];

        let tags_clause = if let Some(tags) = &tags {
            query_params.push(tags);
            " AND tags @> $5::text[]".to_string()
        } else {
            String::new()
        };

        let stmt = connection
            .prepare(&format!(
                "
            SELECT 
                display_name
            FROM 
                user_permitted_datasets p JOIN datasets d ON (p.dataset_id = d.id)
            WHERE 
                p.user_id = $1
                AND display_name ILIKE $4 ESCAPE '\\'
                {tags_clause}
            ORDER BY display_name ASC
            LIMIT $2
            OFFSET $3;"
            ))
            .await?;

        let rows = connection.query(&stmt, &query_params).await?;

        Ok(rows.iter().map(|row| row.get(0)).collect())
    }
}

#[async_trait]
impl<Tls>
    MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor, VectorQueryRectangle>
    for ProPostgresDb<Tls>
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
    for ProPostgresDb<Tls>
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

        let mut conn = self.conn_pool.get().await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            }
        })?;
        let tx = conn.build_transaction().start().await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            }
        })?;

        if !self
            .has_permission_in_tx(id, Permission::Read, &tx)
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?
        {
            return Err(geoengine_operators::error::Error::PermissionDenied);
        };

        let stmt = tx
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
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?;

        let row = tx
            .query_one(&stmt, &[&id, &self.session.user.id])
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
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

        tx.commit()
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?;

        Ok(Box::new(meta_data))
    }
}

#[async_trait]
impl<Tls> MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor, RasterQueryRectangle>
    for ProPostgresDb<Tls>
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

        let mut conn = self.conn_pool.get().await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            }
        })?;
        let tx = conn.build_transaction().start().await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            }
        })?;

        if !self
            .has_permission_in_tx(id, Permission::Read, &tx)
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?
        {
            return Err(geoengine_operators::error::Error::PermissionDenied);
        };

        let stmt = tx
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
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?;

        let row = tx
            .query_one(&stmt, &[&id, &self.session.user.id])
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
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
    pub meta_data: &'m MetaDataDefinition,
    pub result_descriptor: TypedResultDescriptor,
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
impl<Tls> DatasetStore for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_dataset(
        &self,
        dataset: AddDataset,
        meta_data: MetaDataDefinition,
    ) -> Result<DatasetIdAndName> {
        let id = DatasetId::new();
        let name = dataset.name.unwrap_or_else(|| DatasetName {
            namespace: Some(self.session.user.id.to_string()),
            name: id.to_string(),
        });

        log::info!(
            "Adding dataset with name: {:?}, tags: {:?}",
            name,
            dataset.tags
        );

        self.check_namespace(&name)?;

        let typed_meta_data = meta_data.to_typed_metadata();

        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        // unique constraint on `id` checks if dataset with same id exists

        let stmt = tx
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
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::\"Provenance\"[], $10::text[])",
            )
            .await?;

        tx.execute(
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

        let stmt = tx
            .prepare(
                "
            INSERT INTO permissions (
                role_id,
                dataset_id,
                permission
            )
            VALUES ($1, $2, $3)",
            )
            .await?;

        tx.execute(
            &stmt,
            &[&RoleId::from(self.session.user.id), &id, &Permission::Owner],
        )
        .await?;

        tx.commit().await?;

        Ok(DatasetIdAndName { id, name })
    }

    async fn delete_dataset(&self, dataset_id: DatasetId) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        ensure!(
            self.has_permission_in_tx(dataset_id, Permission::Owner, &tx)
                .await?,
            error::PermissionDenied
        );

        let stmt = tx
            .prepare(
                "
        SELECT 
            TRUE
        FROM 
            user_permitted_datasets p JOIN datasets d 
                ON (p.dataset_id = d.id)
        WHERE 
            d.id = $1 AND p.user_id = $2 AND p.permission = 'Owner';",
            )
            .await?;

        let rows = tx
            .query(&stmt, &[&dataset_id, &self.session.user.id])
            .await?;

        if rows.is_empty() {
            return Err(Error::OperationRequiresOwnerPermission);
        }

        let stmt = tx.prepare("DELETE FROM datasets WHERE id = $1;").await?;

        tx.execute(&stmt, &[&dataset_id]).await?;

        tx.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl<Tls> UploadDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn load_upload(&self, upload: UploadId) -> Result<Upload> {
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT u.id, u.files 
            FROM uploads u JOIN user_uploads uu ON(u.id = uu.upload_id)
            WHERE u.id = $1 AND uu.user_id = $2",
            )
            .await?;

        let row = conn
            .query_one(&stmt, &[&upload, &self.session.user.id])
            .await?;

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
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        let stmt = tx
            .prepare("INSERT INTO uploads (id, files) VALUES ($1, $2)")
            .await?;

        tx.execute(
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

        let stmt = tx
            .prepare("INSERT INTO user_uploads (user_id, upload_id) VALUES ($1, $2)")
            .await?;

        tx.execute(&stmt, &[&self.session.user.id, &upload.id])
            .await?;

        tx.commit().await?;

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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::{
        contexts::{ApplicationContext, SessionContext},
        pro::{
            contexts::ProPostgresContext,
            ge_context,
            users::{UserAuth, UserSession},
        },
    };
    use geoengine_datatypes::{
        collections::VectorDataType,
        primitives::{CacheTtlSeconds, FeatureDataType, Measurement},
        spatial_reference::SpatialReference,
    };
    use geoengine_operators::{
        engine::{StaticMetaData, VectorColumnInfo},
        source::{
            CsvHeader, FormatSpecifics, OgrSourceColumnSpec, OgrSourceDatasetTimeType,
            OgrSourceDurationSpec, OgrSourceErrorSpec, OgrSourceTimeFormat,
        },
    };
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_autocompletes_datasets(app_ctx: ProPostgresContext<NoTls>) {
        let session_a = app_ctx.create_anonymous_session().await.unwrap();
        let session_b = app_ctx.create_anonymous_session().await.unwrap();

        let db_a = app_ctx.session_context(session_a.clone()).db();
        let db_b = app_ctx.session_context(session_b.clone()).db();

        add_single_dataset(&db_a, &session_a).await;

        assert_eq!(
            db_a.dataset_autocomplete_search(None, "Ogr".to_owned(), 10, 0)
                .await
                .unwrap(),
            vec!["Ogr Test"]
        );
        assert_eq!(
            db_a.dataset_autocomplete_search(
                Some(vec!["upload".to_string()]),
                "Ogr".to_owned(),
                10,
                0
            )
            .await
            .unwrap(),
            vec!["Ogr Test"]
        );

        // check that other user B cannot access datasets of user A

        assert!(db_b
            .dataset_autocomplete_search(None, "Ogr".to_owned(), 10, 0)
            .await
            .unwrap()
            .is_empty());
        assert!(db_b
            .dataset_autocomplete_search(Some(vec!["upload".to_string()]), "Ogr".to_owned(), 10, 0)
            .await
            .unwrap()
            .is_empty());
    }

    async fn add_single_dataset(db: &ProPostgresDb<NoTls>, session: &UserSession) {
        let loading_info = OgrSourceDataset {
            file_name: PathBuf::from("test.csv"),
            layer_name: "test.csv".to_owned(),
            data_type: Some(VectorDataType::MultiPoint),
            time: OgrSourceDatasetTimeType::Start {
                start_field: "start".to_owned(),
                start_format: OgrSourceTimeFormat::Auto,
                duration: OgrSourceDurationSpec::Zero,
            },
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: Some(FormatSpecifics::Csv {
                    header: CsvHeader::Auto,
                }),
                x: "x".to_owned(),
                y: None,
                int: vec![],
                float: vec![],
                text: vec![],
                bool: vec![],
                datetime: vec![],
                rename: None,
            }),
            force_ogr_time_filter: false,
            force_ogr_spatial_filter: false,
            on_error: OgrSourceErrorSpec::Ignore,
            sql_query: None,
            attribute_query: None,
            cache_ttl: CacheTtlSeconds::default(),
        };

        let meta_data = MetaDataDefinition::OgrMetaData(StaticMetaData::<
            OgrSourceDataset,
            VectorResultDescriptor,
            VectorQueryRectangle,
        > {
            loading_info: loading_info.clone(),
            result_descriptor: VectorResultDescriptor {
                data_type: VectorDataType::MultiPoint,
                spatial_reference: SpatialReference::epsg_4326().into(),
                columns: [(
                    "foo".to_owned(),
                    VectorColumnInfo {
                        data_type: FeatureDataType::Float,
                        measurement: Measurement::Unitless,
                    },
                )]
                .into_iter()
                .collect(),
                time: None,
                bbox: None,
            },
            phantom: Default::default(),
        });

        let dataset_name = DatasetName::new(Some(session.user.id.to_string()), "my_dataset");

        db.add_dataset(
            AddDataset {
                name: Some(dataset_name.clone()),
                display_name: "Ogr Test".to_owned(),
                description: "desc".to_owned(),
                source_operator: "OgrSource".to_owned(),
                symbology: None,
                provenance: None,
                tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
            },
            meta_data,
        )
        .await
        .unwrap();
    }
}
