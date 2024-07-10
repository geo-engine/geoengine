use crate::api::model::services::UpdateDataset;
use crate::datasets::listing::Provenance;
use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider};
use crate::datasets::listing::{OrderBy, ProvenanceOutput};
use crate::datasets::postgres::resolve_dataset_name_to_id;
use crate::datasets::storage::{
    Dataset, DatasetDb, DatasetStore, MetaDataDefinition, ReservedTags,
};
use crate::datasets::upload::{delete_upload, FileId};
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::datasets::{AddDataset, DatasetIdAndName, DatasetName};
use crate::error::Error::{
    ExpirationTimestampInPast, IllegalDatasetStatus, IllegalExpirationUpdate, UnknownDatasetId,
};
use crate::error::{self, Error, Result};
use crate::pro::contexts::ProPostgresDb;
use crate::pro::datasets::storage::DatasetDeletionType::{DeleteData, DeleteRecordAndData};
use crate::pro::datasets::storage::InternalUploadedDatasetStatus::{Deleted, DeletedWithError};
use crate::pro::datasets::storage::{
    ChangeDatasetExpiration, DatasetDeletionType, InternalUploadedDatasetStatus,
    TxUploadedUserDatasetStore, UploadedDatasetStatus, UploadedUserDatasetStore,
};
use crate::pro::datasets::{Expiration, ExpirationChange};
use crate::pro::permissions::postgres_permissiondb::TxPermissionDb;
use crate::pro::permissions::{Permission, RoleId};
use crate::projects::Symbology;
use crate::util::postgres::PostgresErrorExt;
use async_trait::async_trait;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::tokio_postgres::Socket;
use geoengine_datatypes::dataset::{DataId, DatasetId};
use geoengine_datatypes::error::BoxedResultExt;
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
use tokio_postgres::Transaction;
use InternalUploadedDatasetStatus::{Available, Expired, Expires, UpdateExpired};

impl<Tls> DatasetDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl<Tls> DatasetProvider for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_datasets(&self, options: DatasetListOptions) -> Result<Vec<DatasetListing>> {
        self.update_datasets_status().await?;
        let conn = self.conn_pool.get().await?;

        let mut pos = 3;
        let order_sql = if options.order == OrderBy::NameAsc {
            "display_name ASC"
        } else {
            "display_name DESC"
        };

        let filter_sql = if options.filter.is_some() {
            pos += 1;
            format!("AND display_name ILIKE ${pos} ESCAPE '\\'")
        } else {
            String::new()
        };

        let (filter_tags_sql, filter_tags_list) = if let Some(filter_tags) = &options.tags {
            pos += 1;
            (format!("AND d.tags @> ${pos}::text[]"), filter_tags.clone())
        } else {
            (
                format!("AND NOT d.tags @> '{{{}}}'::text[]", ReservedTags::Deleted),
                vec![],
            )
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
        self.update_dataset_status(dataset).await?;
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
        self.update_dataset_status(dataset).await?;
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

    async fn load_loading_info(&self, dataset: &DatasetId) -> Result<MetaDataDefinition> {
        self.update_dataset_status(dataset).await?;
        let conn = self.conn_pool.get().await?;

        let stmt = conn
            .prepare(
                "
            SELECT 
                meta_data 
            FROM 
                user_permitted_datasets p JOIN datasets d
                    ON(p.dataset_id = d.id)
            WHERE 
                p.user_id = $1 AND d.id = $2",
            )
            .await?;

        let row = conn
            .query_one(&stmt, &[&self.session.user.id, dataset])
            .await?;

        Ok(row.get(0))
    }

    async fn resolve_dataset_name_to_id(
        &self,
        dataset_name: &DatasetName,
    ) -> Result<Option<DatasetId>> {
        self.update_datasets_status().await?;
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
        self.update_datasets_status().await?;
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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

        let uploaded_status = self.uploaded_dataset_status_in_tx(&id, &tx).await;
        if let Ok(status) = uploaded_status {
            if matches!(status, UploadedDatasetStatus::Deleted) {
                return Err(geoengine_operators::error::Error::DatasetDeleted {
                    id: id.to_string(),
                });
            }
        }

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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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

        let uploaded_status = self.uploaded_dataset_status_in_tx(&id, &tx).await;
        if let Ok(status) = uploaded_status {
            if matches!(status, UploadedDatasetStatus::Deleted) {
                return Err(geoengine_operators::error::Error::DatasetDeleted {
                    id: id.to_string(),
                });
            }
        }

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
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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

        tx.execute(
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
        .await
        .map_unique_violation("datasets", "name", || error::Error::InvalidDatasetName)?;

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

    async fn update_dataset(&self, dataset: DatasetId, update: UpdateDataset) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(dataset.into(), Permission::Owner, &tx)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        tx.execute(
            "UPDATE datasets SET name = $2, display_name = $3, description = $4, tags = $5 WHERE id = $1;",
            &[
                &dataset,
                &update.name,
                &update.display_name,
                &update.description,
                &update.tags,
            ],
        )
        .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn update_dataset_symbology(
        &self,
        dataset: DatasetId,
        symbology: &Symbology,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(dataset.into(), Permission::Owner, &tx)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        tx.execute(
            "UPDATE datasets SET symbology = $2 WHERE id = $1;",
            &[&dataset, &symbology],
        )
        .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn update_dataset_provenance(
        &self,
        dataset: DatasetId,
        provenance: &[Provenance],
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(dataset.into(), Permission::Owner, &tx)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        tx.execute(
            "UPDATE datasets SET provenance = $2 WHERE id = $1;",
            &[&dataset, &provenance],
        )
        .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn delete_dataset(&self, dataset_id: DatasetId) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        let _uploaded = self.uploaded_dataset_status_in_tx(&dataset_id, &tx).await;
        if let Err(error) = _uploaded {
            if matches!(error, UnknownDatasetId) {
                self.ensure_permission_in_tx(dataset_id.into(), Permission::Owner, &tx)
                    .await
                    .boxed_context(crate::error::PermissionDb)?;

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

                return Ok(());
            }
        }

        self.expire_uploaded_dataset(ChangeDatasetExpiration::delete_full(dataset_id))
            .await?;

        Ok(())
    }
}

#[async_trait]
impl<Tls> UploadDb for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
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

#[async_trait]
impl<Tls> TxUploadedUserDatasetStore for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn validate_expiration_request_in_tx(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
        expiration: &Expiration,
    ) -> Result<()> {
        let (status, deletion_type, legal_expiration): (
            InternalUploadedDatasetStatus,
            Option<DatasetDeletionType>,
            bool,
        ) = if let Some(timestamp) = expiration.deletion_timestamp {
            let stmt = tx
                .prepare(
                    "
                    SELECT
                        status,
                        deletion_type,
                        $2 >= CURRENT_TIMESTAMP as legal_expiration
                    FROM
                        uploaded_user_datasets
                    WHERE
                        dataset_id = $1;",
                )
                .await?;
            let row = tx
                .query_opt(&stmt, &[&dataset_id, &timestamp])
                .await?
                .ok_or(UnknownDatasetId)?;
            (row.get(0), row.get(1), row.get::<usize, bool>(2))
        } else {
            let stmt = tx
                .prepare(
                    "
                    SELECT
                        status,
                        deletion_type,
                        TRUE as legal_expiration
                    FROM
                        uploaded_user_datasets
                    WHERE
                        dataset_id = $1;",
                )
                .await?;
            let row = tx
                .query_opt(&stmt, &[&dataset_id])
                .await?
                .ok_or(UnknownDatasetId)?;
            (row.get(0), row.get(1), row.get::<usize, bool>(2))
        };

        match status {
            Available | Expires => {
                if !legal_expiration {
                    return Err(ExpirationTimestampInPast);
                }
            }
            Expired | UpdateExpired | Deleted => {
                if matches!(expiration.deletion_type, DeleteData)
                    && matches!(deletion_type, Some(DeleteRecordAndData))
                {
                    return Err(IllegalExpirationUpdate {
                        reason: "Prior expiration already deleted data and record".to_string(),
                    });
                }
                if expiration.deletion_timestamp.is_some() {
                    return Err(IllegalExpirationUpdate {
                        reason: "Setting expiration after deletion".to_string(),
                    });
                }
            }
            DeletedWithError => {
                return Err(IllegalDatasetStatus {
                    status: "Dataset was deleted, but an error occurred during deletion"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    async fn uploaded_dataset_status_in_tx(
        &self,
        dataset_id: &DatasetId,
        tx: &Transaction,
    ) -> Result<UploadedDatasetStatus> {
        self.ensure_permission_in_tx((*dataset_id).into(), Permission::Read, tx)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        self.update_dataset_status_in_tx(tx, dataset_id).await?;

        let stmt = tx
            .prepare(
                "
            SELECT
                status
            FROM
                uploaded_user_datasets
            WHERE
                dataset_id = $1;",
            )
            .await?;

        let result = tx
            .query_opt(&stmt, &[&dataset_id])
            .await?
            .ok_or(error::Error::UnknownDatasetId)?;

        let internal_status: InternalUploadedDatasetStatus = result.get(0);

        Ok(internal_status.into())
    }

    async fn update_dataset_status_in_tx(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
    ) -> Result<()> {
        let mut newly_expired_datasets = 0;

        let delete_records = tx
            .prepare(
                "
                DELETE FROM
                    datasets
                USING
                    user_permitted_datasets p, uploaded_user_datasets u
                WHERE
                    p.user_id = $1 AND datasets.id = $2
                        AND p.dataset_id = datasets.id AND u.dataset_id = datasets.id
                        AND (u.status = $3 OR u.status = $4) AND u.expiration <= CURRENT_TIMESTAMP
                        AND u.deletion_type = $5;",
            )
            .await?;
        newly_expired_datasets += tx
            .execute(
                &delete_records,
                &[
                    &self.session.user.id,
                    &dataset_id,
                    &Expires,
                    &UpdateExpired,
                    &DeleteRecordAndData,
                ],
            )
            .await?;

        let tag_deletion = tx
            .prepare(
                format!(
                    "
            UPDATE
                datasets
            SET
                tags = tags || '{{{}}}'
            FROM
                user_permitted_datasets p, uploaded_user_datasets u
            WHERE
                p.user_id = $1 AND datasets.id = $2
                    AND p.dataset_id = datasets.id AND u.dataset_id = datasets.id
                    AND u.status = $3 AND u.expiration <= CURRENT_TIMESTAMP
                    AND u.deletion_type = $4;",
                    ReservedTags::Deleted
                )
                .as_str(),
            )
            .await?;
        newly_expired_datasets += tx
            .execute(
                &tag_deletion,
                &[&self.session.user.id, &dataset_id, &Expires, &DeleteData],
            )
            .await?;

        if newly_expired_datasets > 0 {
            let mark_deletion = tx
                .prepare(
                    "
                UPDATE
                    uploaded_user_datasets
                SET
                    status = $3
                FROM
                    user_permitted_datasets p
                WHERE
                    p.user_id = $1 AND uploaded_user_datasets.dataset_id = $2
                        AND uploaded_user_datasets.dataset_id = p.dataset_id
                        AND (status = $4 OR status = $5) AND expiration <= CURRENT_TIMESTAMP;",
                )
                .await?;

            tx.execute(
                &mark_deletion,
                &[
                    &self.session.user.id,
                    &dataset_id,
                    &Expired,
                    &Expires,
                    &UpdateExpired,
                ],
            )
            .await?;
        }

        Ok(())
    }

    async fn update_datasets_status_in_tx(&self, tx: &Transaction) -> Result<()> {
        if self.session.is_admin() {
            self.admin_update_datasets_status_in_tx(tx).await?;
        } else {
            let delete_records = tx
                .prepare(
                    "
                DELETE FROM
                    datasets
                USING
                    user_permitted_datasets p, uploaded_user_datasets u
                WHERE
                    p.user_id = $1 AND p.dataset_id = datasets.id AND u.dataset_id = datasets.id
                        AND u.status = $2 AND u.expiration <= CURRENT_TIMESTAMP
                        AND u.deletion_type = $3;",
                )
                .await?;
            tx.execute(
                &delete_records,
                &[&self.session.user.id, &Expires, &DeleteRecordAndData],
            )
            .await?;

            let tag_deletion = tx
                .prepare(
                    format!(
                        "
                UPDATE
                    datasets
                SET
                    tags = tags || '{{{}}}'
                FROM
                    user_permitted_datasets p, uploaded_user_datasets u
                WHERE
                    p.user_id = $1 AND p.dataset_id = datasets.id AND u.dataset_id = datasets.id
                        AND u.status = $2 AND u.expiration <= CURRENT_TIMESTAMP
                        AND u.deletion_type = $3;",
                        ReservedTags::Deleted
                    )
                    .as_str(),
                )
                .await?;
            tx.execute(
                &tag_deletion,
                &[&self.session.user.id, &Expires, &DeleteData],
            )
            .await?;

            let mark_deletion = tx
                .prepare(
                    "
                UPDATE
                    uploaded_user_datasets
                SET
                    status = $2
                FROM
                    user_permitted_datasets p
                WHERE
                    p.user_id = $1 AND uploaded_user_datasets.dataset_id = p.dataset_id
                        AND (status = $3 OR status = $4) AND expiration <= CURRENT_TIMESTAMP;",
                )
                .await?;

            tx.execute(
                &mark_deletion,
                &[&self.session.user.id, &Expired, &Expires, &UpdateExpired],
            )
            .await?;
        }
        Ok(())
    }

    async fn admin_update_datasets_status_in_tx(&self, tx: &Transaction) -> Result<()> {
        ensure!(self.session.is_admin(), error::PermissionDenied);
        let delete_records = tx
            .prepare(
                "
            DELETE FROM
                datasets
            USING
                user_permitted_datasets p, uploaded_user_datasets u
            WHERE
                u.dataset_id = datasets.id
                    AND u.status = $1 AND u.expiration <= CURRENT_TIMESTAMP
                    AND u.deletion_type = $2;",
            )
            .await?;
        tx.execute(&delete_records, &[&Expires, &DeleteRecordAndData])
            .await?;

        let tag_deletion = tx
            .prepare(
                format!(
                    "
            UPDATE
                datasets
            SET
                tags = tags || '{{{}}}'
            FROM
                uploaded_user_datasets u
            WHERE
                u.dataset_id = datasets.id
                    AND u.status = $1 AND u.expiration <= CURRENT_TIMESTAMP
                    AND u.deletion_type = $2;",
                    ReservedTags::Deleted
                )
                .as_str(),
            )
            .await?;
        tx.execute(&tag_deletion, &[&Expires, &DeleteData]).await?;

        let mark_deletion = tx
            .prepare(
                "
            UPDATE
                uploaded_user_datasets
            SET
                status = $1
            WHERE
                (status = $2 or status = $3) AND expiration <= CURRENT_TIMESTAMP;",
            )
            .await?;

        tx.execute(&mark_deletion, &[&Expired, &Expires, &UpdateExpired])
            .await?;
        Ok(())
    }

    async fn set_expire_for_uploaded_dataset(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
        expiration: &Expiration,
    ) -> Result<()> {
        let num_changes = if let Some(delete_timestamp) = expiration.deletion_timestamp {
            let stmt = tx
                .prepare("
                UPDATE uploaded_user_datasets
                SET status = $2, expiration = $3, deletion_type = $4
                WHERE dataset_id = $1 AND $3 >= CURRENT_TIMESTAMP AND (status = $5 OR status = $6);",
                ).await?;
            tx.execute(
                &stmt,
                &[
                    &dataset_id,
                    &Expires,
                    &delete_timestamp,
                    &expiration.deletion_type,
                    &Available,
                    &Expires,
                ],
            )
            .await?
        } else {
            let stmt = tx
                .prepare(
                    "
                UPDATE uploaded_user_datasets
                SET status = $2, expiration = CURRENT_TIMESTAMP, deletion_type = $3
                WHERE dataset_id = $1 AND (status = $4 OR status = $5);",
                )
                .await?;
            let num_expired = tx
                .execute(
                    &stmt,
                    &[
                        &dataset_id,
                        &Expires,
                        &expiration.deletion_type,
                        &Available,
                        &Expires,
                    ],
                )
                .await?;

            if num_expired == 0 && matches!(expiration.deletion_type, DeleteRecordAndData) {
                let stmt = tx
                    .prepare(
                        "
                    UPDATE uploaded_user_datasets
                    SET deletion_type = $2,
                        status = $3
                    WHERE dataset_id = $1 AND (status = $4 OR status = $5) AND deletion_type = $6;",
                    )
                    .await?;
                tx.execute(
                    &stmt,
                    &[
                        &dataset_id,
                        &expiration.deletion_type,
                        &UpdateExpired,
                        &Expired,
                        &Deleted,
                        &DeleteData,
                    ],
                )
                .await?
            } else {
                num_expired
            }
        };

        if num_changes == 0 {
            self.validate_expiration_request_in_tx(tx, dataset_id, expiration)
                .await?;
        };

        Ok(())
    }

    async fn unset_expire_for_uploaded_dataset(
        &self,
        tx: &Transaction,
        dataset_id: &DatasetId,
    ) -> Result<()> {
        let stmt = tx
            .prepare(
                "
                    UPDATE uploaded_user_datasets
                    SET status = $2, expiration = NULL, deletion_type = NULL
                    WHERE dataset_id = $1 AND status = $3;",
            )
            .await?;
        let set_changes = tx
            .execute(&stmt, &[&dataset_id, &Available, &Expires])
            .await?;
        if set_changes == 0 {
            return Err(IllegalDatasetStatus {
                status: "Requested dataset does not exist or does not have an expiration"
                    .to_string(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl<Tls> UploadedUserDatasetStore for ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn add_uploaded_dataset(
        &self,
        upload_id: UploadId,
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

        tx.execute(
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
        .await
        .map_unique_violation("datasets", "name", || error::Error::InvalidDatasetName)?;

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

        let stmt = tx
            .prepare(
                "
            INSERT INTO uploaded_user_datasets (
                user_id,
                upload_id,
                dataset_id,
                status,
                created,
                expiration,
                deleted,
                deletion_type
            )
            VALUES ($1, $2, $3, 'Available', CURRENT_TIMESTAMP, NULL, NULL, NULL)",
            )
            .await?;

        tx.execute(
            &stmt,
            &[&RoleId::from(self.session.user.id), &upload_id, &id],
        )
        .await?;

        tx.commit().await?;

        Ok(DatasetIdAndName { id, name })
    }

    async fn expire_uploaded_dataset(&self, expire_dataset: ChangeDatasetExpiration) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(expire_dataset.dataset_id.into(), Permission::Owner, &tx)
            .await
            .boxed_context(error::PermissionDb)?;

        self.update_dataset_status_in_tx(&tx, &expire_dataset.dataset_id)
            .await?;

        match expire_dataset.expiration_change {
            ExpirationChange::SetExpire(expiration) => {
                self.set_expire_for_uploaded_dataset(&tx, &expire_dataset.dataset_id, &expiration)
                    .await?;
            }
            ExpirationChange::UnsetExpire => {
                self.unset_expire_for_uploaded_dataset(&tx, &expire_dataset.dataset_id)
                    .await?;
            }
        }
        self.update_dataset_status_in_tx(&tx, &expire_dataset.dataset_id)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    async fn update_dataset_status(&self, dataset_id: &DatasetId) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;
        self.update_dataset_status_in_tx(&tx, dataset_id).await?;
        tx.commit().await?;

        Ok(())
    }

    async fn update_datasets_status(&self) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;
        self.update_datasets_status_in_tx(&tx).await?;
        tx.commit().await?;

        Ok(())
    }

    async fn clear_expired_datasets(&self) -> Result<u64> {
        ensure!(self.session.is_admin(), error::PermissionDenied);

        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.update_datasets_status_in_tx(&tx).await?;

        let update_expired = tx
            .prepare(
                "
                UPDATE
                    uploaded_user_datasets
                SET
                    status = $1
                WHERE
                    status = $2 AND deleted IS NOT NULL;",
            )
            .await?;
        let mut updated = tx.execute(&update_expired, &[&Deleted, &Expired]).await?;

        let marked_datasets = tx
            .prepare(
                "
                SELECT
                    dataset_id, upload_id
                FROM
                    uploaded_user_datasets
                WHERE
                    status = $1 AND deleted IS NULL;",
            )
            .await?;

        let rows = tx.query(&marked_datasets, &[&Expired]).await?;

        let mut deleted = vec![];
        let mut deleted_with_error = vec![];

        for row in rows {
            let dataset_id: DatasetId = row.get(0);
            let upload_id = row.get(1);
            let res = delete_upload(upload_id).await;
            if let Err(error) = res {
                log::error!("Error during deletion of upload {upload_id} from dataset {dataset_id}: {error}, marking as DeletedWithError");
                deleted_with_error.push(upload_id);
            } else {
                deleted.push(upload_id);
            }
            updated += 1; //Could hypothetically overflow
        }

        if !deleted.is_empty() {
            let mark_deletion = tx
                .prepare(
                    "
                UPDATE
                    uploaded_user_datasets
                SET
                    status = $1, deleted = CURRENT_TIMESTAMP
                WHERE
                    status = $2 AND upload_id = ANY($3);",
                )
                .await?;
            tx.execute(&mark_deletion, &[&Deleted, &Expired, &deleted])
                .await?;
        }

        if !deleted_with_error.is_empty() {
            let mark_error = tx
                .prepare(
                    "
                UPDATE
                    uploaded_user_datasets
                SET
                    status = $1, deleted = CURRENT_TIMESTAMP
                WHERE
                    status = $2 AND upload_id = ANY($3);",
                )
                .await?;
            tx.execute(
                &mark_error,
                &[&DeletedWithError, &Expired, &deleted_with_error],
            )
            .await?;
        }

        tx.commit().await?;

        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::ops::{Add, Sub};
    use std::path::PathBuf;

    use super::*;
    use crate::api::model::responses::IdResponse;
    use crate::contexts::SessionId;
    use crate::datasets::upload::UploadRootPath;
    use crate::error::Error::PermissionDenied;
    use crate::pro::users::{UserCredentials, UserRegistration};
    use crate::pro::util::tests::get_db_timestamp;
    use crate::pro::util::tests::{admin_login, send_pro_test_request};
    use crate::util::tests::{SetMultipartBody, TestDataUploads};
    use crate::{
        contexts::{ApplicationContext, SessionContext},
        pro::{
            contexts::ProPostgresContext,
            ge_context,
            users::{UserAuth, UserSession},
        },
    };
    use actix_web::http::header;
    use actix_web::test;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::Duration;
    use geoengine_datatypes::{
        collections::VectorDataType,
        primitives::{CacheTtlSeconds, FeatureDataType, Measurement},
        spatial_reference::SpatialReference,
    };
    use geoengine_operators::error::Error::DatasetDeleted;
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

    const TEST_POINT_DATASET_SOURCE_PATH: &str = "vector/data/points.fgb";

    struct TestDatasetDefinition {
        meta_data: MetaDataDefinition,
        dataset_name: DatasetName,
    }

    struct UploadedTestDataset {
        dataset_name: DatasetName,
        dataset_id: DatasetId,
        upload_id: UploadId,
    }

    fn test_point_dataset(name_space: String, name: &str) -> TestDatasetDefinition {
        let local_path = PathBuf::from(TEST_POINT_DATASET_SOURCE_PATH);
        let file_name = local_path.file_name().unwrap().to_str().unwrap();
        let loading_info = OgrSourceDataset {
            file_name: PathBuf::from(file_name),
            layer_name: file_name.to_owned(),
            data_type: Some(VectorDataType::MultiPoint),
            time: OgrSourceDatasetTimeType::None,
            default_geometry: None,
            columns: Some(OgrSourceColumnSpec {
                format_specifics: None,
                x: "x".to_owned(),
                y: Some("y".to_owned()),
                int: vec!["num".to_owned()],
                float: vec![],
                text: vec!["txt".to_owned()],
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
                columns: [
                    (
                        "num".to_owned(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Int,
                            measurement: Measurement::Unitless,
                        },
                    ),
                    (
                        "txt".to_owned(),
                        VectorColumnInfo {
                            data_type: FeatureDataType::Text,
                            measurement: Measurement::Unitless,
                        },
                    ),
                ]
                .into_iter()
                .collect(),
                time: None,
                bbox: None,
            },
            phantom: Default::default(),
        });

        let dataset_name = DatasetName::new(Some(name_space), name);

        TestDatasetDefinition {
            meta_data,
            dataset_name,
        }
    }

    async fn upload_point_dataset(
        app_ctx: &ProPostgresContext<NoTls>,
        session_id: SessionId,
    ) -> UploadId {
        let files =
            vec![geoengine_datatypes::test_data!(TEST_POINT_DATASET_SOURCE_PATH).to_path_buf()];

        let req = actix_web::test::TestRequest::post()
            .uri("/upload")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_multipart_files(&files);

        let res = send_pro_test_request(req, app_ctx.clone()).await;
        assert_eq!(res.status(), 200);
        let upload: IdResponse<UploadId> = test::read_body_json(res).await;

        upload.id
    }

    async fn upload_and_add_point_dataset(
        app_ctx: &ProPostgresContext<NoTls>,
        user_session: &UserSession,
        name: &str,
        upload_dir: &mut TestDataUploads,
    ) -> UploadedTestDataset {
        let test_dataset = test_point_dataset(user_session.user.id.to_string(), name);
        let upload_id = upload_point_dataset(app_ctx, user_session.id).await;

        let res = app_ctx
            .session_context(user_session.clone())
            .db()
            .add_uploaded_dataset(
                upload_id,
                AddDataset {
                    name: Some(test_dataset.dataset_name.clone()),
                    display_name: "Ogr Test".to_owned(),
                    description: "desc".to_owned(),
                    source_operator: "OgrSource".to_owned(),
                    symbology: None,
                    provenance: None,
                    tags: Some(vec!["upload".to_owned(), "test".to_owned()]),
                },
                test_dataset.meta_data.clone(),
            )
            .await
            .unwrap();

        upload_dir.uploads.push(upload_id);

        UploadedTestDataset {
            dataset_name: test_dataset.dataset_name,
            dataset_id: res.id,
            upload_id,
        }
    }

    fn listing_not_deleted(dataset: &DatasetListing, origin: &UploadedTestDataset) -> bool {
        dataset.name == origin.dataset_name
            && !dataset.tags.contains(&ReservedTags::Deleted.to_string())
    }

    fn dataset_deleted(dataset: &Dataset, origin: &UploadedTestDataset) -> bool {
        let tags = dataset.tags.clone().unwrap();
        let mut num_deleted = 0;
        for tag in tags {
            if tag == ReservedTags::Deleted.to_string() {
                num_deleted += 1;
            }
        }
        dataset.name == origin.dataset_name && num_deleted == 1
    }

    fn dir_exists(origin: &UploadedTestDataset) -> bool {
        let path = origin.upload_id.root_path().unwrap();
        fs::read_dir(path).is_ok()
    }

    async fn register_test_user(app_ctx: &ProPostgresContext<NoTls>) -> UserSession {
        let _user_id = app_ctx
            .register_user(UserRegistration {
                email: "test@localhost".to_string(),
                real_name: "Foo Bar".to_string(),
                password: "test".to_string(),
            })
            .await
            .unwrap();

        app_ctx
            .login(UserCredentials {
                email: "test@localhost".to_string(),
                password: "test".to_string(),
            })
            .await
            .unwrap()
    }

    #[ge_context::test]
    async fn it_deletes_datasets(app_ctx: ProPostgresContext<NoTls>) {
        let mut test_data = TestDataUploads::default();
        let user_session = register_test_user(&app_ctx).await;

        let available =
            upload_and_add_point_dataset(&app_ctx, &user_session, "available", &mut test_data)
                .await;
        let fair =
            upload_and_add_point_dataset(&app_ctx, &user_session, "fair", &mut test_data).await;
        let full =
            upload_and_add_point_dataset(&app_ctx, &user_session, "full", &mut test_data).await;

        let db = app_ctx.session_context(user_session.clone()).db();

        let default_list_options = DatasetListOptions {
            filter: None,
            order: OrderBy::NameAsc,
            offset: 0,
            limit: 10,
            tags: None,
        };

        let listing = db
            .list_datasets(default_list_options.clone())
            .await
            .unwrap();

        assert_eq!(listing.len(), 3);
        assert!(listing_not_deleted(listing.first().unwrap(), &available));
        assert!(listing_not_deleted(listing.get(1).unwrap(), &fair));
        assert!(listing_not_deleted(listing.get(2).unwrap(), &full));

        db.expire_uploaded_dataset(ChangeDatasetExpiration::delete_fair(fair.dataset_id))
            .await
            .unwrap();
        db.expire_uploaded_dataset(ChangeDatasetExpiration::delete_full(full.dataset_id))
            .await
            .unwrap();

        let listing = db
            .list_datasets(default_list_options.clone())
            .await
            .unwrap();

        assert_eq!(listing.len(), 1);
        assert!(listing_not_deleted(listing.first().unwrap(), &available));
        assert!(dataset_deleted(
            &db.load_dataset(&fair.dataset_id).await.unwrap(),
            &fair
        ));
        assert!(matches!(
            db.load_dataset(&full.dataset_id).await.unwrap_err(),
            UnknownDatasetId
        ));

        assert!(dir_exists(&available));
        assert!(dir_exists(&fair));
        assert!(dir_exists(&full));

        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session.clone());
        let deleted = admin_ctx.db().clear_expired_datasets().await.unwrap();

        assert_eq!(deleted, 2);
        assert!(dir_exists(&available));
        assert!(!dir_exists(&fair));
        assert!(!dir_exists(&full));

        let deleted = admin_ctx.db().clear_expired_datasets().await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[ge_context::test]
    async fn it_expires_dataset(app_ctx: ProPostgresContext<NoTls>) {
        let mut test_data = TestDataUploads::default();
        let user_session = register_test_user(&app_ctx).await;

        let current_time = get_db_timestamp(&app_ctx).await;
        let future_time = current_time.add(Duration::seconds(3));

        let fair =
            upload_and_add_point_dataset(&app_ctx, &user_session, "fair", &mut test_data).await;

        let db = app_ctx.session_context(user_session.clone()).db();

        let default_list_options = DatasetListOptions {
            filter: None,
            order: OrderBy::NameAsc,
            offset: 0,
            limit: 10,
            tags: None,
        };

        db.expire_uploaded_dataset(ChangeDatasetExpiration::expire_fair(
            fair.dataset_id,
            future_time,
        ))
        .await
        .unwrap();

        let listing = db
            .list_datasets(default_list_options.clone())
            .await
            .unwrap();

        assert_eq!(listing.len(), 1);
        assert!(listing_not_deleted(listing.first().unwrap(), &fair));

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let listing = db
            .list_datasets(default_list_options.clone())
            .await
            .unwrap();

        assert_eq!(listing.len(), 0);
        assert!(dataset_deleted(
            &db.load_dataset(&fair.dataset_id).await.unwrap(),
            &fair
        ));
    }

    #[ge_context::test]
    async fn it_updates_expiring_dataset(app_ctx: ProPostgresContext<NoTls>) {
        let mut test_data = TestDataUploads::default();
        let user_session = register_test_user(&app_ctx).await;

        let current_time = get_db_timestamp(&app_ctx).await;
        let future_time_1 = current_time.add(Duration::seconds(2));
        let future_time_2 = current_time.add(Duration::seconds(5));

        let fair =
            upload_and_add_point_dataset(&app_ctx, &user_session, "fair", &mut test_data).await;
        let fair2full =
            upload_and_add_point_dataset(&app_ctx, &user_session, "fair2full", &mut test_data)
                .await;

        let db = app_ctx.session_context(user_session.clone()).db();

        let default_list_options = DatasetListOptions {
            filter: None,
            order: OrderBy::NameAsc,
            offset: 0,
            limit: 10,
            tags: None,
        };

        db.expire_uploaded_dataset(ChangeDatasetExpiration::expire_fair(
            fair.dataset_id,
            future_time_1,
        ))
        .await
        .unwrap();
        db.expire_uploaded_dataset(ChangeDatasetExpiration::expire_fair(
            fair.dataset_id,
            future_time_2,
        ))
        .await
        .unwrap();
        db.expire_uploaded_dataset(ChangeDatasetExpiration::expire_fair(
            fair2full.dataset_id,
            future_time_1,
        ))
        .await
        .unwrap();
        db.expire_uploaded_dataset(ChangeDatasetExpiration::expire_full(
            fair2full.dataset_id,
            future_time_1,
        ))
        .await
        .unwrap();

        let listing = db
            .list_datasets(default_list_options.clone())
            .await
            .unwrap();

        assert_eq!(listing.len(), 2);
        assert!(listing_not_deleted(listing.first().unwrap(), &fair));
        assert!(listing_not_deleted(listing.get(1).unwrap(), &fair2full));

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let listing = db
            .list_datasets(default_list_options.clone())
            .await
            .unwrap();

        assert_eq!(listing.len(), 1);
        assert!(listing_not_deleted(listing.first().unwrap(), &fair));
        assert!(matches!(
            db.load_dataset(&fair2full.dataset_id).await.unwrap_err(),
            UnknownDatasetId
        ));

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let listing = db
            .list_datasets(default_list_options.clone())
            .await
            .unwrap();
        assert_eq!(listing.len(), 0);
        assert!(dataset_deleted(
            &db.load_dataset(&fair.dataset_id).await.unwrap(),
            &fair
        ));
    }

    #[allow(clippy::too_many_lines)]
    #[ge_context::test]
    async fn it_updates_expired_dataset(app_ctx: ProPostgresContext<NoTls>) {
        let mut test_data = TestDataUploads::default();
        let user_session = register_test_user(&app_ctx).await;

        let db = app_ctx.session_context(user_session.clone()).db();
        let default_list_options = DatasetListOptions {
            filter: None,
            order: OrderBy::NameAsc,
            offset: 0,
            limit: 10,
            tags: None,
        };

        let fair2full =
            upload_and_add_point_dataset(&app_ctx, &user_session, "fair2full", &mut test_data)
                .await;
        db.expire_uploaded_dataset(ChangeDatasetExpiration::delete_fair(fair2full.dataset_id))
            .await
            .unwrap();
        assert!(dataset_deleted(
            &db.load_dataset(&fair2full.dataset_id).await.unwrap(),
            &fair2full
        ));

        let admin_session = admin_login(&app_ctx).await;
        let admin_ctx = app_ctx.session_context(admin_session.clone());
        let deleted = admin_ctx.db().clear_expired_datasets().await.unwrap();
        assert_eq!(deleted, 1);

        db.expire_uploaded_dataset(ChangeDatasetExpiration::delete_full(fair2full.dataset_id))
            .await
            .unwrap();
        assert!(matches!(
            db.load_dataset(&fair2full.dataset_id).await.unwrap_err(),
            UnknownDatasetId
        ));

        let deleted = admin_ctx.db().clear_expired_datasets().await.unwrap();
        assert_eq!(deleted, 1);

        assert!(db
            .expire_uploaded_dataset(ChangeDatasetExpiration::delete_fair(fair2full.dataset_id))
            .await
            .is_err());

        let current_time = get_db_timestamp(&app_ctx).await;
        let future_time = current_time.add(Duration::seconds(2));
        let fair2available =
            upload_and_add_point_dataset(&app_ctx, &user_session, "fair2available", &mut test_data)
                .await;
        db.expire_uploaded_dataset(ChangeDatasetExpiration::expire_fair(
            fair2available.dataset_id,
            future_time,
        ))
        .await
        .unwrap();
        db.expire_uploaded_dataset(ChangeDatasetExpiration::unset_expire(
            fair2available.dataset_id,
        ))
        .await
        .unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        let deleted = admin_ctx.db().clear_expired_datasets().await.unwrap();
        assert_eq!(deleted, 0);

        let listing = db
            .list_datasets(default_list_options.clone())
            .await
            .unwrap();
        assert_eq!(listing.len(), 1);
        assert!(listing_not_deleted(
            listing.first().unwrap(),
            &fair2available
        ));

        assert!(dir_exists(&fair2available));
        assert!(!dir_exists(&fair2full));
    }

    #[ge_context::test]
    async fn it_handles_expiration_errors(app_ctx: ProPostgresContext<NoTls>) {
        let mut test_data = TestDataUploads::default();
        let user_session = register_test_user(&app_ctx).await;

        let current_time = get_db_timestamp(&app_ctx).await;
        let future_time = current_time.add(Duration::hours(1));
        let past_time = current_time.sub(Duration::hours(1));

        let db = app_ctx.session_context(user_session.clone()).db();

        //Expire before current time
        let test_dataset =
            upload_and_add_point_dataset(&app_ctx, &user_session, "fair2full", &mut test_data)
                .await;
        let err = db
            .expire_uploaded_dataset(ChangeDatasetExpiration::expire_fair(
                test_dataset.dataset_id,
                past_time,
            ))
            .await;
        assert!(err.is_err());
        assert!(matches!(err.unwrap_err(), ExpirationTimestampInPast));

        //Unset expire for non-expiring dataset
        let err = db
            .expire_uploaded_dataset(ChangeDatasetExpiration::unset_expire(
                test_dataset.dataset_id,
            ))
            .await;
        assert!(err.is_err());
        assert!(matches!(err.unwrap_err(), IllegalDatasetStatus { .. }));

        //Expire already deleted
        db.expire_uploaded_dataset(ChangeDatasetExpiration::delete_fair(
            test_dataset.dataset_id,
        ))
        .await
        .unwrap();
        let err = db
            .expire_uploaded_dataset(ChangeDatasetExpiration::expire_fair(
                test_dataset.dataset_id,
                future_time,
            ))
            .await;
        assert!(err.is_err());
        assert!(matches!(err.unwrap_err(), IllegalExpirationUpdate { .. }));

        // Call meta data for deleted
        let err: std::result::Result<
            Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>>,
            geoengine_operators::error::Error,
        > = db
            .meta_data(&DataId::Internal {
                dataset_id: test_dataset.dataset_id,
            })
            .await;
        assert!(err.is_err());
        assert!(matches!(err.unwrap_err(), DatasetDeleted { .. }));

        //Clear without admin permission
        let err = db.clear_expired_datasets().await;
        assert!(err.is_err());
        assert!(matches!(err.unwrap_err(), PermissionDenied));
    }
}
