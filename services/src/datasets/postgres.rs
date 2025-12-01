use std::path::PathBuf;

use crate::api::handlers::datasets::{AddDatasetTile, DatasetTile, GetDatasetTilesParams};
use crate::api::model::datatypes::SpatialPartition2D;
use crate::api::model::services::{DataPath, UpdateDataset};
use crate::config::Gdal;
use crate::contexts::PostgresDb;
use crate::datasets::listing::Provenance;
use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider};
use crate::datasets::listing::{OrderBy, ProvenanceOutput};
use crate::datasets::storage::{Dataset, DatasetDb, DatasetStore, MetaDataDefinition};
use crate::datasets::upload::{FileId, UploadRootPath, Volumes};
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::datasets::{AddDataset, DatasetIdAndName, DatasetName};
use crate::error::{self, Error, Result};
use crate::identifier;
use crate::permissions::TxPermissionDb;
use crate::permissions::{Permission, RoleId};
use crate::projects::Symbology;
use crate::util::postgres::PostgresErrorExt;
use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::tokio_postgres::Socket;
use bb8_postgres::tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use geoengine_datatypes::dataset::{DataId, DatasetId};
use geoengine_datatypes::error::BoxedResultExt;
use geoengine_datatypes::primitives::{
    CacheHint, RasterQueryRectangle, TimeDimension, TimeInstance, TryIrregularTimeFillIterExt,
    TryRegularTimeFillIterExt,
};
use geoengine_datatypes::primitives::{TimeInterval, VectorQueryRectangle};
use geoengine_datatypes::raster::{GridBoundingBox2D, SpatialGridDefinition};
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::TypedResultDescriptor;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    GdalDatasetGeoTransform, GdalDatasetParameters, GdalLoadingInfo, MultiBandGdalLoadingInfo,
    MultiBandGdalLoadingInfoQueryRectangle, OgrSourceDataset, TileFile,
};
use postgres_types::{FromSql, ToSql};
use tokio_postgres::Transaction;

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

#[async_trait]
pub trait PostgresStorable<Tls>: Send + Sync
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn to_typed_metadata(&self) -> Result<DatasetMetaData<'_>>;
}

pub struct DatasetMetaData<'m> {
    pub meta_data: &'m MetaDataDefinition,
    pub result_descriptor: TypedResultDescriptor,
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

impl<Tls> DatasetDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl<Tls> DatasetProvider for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_datasets(&self, options: DatasetListOptions) -> Result<Vec<DatasetListing>> {
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
                // get the real TypedResultDescriptor and convert it to the API one
                let result_desc: TypedResultDescriptor = row.get(6);
                let result_desc = result_desc.into();

                Result::<DatasetListing>::Ok(DatasetListing {
                    id: row.get(0),
                    name: row.get(1),
                    display_name: row.get(2),
                    description: row.get(3),
                    tags: row.get::<_, Option<Vec<String>>>(4).unwrap_or_default(),
                    source_operator: row.get(5),
                    result_descriptor: result_desc,
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
                d.tags,
                d.data_path
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
            data_path: row.get(9),
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
                p.user_id = $1 AND d.id = $2
            LIMIT 
                1",
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
                p.user_id = $1 AND d.id = $2
            LIMIT 
                1",
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
    for PostgresDb<Tls>
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
    for PostgresDb<Tls>
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
                d.id = $1 AND p.user_id = $2
            LIMIT 
                1",
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
    for PostgresDb<Tls>
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
                d.id = $1 AND p.user_id = $2
            LIMIT 
                1",
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
impl<Tls>
    MetaDataProvider<
        MultiBandGdalLoadingInfo,
        RasterResultDescriptor,
        MultiBandGdalLoadingInfoQueryRectangle,
    > for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn meta_data(
        &self,
        data_id: &DataId,
    ) -> geoengine_operators::util::Result<
        Box<
            dyn MetaData<
                    MultiBandGdalLoadingInfo,
                    RasterResultDescriptor,
                    MultiBandGdalLoadingInfoQueryRectangle,
                >,
        >,
    > {
        let id = data_id
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
        }

        let stmt = tx
            .prepare(
                "
            SELECT
                d.meta_data, d.data_path
            FROM
                user_permitted_datasets p JOIN datasets d
                    ON (p.dataset_id = d.id)
            WHERE
                d.id = $1 AND p.user_id = $2
            LIMIT 
                1",
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

        let result_descriptor = match meta_data {
            MetaDataDefinition::GdalMultiBand(b) => b.result_descriptor,
            _ => return Err(geoengine_operators::error::Error::DataIdTypeMissMatch),
        };

        let data_path: DataPath = row.get(1);

        let data_path =
            match data_path {
                DataPath::Volume(volume_name) =>
                // TODO: after Volume management is implemented, this needs to be adapted
                {
                    Volumes::default()
                        .volumes
                        .iter()
                        .find(|v| v.name == volume_name)
                        .ok_or(Error::UnknownVolumeName {
                            volume_name: volume_name.0.clone(),
                        })
                        .map_err(|e| geoengine_operators::error::Error::MetaData {
                            source: Box::new(e),
                        })?
                        .path
                        .clone()
                }
                DataPath::Upload(upload_id) => upload_id.root_path().map_err(|e| {
                    geoengine_operators::error::Error::MetaData {
                        source: Box::new(e),
                    }
                })?,
            };

        Ok(Box::new(MultiBandGdalLoadingInfoProvider {
            dataset_id: id,
            result_descriptor,
            data_path,
            db: self.clone(),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct MultiBandGdalLoadingInfoProvider<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    dataset_id: DatasetId,
    result_descriptor: RasterResultDescriptor,
    data_path: PathBuf,
    db: PostgresDb<Tls>,
}

async fn create_gap_free_time_steps<Tls>(
    dataset_id: DatasetId,
    query_time: TimeInterval,
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
) -> Result<Vec<TimeInterval>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    // determine the regularity of the dataset
    let time_dim = resolve_time_dim(dataset_id, conn).await?;

    // fill the gaps before, between and after the data tiles to fully cover the query time range
    match time_dim {
        TimeDimension::Regular(regular_time_dimension) => {
            // we do not even need to know the existing tile's time steps here, since the steps are regular
            vec![]
                .into_iter()
                .try_time_regular_range_fill(regular_time_dimension, query_time)
                .collect()
        }
        TimeDimension::Irregular => {
            // query all time steps inside the query, across all spatial tiles and bands
            let time_steps = collect_timesteps_in_query(dataset_id, query_time, conn)
                .await?
                .into_iter()
                .map(Result::Ok)
                .collect::<Vec<Result<TimeInterval>>>();

            // determine the time range to cover by finding the last tile before, and the first tile after the `time_steps`
            let (start, end) = if let Some(Ok(first_time)) = time_steps.first()
                && let Some(Ok(last_time)) = time_steps.last()
            {
                let start = if first_time.start() > query_time.start() {
                    resolve_first_time_end_before(dataset_id, conn, first_time.start()).await?
                } else {
                    query_time.start()
                };

                let end = if last_time.end() < query_time.end() {
                    resolve_first_time_start_after(dataset_id, conn, last_time.end()).await?
                } else {
                    query_time.end()
                };

                (start, end)
            } else {
                let start =
                    resolve_first_time_end_before(dataset_id, conn, query_time.start()).await?;
                let end =
                    resolve_first_time_start_after(dataset_id, conn, query_time.end()).await?;

                (start, end)
            };

            time_steps
                .into_iter()
                .try_time_irregular_range_fill(TimeInterval::new(start, end)?)
                .collect()
        }
    }
}

async fn resolve_first_time_end_before<Tls>(
    dataset_id: DatasetId,
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    start: TimeInstance,
) -> Result<TimeInstance>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let rows = conn
        .query(
            "
                SELECT DISTINCT
                    (time).end
                FROM
                    dataset_tiles
                WHERE
                    dataset_id = $1 AND (time).end <= $2
                ORDER BY
                (time).end DESC
                LIMIT 1",
            &[&dataset_id, &start],
        )
        .await
        .map_err(|e| geoengine_operators::error::Error::MetaData {
            source: Box::new(e),
        })?;
    Ok(if let Some(row) = rows.first() {
        row.get(0)
    } else {
        TimeInstance::MIN
    })
}

async fn resolve_first_time_start_after<Tls>(
    dataset_id: DatasetId,
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
    end: TimeInstance,
) -> Result<TimeInstance>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let rows = conn
        .query(
            "
            SELECT DISTINCT
                (time).start
            FROM
                dataset_tiles
            WHERE
                dataset_id = $1 AND (time).start >= $2
            ORDER BY
            (time).start ASC
            LIMIT 1",
            &[&dataset_id, &end],
        )
        .await
        .map_err(|e| geoengine_operators::error::Error::MetaData {
            source: Box::new(e),
        })?;

    Ok(if let Some(row) = rows.first() {
        row.get(0)
    } else {
        TimeInstance::MAX
    })
}

async fn resolve_time_dim<Tls>(
    dataset_id: DatasetId,
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
) -> Result<TimeDimension>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let time_dim: TimeDimension = conn
        .query_one(
            r#"
        SELECT (result_descriptor).raster."time".dimension
        FROM datasets
        WHERE id = $1;
    "#,
            &[&dataset_id],
        )
        .await?
        .get(0);
    Ok(time_dim)
}

async fn collect_timesteps_in_query<Tls>(
    dataset_id: DatasetId,
    query_time: TimeInterval,
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
) -> Result<Vec<TimeInterval>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let rows = conn
        .query(
            "
            SELECT DISTINCT
                time, (time).start
            FROM
                dataset_tiles
            WHERE
                dataset_id = $1 AND time_interval_intersects(time, $2)
            ORDER BY
               (time).start",
            &[&dataset_id, &query_time],
        )
        .await
        .map_err(|e| geoengine_operators::error::Error::MetaData {
            source: Box::new(e),
        })?;

    let time_steps: Vec<TimeInterval> = rows.into_iter().map(|row| row.get(0)).collect();

    Ok(time_steps)
}

#[async_trait]
impl<Tls>
    MetaData<
        MultiBandGdalLoadingInfo,
        RasterResultDescriptor,
        MultiBandGdalLoadingInfoQueryRectangle,
    > for MultiBandGdalLoadingInfoProvider<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn loading_info(
        &self,
        query: MultiBandGdalLoadingInfoQueryRectangle,
    ) -> Result<MultiBandGdalLoadingInfo, geoengine_operators::error::Error> {
        // NOTE: we query only the files that are needed for answering the query, but to retrieve ALL the time steps in the query we have to consider the whole world and not the restrained bbox

        let conn = self.db.conn_pool.get().await.map_err(|e| {
            geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            }
        })?;

        let files = if query.fetch_tiles {
            // query the files
            let rows = conn
                .query(
                    "
            SELECT
                bbox, time, band, z_index, gdal_params
            FROM
                dataset_tiles
            WHERE
                dataset_id = $1 AND 
                spatial_partition2d_intersects(bbox, $2) AND
                time_interval_intersects(time, $3) AND
                band = ANY($4)
            ORDER BY
                (time).start, band, z_index",
                    &[
                        &self.dataset_id,
                        &query.query_rectangle.spatial_bounds(),
                        &query.query_rectangle.time_interval(),
                        &query.query_rectangle.attributes().as_slice(),
                    ],
                )
                .await
                .map_err(|e| geoengine_operators::error::Error::MetaData {
                    source: Box::new(e),
                })?;

            let files: Vec<TileFile> = rows
                .into_iter()
                .map(|row| TileFile {
                    spatial_partition: row.get(0),
                    time: row.get(1),
                    band: row.get(2),
                    z_index: row.get(3),
                    params: {
                        let mut params: GdalDatasetParameters = row.get(4);
                        // at some point we need to turn the relative file paths of tiles into absolute paths
                        params.file_path = self.data_path.join(&params.file_path);
                        params
                    },
                })
                .collect();

            files
        } else {
            // TODO: configure the resulting loading info to return an error on accessing the files
            vec![]
        };

        let time_steps = create_gap_free_time_steps(
            self.dataset_id,
            query.query_rectangle.time_interval(),
            &conn,
        )
        .await
        .map_err(|e| geoengine_operators::error::Error::MetaData {
            source: Box::new(e),
        })?;

        Ok(MultiBandGdalLoadingInfo::new(
            time_steps,
            files,
            CacheHint::default(), // TODO: implement cache hint, should it be one value for the whole dataset? If so, load it once(!) from the database and add it to the loading info. Otherwise add the cache hint as a new attribute to the tiles.
        ))
    }

    async fn result_descriptor(
        &self,
    ) -> Result<RasterResultDescriptor, geoengine_operators::error::Error> {
        Ok(self.result_descriptor.clone())
    }

    fn box_clone(
        &self,
    ) -> Box<
        dyn MetaData<
                MultiBandGdalLoadingInfo,
                RasterResultDescriptor,
                MultiBandGdalLoadingInfoQueryRectangle,
            >,
    > {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, ToSql, FromSql)]
struct TileKey {
    time: crate::api::model::datatypes::TimeInterval,
    bbox: SpatialPartition2D,
    band: u32,
    z_index: u32,
}

identifier!(DatasetTileId);

#[derive(Debug, Clone, PartialEq, ToSql, FromSql)]
struct TileEntry {
    id: DatasetTileId,
    dataset_id: DatasetId,
    time: crate::api::model::datatypes::TimeInterval,
    bbox: SpatialPartition2D,
    band: u32,
    z_index: u32,
    gdal_params: GdalDatasetParameters,
}

#[async_trait]
impl<Tls> DatasetStore for PostgresDb<Tls>
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
        data_path: Option<DataPath>,
    ) -> Result<DatasetIdAndName> {
        let id = DatasetId::new();
        let name = dataset.name.unwrap_or_else(|| DatasetName {
            namespace: Some(self.session.user.id.to_string()),
            name: id.to_string(),
        });

        tracing::info!(
            "Adding dataset with name: {:?}, tags: {:?}",
            name,
            dataset.tags
        );

        self.check_dataset_namespace(&name)?;

        // TODO: check `data_path` exists?

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
                    tags,
                    data_path
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::text[], $11)",
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
                &data_path,
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

    async fn update_dataset_loading_info(
        &self,
        dataset: DatasetId,
        meta_data: &MetaDataDefinition,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;

        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(dataset.into(), Permission::Owner, &tx)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        let typed_meta_data = meta_data.to_typed_metadata();

        tx.execute(
            "UPDATE datasets SET meta_data = $2, result_descriptor = $3 WHERE id = $1;",
            &[&dataset, &meta_data, &typed_meta_data.result_descriptor],
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
            d.id = $1 AND p.user_id = $2 AND p.max_permission = 'Owner';",
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

    async fn add_dataset_tiles(
        &self,
        dataset: DatasetId,
        tiles: Vec<AddDatasetTile>,
    ) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(dataset.into(), Permission::Owner, &tx)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        validate_time(&tx, dataset, &tiles).await?;

        validate_z_index(&tx, dataset, &tiles).await?;

        batch_insert_tiles(&tx, dataset, &tiles).await?;

        update_dataset_extents(&tx, dataset, &tiles).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn get_dataset_tiles(
        &self,
        dataset: DatasetId,
        params: &GetDatasetTilesParams,
    ) -> Result<Vec<DatasetTile>> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(dataset.into(), Permission::Read, &tx)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        let rows = tx
            .query(
                "
            SELECT
                id, time, bbox, band, z_index, gdal_params
            FROM
                dataset_tiles
            WHERE
                dataset_id = $1
            ORDER BY
                (time).start, 
                band, 
                (bbox).upper_left_coordinate.x, 
                (bbox).upper_left_coordinate.y, 
                z_index
            OFFSET $2
            LIMIT $3",
                &[&dataset, &(params.offset as i64), &(params.limit as i64)],
            )
            .await?;

        let tiles: Vec<DatasetTile> = rows
            .into_iter()
            .map(|row| DatasetTile {
                id: row.get(0),
                time: row.get(1),
                spatial_partition: row.get(2),
                band: row.get(3),
                z_index: row.get(4),
                params: row.get::<_, GdalDatasetParameters>(5).into(),
            })
            .collect();

        Ok(tiles)
    }
}

async fn validate_time(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    tiles: &[AddDatasetTile],
) -> Result<()> {
    // validate the time of the tiles
    let time_dim: TimeDimension = tx
        .query_one(
            r#"
                    SELECT (result_descriptor).raster."time".dimension
                    FROM datasets
                    WHERE id = $1;
                "#,
            &[&dataset],
        )
        .await?
        .get(0);

    match time_dim {
        TimeDimension::Regular(regular_time_dimension) => {
            // check all tiles if they fit to time step
            let invalid_tiles = tiles
                .iter()
                .filter(|tile| !regular_time_dimension.valid_interval(tile.time.into()))
                .collect::<Vec<_>>();

            if !invalid_tiles.is_empty() {
                return Err(Error::DatasetTileRegularTimeConflict {
                    times: invalid_tiles.iter().map(|tile| tile.time).collect(),
                    time_dim: regular_time_dimension,
                });
            }
        }
        TimeDimension::Irregular => {
            // check if any tile conflicts with time of existing tiles. there must be no overlaps.
            let times = tiles.iter().map(|tile| tile.time).collect::<Vec<_>>();

            let incompatible_times = tx
                .query(
                    r#"
                SELECT DISTINCT
                    dt.time,
                    nt
                FROM
                    dataset_tiles dt JOIN
                    unnest($2::"TimeInterval"[]) as nt
                    ON (time_interval_intersects(dt.time, nt))
                WHERE
                    dataset_id = $1 AND (
                        (dt.time).start != (nt).start OR
                        (dt.time)."end" != (nt)."end"
                    );
                "#,
                    &[&dataset, &times],
                )
                .await?;

            if !incompatible_times.is_empty() {
                return Err(Error::DatasetTileTimeConflict {
                    existing_times: incompatible_times.iter().map(|row| row.get(0)).collect(),
                    times: incompatible_times.iter().map(|row| row.get(1)).collect(),
                });
            }
        }
    }

    Ok(())
}

async fn validate_z_index(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    tiles: &[AddDatasetTile],
) -> Result<()> {
    // check, on spatial overlap, if the z-index is different than existing tiles for the same time step and band)
    let tile_keys = tiles
        .iter()
        .map(|tile| TileKey {
            time: tile.time,
            bbox: tile.spatial_partition,
            band: tile.band,
            z_index: tile.z_index,
        })
        .collect::<Vec<_>>();
    let incompatible_z_index = tx
        .query(
            r#"
                SELECT DISTINCT
                    (gdal_params).file_path
                FROM
                    dataset_tiles dt, unnest($2::"TileKey"[]) as tk
                WHERE
                    dataset_id = $1 AND 
                    dt.time = tk.time AND
                    SPATIAL_PARTITION2D_INTERSECTS(dt.bbox, tk.bbox) AND
                    dt.band = tk.band AND
                    dt.z_index = tk.z_index
                ;
                "#,
            &[&dataset, &tile_keys],
        )
        .await?;

    if !incompatible_z_index.is_empty() {
        return Err(Error::DatasetTileZIndexConflict {
            files: incompatible_z_index.iter().map(|row| row.get(0)).collect(),
        });
    }

    Ok(())
}

async fn batch_insert_tiles(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    tiles: &[AddDatasetTile],
) -> Result<()> {
    // batch insert using array unnesting
    let tile_entries = tiles
        .iter()
        .map(|tile| TileEntry {
            id: DatasetTileId::new(),
            dataset_id: dataset,
            time: tile.time,
            bbox: tile.spatial_partition,
            band: tile.band,
            z_index: tile.z_index,
            gdal_params: tile.params.clone().into(),
        })
        .collect::<Vec<_>>();

    tx.execute(
        r#"
            INSERT INTO dataset_tiles (id, dataset_id, time, bbox, band, z_index, gdal_params)
                SELECT * FROM unnest($1::"TileEntry"[]);
            "#,
        &[&tile_entries],
    )
    .await?;

    Ok(())
}

async fn update_dataset_extents(
    tx: &Transaction<'_>,
    dataset: DatasetId,
    tiles: &[AddDatasetTile],
) -> Result<()> {
    // update the dataset extents
    let row = tx
        .query_one(
            r#"
            SELECT
                (result_descriptor).raster.spatial_grid.spatial_grid,
                (result_descriptor).raster."time".bounds
            FROM
                datasets
            WHERE
                id = $1;
            "#,
            &[&dataset],
        )
        .await?;

    let (mut dataset_grid, mut time_bounds): (SpatialGridDefinition, Option<TimeInterval>) =
        (row.get(0), row.get(1));

    for tile in tiles {
        // TODO: handle datasets with flipped y axis?
        let tile_grid = SpatialGridDefinition::new(
            GdalDatasetGeoTransform::from(tile.params.geo_transform).try_into()?,
            GridBoundingBox2D::new_unchecked(
                [0, 0],
                [tile.params.height as isize, tile.params.width as isize],
            ),
        );

        dataset_grid = dataset_grid
                .merge(&tile_grid)
                .expect("grids should be compatible because the compatibility was checked before inserting tiles");

        if let Some(time_bounds) = &mut time_bounds {
            *time_bounds = time_bounds.extend(&tile.time.into());
        }
    }

    tx.execute(
        r#"
            UPDATE datasets
            SET 
                result_descriptor.raster.spatial_grid.spatial_grid = $2,
                result_descriptor.raster."time".bounds = $3,
                meta_data.gdal_multi_band.result_descriptor.spatial_grid.spatial_grid = $2,
                meta_data.gdal_multi_band.result_descriptor."time".bounds = $3
            WHERE id = $1;
            "#,
        &[&dataset, &dataset_grid, &time_bounds],
    )
    .await?;

    Ok(())
}

#[async_trait]
impl<Tls> UploadDb for PostgresDb<Tls>
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::{
        contexts::{ApplicationContext, PostgresContext, SessionContext},
        ge_context,
        permissions::PermissionDb,
        users::{UserAuth, UserSession},
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
    async fn it_autocompletes_datasets(app_ctx: PostgresContext<NoTls>) {
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

        assert!(
            db_b.dataset_autocomplete_search(None, "Ogr".to_owned(), 10, 0)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            db_b.dataset_autocomplete_search(
                Some(vec!["upload".to_string()]),
                "Ogr".to_owned(),
                10,
                0
            )
            .await
            .unwrap()
            .is_empty()
        );
    }

    #[ge_context::test]
    async fn it_loads_own_datasets(app_ctx: PostgresContext<NoTls>) {
        let session_a = app_ctx.create_anonymous_session().await.unwrap();

        let db_a = app_ctx.session_context(session_a.clone()).db();

        let DatasetIdAndName {
            id: dataset_id,
            name: _,
        } = add_single_dataset(&db_a, &session_a).await;

        // we are already owner, but we give the permission again to test the permission check
        db_a.add_permission(session_a.user.id.into(), dataset_id, Permission::Read)
            .await
            .unwrap();

        db_a.load_loading_info(&dataset_id).await.unwrap();
        let _: Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor, VectorQueryRectangle>> =
            db_a.meta_data(&DataId::from(dataset_id)).await.unwrap();
    }

    async fn add_single_dataset(db: &PostgresDb<NoTls>, session: &UserSession) -> DatasetIdAndName {
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
            None,
        )
        .await
        .unwrap()
    }
}
