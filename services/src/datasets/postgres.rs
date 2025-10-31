use crate::api::handlers::datasets::DatasetTile;
use crate::api::model::datatypes::SpatialPartition2D;
use crate::api::model::services::UpdateDataset;
use crate::contexts::PostgresDb;
use crate::datasets::listing::Provenance;
use crate::datasets::listing::{DatasetListOptions, DatasetListing, DatasetProvider};
use crate::datasets::listing::{OrderBy, ProvenanceOutput};
use crate::datasets::storage::{Dataset, DatasetDb, DatasetStore, MetaDataDefinition};
use crate::datasets::upload::FileId;
use crate::datasets::upload::{Upload, UploadDb, UploadId};
use crate::datasets::{AddDataset, DatasetIdAndName, DatasetName};
use crate::error::{self, Error, Result};
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
use geoengine_datatypes::primitives::{CacheHint, RasterQueryRectangle, TimeInstance};
use geoengine_datatypes::primitives::{TimeInterval, VectorQueryRectangle};
use geoengine_datatypes::util::Identifier;
use geoengine_operators::engine::TypedResultDescriptor;
use geoengine_operators::engine::{
    MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor,
};
use geoengine_operators::mock::MockDatasetDataSourceLoadingInfo;
use geoengine_operators::source::{
    GdalDatasetParameters, GdalLoadingInfo, MultiBandGdalLoadingInfo,
    MultiBandGdalLoadingInfoQueryRectangle, OgrSourceDataset, TileFile,
};
use postgres_types::{FromSql, ToSql};

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

        let result_descriptor = match meta_data {
            MetaDataDefinition::GdalMultiBand(b) => b.result_descriptor,
            _ => return Err(geoengine_operators::error::Error::DataIdTypeMissMatch),
        };

        Ok(Box::new(MultiBandGdalLoadingInfoProvider {
            dataset_id: id,
            result_descriptor,
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
    // query all time steps inside the query, across all spatial tiles and bands
    let time_steps = collect_timesteps_in_query(dataset_id, query_time, conn).await?;

    // handle case where no time steps are found
    if time_steps.is_empty() {
        // Query for the closest time step before the query time
        let time =
            resolve_time_from_first_tile_before_and_after_query(dataset_id, query_time, conn)
                .await?;

        return Ok(vec![time]);
    }

    // fill the gaps in the returned time steps
    let mut filled_time_steps = vec![];

    for time_step in time_steps {
        let Some(last_step) = filled_time_steps.last() else {
            filled_time_steps.push(time_step);
            continue;
        };

        if last_step.end() < time_step.start() {
            // gap, add the gap
            filled_time_steps.push(
                TimeInterval::new(last_step.end(), time_step.start())
                    .expect("start must be before end"),
            );
        }

        filled_time_steps.push(time_step);
    }

    // fill the gaps before and after the data/query time
    let (time_start, time_end) = match (filled_time_steps.first(), filled_time_steps.last()) {
        (Some(first), Some(last)) => (first.start(), last.end()),
        _ => (query_time.start(), query_time.end()),
    };

    // fill possible gaps at the beginning and end of the data/query time

    if time_start > query_time.start() {
        // last tile before query
        let rows = conn
            .query(
                "
            SELECT DISTINCT
                (time).end
            FROM
                dataset_tiles
            WHERE
                dataset_id = $1 AND (time).end < $2
            ORDER BY
               (time).end DESC
            LIMIT 1",
                &[&dataset_id, &time_start],
            )
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?;

        let start = if let Some(row) = rows.first() {
            row.get(0)
        } else {
            TimeInstance::MIN
        };

        filled_time_steps.insert(
            0,
            TimeInterval::new(start, time_start).expect("start must be before end"),
        );
    }

    if time_end < query_time.end() {
        // first tile after query

        let rows = conn
            .query(
                "
            SELECT DISTINCT
                (time).start
            FROM
                dataset_tiles
            WHERE
                dataset_id = $1 AND (time).start > $2
            ORDER BY
               (time).start ASC
            LIMIT 1",
                &[
                    &dataset_id,
                    &filled_time_steps
                        .last()
                        .expect("at least one time step")
                        .start(),
                ],
            )
            .await
            .map_err(|e| geoengine_operators::error::Error::MetaData {
                source: Box::new(e),
            })?;

        let end = if let Some(row) = rows.first() {
            row.get(0)
        } else {
            TimeInstance::MAX
        };

        filled_time_steps.push(TimeInterval::new(time_end, end).expect("start must be before end"));
    }

    Ok(filled_time_steps)
}

async fn resolve_time_from_first_tile_before_and_after_query<Tls>(
    dataset_id: DatasetId,
    query_time: TimeInterval,
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
) -> Result<TimeInterval>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let before_rows = conn
        .query(
            "
                SELECT
                    (time).end
                FROM
                    dataset_tiles
                WHERE
                    dataset_id = $1 AND (time).end <= $2
                ORDER BY
                   (time).end DESC
                LIMIT 1",
            &[&dataset_id, &query_time.start()],
        )
        .await
        .map_err(|e| geoengine_operators::error::Error::MetaData {
            source: Box::new(e),
        })?;

    let after_rows = conn
        .query(
            "
                SELECT
                    (time).start
                FROM
                    dataset_tiles
                WHERE
                    dataset_id = $1 AND (time).start >= $2
                ORDER BY
                   (time).start ASC
                LIMIT 1",
            &[&dataset_id, &query_time.end()],
        )
        .await
        .map_err(|e| geoengine_operators::error::Error::MetaData {
            source: Box::new(e),
        })?;

    let start = if let Some(row) = before_rows.first() {
        row.get(0)
    } else {
        TimeInstance::MIN
    };

    let end = if let Some(row) = after_rows.first() {
        row.get(0)
    } else {
        TimeInstance::MAX
    };

    Ok(TimeInterval::new(start, end).expect("start must be before end"))
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
                    &query.spatial_bounds(),
                    &query.time_interval(),
                    &query.attributes().as_slice(),
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
                params: row.get(4),
            })
            .collect();

        let time_steps = create_gap_free_time_steps(self.dataset_id, query.time_interval(), &conn)
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

#[derive(Debug, Clone, PartialEq, ToSql, FromSql)]
struct TileEntry {
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

    async fn add_dataset_tiles(&self, dataset: DatasetId, tiles: Vec<DatasetTile>) -> Result<()> {
        let mut conn = self.conn_pool.get().await?;
        let tx = conn.build_transaction().start().await?;

        self.ensure_permission_in_tx(dataset.into(), Permission::Owner, &tx)
            .await
            .boxed_context(crate::error::PermissionDb)?;

        // check if any tile conflicts with existing tiles
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

        let tile_keys = tiles
            .iter()
            .map(|tile| TileKey {
                time: tile.time,
                bbox: tile.spatial_partition,
                band: tile.band,
                z_index: tile.z_index,
            })
            .collect::<Vec<_>>();

        // check, on spatial overlap, if the z-index is different than existing tiles for the same time step and band)
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

        // batch insert using array unnesting
        let tile_entries = tiles
            .into_iter()
            .map(|tile| TileEntry {
                dataset_id: dataset,
                time: tile.time,
                bbox: tile.spatial_partition,
                band: tile.band,
                z_index: tile.z_index,
                gdal_params: tile.params.into(),
            })
            .collect::<Vec<_>>();

        tx.execute(
            r#"
            INSERT INTO dataset_tiles (dataset_id, time, bbox, band, z_index, gdal_params)
                SELECT * FROM unnest($1::"TileEntry"[]);
            "#,
            &[&tile_entries],
        )
        .await?;

        tx.commit().await?;

        Ok(())
    }
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
        )
        .await
        .unwrap()
    }
}
