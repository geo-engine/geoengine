use super::{
    error,
    metadata::{Creator, DataRange, NetCdfGroupMetadata, NetCdfOverviewMetadata},
    NetCdfEntity, NetCdfGroup, NetCdfOverview, Result,
};
use crate::contexts::PostgresDb;
use async_trait::async_trait;
use bb8_postgres::{bb8::PooledConnection, PostgresConnectionManager};
use geoengine_datatypes::{
    dataset::DataProviderId, error::BoxedResultExt, primitives::TimeInstance,
};
use geoengine_operators::source::GdalMetaDataList;
use ouroboros::self_referencing;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket, Transaction,
};

#[async_trait]
/// Storage for the [`NetCdfCfDataProvider`] provider
pub trait NetCdfCfProviderDb: Send + Sync {
    type Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static;

    async fn read_access(
        &self,
        provider_id: DataProviderId,
    ) -> Result<NetCdfCfReadAccess<Self::Tls>>
    where
        <Self::Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
        <Self::Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
        <<Self::Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send;

    async fn write_access(
        &self,
        provider_id: DataProviderId,
    ) -> Result<NetCdfCfWriteAccess<Self::Tls>>
    where
        <Self::Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
        <Self::Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
        <<Self::Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send;
}

#[async_trait]
impl<Tls> NetCdfCfProviderDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Tls = Tls;

    async fn read_access(&self, provider_id: DataProviderId) -> Result<NetCdfCfReadAccess<Tls>> {
        let read_access = _NetCdfCfReadAccessAsyncSendTryBuilder {
            client: self
                .conn_pool
                .get_owned()
                .await
                .boxed_context(error::DatabaseConnection)?,
            transaction_builder: |connection| Box::pin(readonly_transaction(connection)),
        }
        .try_build()
        .await?;

        Ok(NetCdfCfReadAccess {
            this: read_access,
            provider_id,
        })
    }

    async fn write_access(&self, provider_id: DataProviderId) -> Result<NetCdfCfWriteAccess<Tls>> {
        let write_access = _NetCdfCfWriteAccessAsyncSendTryBuilder {
            client: self
                .conn_pool
                .get_owned()
                .await
                .boxed_context(error::DatabaseConnection)?,
            transaction_builder: |connection| {
                Box::pin(async move { deferred_write_transaction(connection).await.map(Some) })
            },
        }
        .try_build()
        .await?;

        Ok(NetCdfCfWriteAccess {
            this: write_access,
            provider_id,
        })
    }
}

// TODO: move
#[cfg(feature = "pro")]
#[async_trait]
impl<Tls> NetCdfCfProviderDb for crate::pro::contexts::ProPostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    type Tls = Tls;

    async fn read_access(&self, provider_id: DataProviderId) -> Result<NetCdfCfReadAccess<Tls>> {
        let read_access = _NetCdfCfReadAccessAsyncSendTryBuilder {
            client: self
                .conn_pool
                .get_owned()
                .await
                .boxed_context(error::DatabaseConnection)?,
            transaction_builder: |connection| Box::pin(readonly_transaction(connection)),
        }
        .try_build()
        .await?;

        Ok(NetCdfCfReadAccess {
            this: read_access,
            provider_id,
        })
    }

    async fn write_access(&self, provider_id: DataProviderId) -> Result<NetCdfCfWriteAccess<Tls>> {
        let write_access = _NetCdfCfWriteAccessAsyncSendTryBuilder {
            client: self
                .conn_pool
                .get_owned()
                .await
                .boxed_context(error::DatabaseConnection)?,
            transaction_builder: |connection| {
                Box::pin(async move { deferred_write_transaction(connection).await.map(Some) })
            },
        }
        .try_build()
        .await?;

        Ok(NetCdfCfWriteAccess {
            this: write_access,
            provider_id,
        })
    }
}

/// Creates a read-only transaction that uses snapshot isolation
async fn readonly_transaction<'db, 't, Tls>(
    db_connection: &'t mut bb8_postgres::bb8::PooledConnection<'db, PostgresConnectionManager<Tls>>,
) -> Result<tokio_postgres::Transaction<'t>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    db_connection
        .build_transaction()
        .read_only(true)
        .deferrable(true) // get snapshot isolation
        .start()
        .await
        .boxed_context(error::DatabaseTransaction)
}

/// Creates a write transaction that defers constraints
async fn deferred_write_transaction<'db, 't, Tls>(
    db_connection: &'t mut bb8_postgres::bb8::PooledConnection<'db, PostgresConnectionManager<Tls>>,
) -> Result<tokio_postgres::Transaction<'t>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let transaction = db_connection
        .transaction()
        .await
        .boxed_context(error::DatabaseTransaction)?;

    // check constraints at the end to speed up insertions
    transaction
        .batch_execute("SET CONSTRAINTS ALL DEFERRED")
        .await
        .boxed_context(error::UnexpectedExecution)?;

    Ok(transaction)
}

#[self_referencing]
struct _NetCdfCfReadAccess<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    client: PooledConnection<'static, PostgresConnectionManager<Tls>>,
    #[borrows(mut client)]
    #[covariant] // TODO: check
    transaction: Transaction<'this>,
}

pub struct NetCdfCfReadAccess<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    this: _NetCdfCfReadAccess<Tls>,
    provider_id: DataProviderId,
}

impl<Tls> NetCdfCfReadAccess<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn db_transaction(&self) -> &Transaction {
        self.this.borrow_transaction()
    }

    pub async fn overview_metadata(
        &self,
        file_name: &str,
    ) -> Result<Option<NetCdfOverviewMetadata>> {
        let Some(row) = self
            .db_transaction()
            .query_opt(
                r#"
                SELECT
                    file_name,
                    title,
                    summary,
                    spatial_reference :: "SpatialReference",
                    colorizer :: "Colorizer",
                    creator_name,
                    creator_email,
                    creator_institution
                FROM
                    ebv_provider_overviews
                WHERE
                    provider_id = $1 AND
                    file_name = $2
            "#,
                &[&self.provider_id, &file_name],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?
        else {
            return Ok(None);
        };

        let metadata = NetCdfOverviewMetadata {
            file_name: row.get("file_name"),
            title: row.get("title"),
            summary: row.get("summary"),
            spatial_reference: row.get("spatial_reference"),
            colorizer: row.get("colorizer"),
            creator: Creator::new(
                row.get("creator_name"),
                row.get("creator_email"),
                row.get("creator_institution"),
            ),
        };

        Ok(Some(metadata))
    }

    pub async fn group_metadata(
        &self,
        file_name: &str,
        group_path: &[String],
    ) -> Result<Option<NetCdfGroupMetadata>> {
        let Some(row) = self
            .db_transaction()
            .query_opt(
                r#"
                SELECT
                    name,
                    title,
                    description,
                    data_type :: "RasterDataType",
                    data_range,
                    unit
                FROM
                    ebv_provider_groups
                WHERE
                    provider_id = $1 AND
                    file_name = $2 AND
                    name = $3
            "#,
                &[&self.provider_id, &file_name, &group_path],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?
        else {
            return Ok(None);
        };

        let metadata = NetCdfGroupMetadata {
            name: row.get::<_, Vec<String>>("name").pop().unwrap_or_default(),
            title: row.get("title"),
            description: row.get("description"),
            data_type: row.get("data_type"),
            data_range: row.get::<_, Option<DataRange>>("data_range"),
            unit: row.get("unit"),
        };

        Ok(Some(metadata))
    }

    pub async fn subgroup_metadata(
        &self,
        file_name: &str,
        group_path: &[String],
        offset: u32,
        limit: u32,
    ) -> Result<Vec<NetCdfGroupMetadata>> {
        let subgroups = self
            .db_transaction()
            .query(
                r#"
                SELECT
                    name,
                    title,
                    description,
                    data_type :: "RasterDataType",
                    data_range,
                    unit
                FROM ebv_provider_groups
                WHERE
                    provider_id = $1 AND
                    file_name = $2 AND
                    name[:$4] = $3 AND
                    array_length(name, 1) = ($4 + 1)
                ORDER BY name ASC
                OFFSET $5 LIMIT $6
                "#,
                &[
                    &self.provider_id,
                    &file_name,
                    &group_path,
                    &(group_path.len() as i32),
                    &i64::from(offset),
                    &i64::from(limit),
                ],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?
            .into_iter()
            .map(|row| NetCdfGroupMetadata {
                name: row.get::<_, Vec<String>>("name").pop().unwrap_or_default(),
                title: row.get("title"),
                description: row.get("description"),
                data_type: row.get("data_type"),
                data_range: row.get::<_, Option<DataRange>>("data_range"),
                unit: row.get("unit"),
            })
            .collect();

        Ok(subgroups)
    }

    pub async fn entities(
        &self,
        file_name: &str,
        offset: u32,
        limit: u32,
    ) -> Result<impl Iterator<Item = NetCdfEntity>> {
        let entities = self
            .db_transaction()
            .query(
                "
                SELECT
                    id, name
                FROM
                    ebv_provider_entities
                WHERE
                    provider_id = $1 AND
                    file_name = $2
                ORDER BY name ASC
                OFFSET $3
                LIMIT $4
                ",
                &[
                    &self.provider_id,
                    &file_name,
                    &i64::from(offset),
                    &i64::from(limit),
                ],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?
            .into_iter()
            .map(|row| NetCdfEntity {
                name: row.get("name"),
                id: row.get::<_, i64>("id") as usize,
            });

        Ok(entities)
    }

    pub async fn entity(&self, file_name: &str, entity: usize) -> Result<Option<NetCdfEntity>> {
        let Some(row) = self
            .db_transaction()
            .query_opt(
                "
                SELECT
                    id,
                    name 
                FROM
                    ebv_provider_entities
                WHERE
                    provider_id = $1 AND
                    file_name = $2 AND
                    id = $3
                ",
                &[&self.provider_id, &file_name, &(entity as i64)],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?
        else {
            return Ok(None);
        };

        let entity = NetCdfEntity {
            name: row.get("name"),
            id: row.get::<_, i64>("id") as usize,
        };

        Ok(Some(entity))
    }

    pub async fn data_range(
        &self,
        file_name: &str,
        group_path: &[String],
    ) -> Result<Option<DataRange>> {
        let Some(row) = self
            .db_transaction()
            .query_opt(
                "
                SELECT
                    data_range
                FROM ebv_provider_groups
                WHERE
                    provider_id = $1 AND
                    file_name = $2 AND
                    name = $3
                ",
                &[&self.provider_id, &file_name, &group_path],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?
        else {
            return Ok(None);
        };

        let data_range = row.get::<_, Option<DataRange>>("data_range");

        Ok(data_range)
    }

    pub async fn timestamps(&self, file_name: &str) -> Result<Vec<TimeInstance>> {
        let timestamps = self
            .db_transaction()
            .query(
                r#"
                SELECT
                    "time"
                FROM ebv_provider_timestamps
                WHERE
                    provider_id = $1 AND
                    file_name = $2
                ORDER BY "time" ASC
                "#,
                &[&self.provider_id, &file_name],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?
            .into_iter()
            .map(|row| row.get("time"))
            .collect();

        Ok(timestamps)
    }

    pub async fn loading_info(
        &self,
        file_name: &str,
        group_path: &[String],
        entity: usize,
    ) -> Result<Option<GdalMetaDataList>> {
        let Some(row) = self
            .db_transaction()
            .query_opt(
                r#"
                SELECT meta_data :: "GdalMetaDataList"
                FROM ebv_provider_loading_infos
                WHERE
                    provider_id = $1 AND
                    file_name = $2 AND
                    group_names = $3 :: TEXT[] AND
                    entity_id = $4
            "#,
                &[&self.provider_id, &file_name, &group_path, &(entity as i64)],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?
        else {
            return Ok(None);
        };

        let meta_data: GdalMetaDataList = row.get(0);

        Ok(Some(meta_data))
    }
}

#[self_referencing]
struct _NetCdfCfWriteAccess<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    client: PooledConnection<'static, PostgresConnectionManager<Tls>>,
    #[borrows(mut client)]
    #[covariant] // TODO: check
    transaction: Option<Transaction<'this>>,
}

pub struct NetCdfCfWriteAccess<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    this: _NetCdfCfWriteAccess<Tls>,
    provider_id: DataProviderId,
}

impl<Tls> NetCdfCfWriteAccess<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn db_transaction(&self) -> &Transaction {
        self.this
            .borrow_transaction()
            .as_ref()
            .expect("There is a transaction until commit")
    }

    #[cfg(test)]
    /// only for testing
    pub(super) fn test_db_transaction(&self) -> &Transaction {
        self.db_transaction()
    }

    pub async fn commit(mut self) -> Result<()> {
        self.this
            .with_transaction_mut(|transaction| {
                let transaction = transaction
                    .take()
                    .expect("There is a transaction until commit");
                transaction.commit()
            })
            .await
            .boxed_context(error::DatabaseTransactionCommit)
    }

    pub async fn overview_exists(&self, file_name: &str) -> Result<bool> {
        self.db_transaction()
            .query_opt(
                "
                SELECT
                    1
                FROM
                    ebv_provider_overviews
                WHERE
                    provider_id = $1 AND
                    file_name = $2
                ",
                &[&self.provider_id, &file_name],
            )
            .await
            .boxed_context(error::UnexpectedExecution)
            .map(|row| row.is_some())
    }

    pub async fn remove_overview(&self, file_name: &str) -> Result<bool> {
        let removed_rows = self
            .db_transaction()
            .execute(
                "
                DELETE FROM
                    ebv_provider_overviews
                WHERE
                    provider_id = $1 AND
                    file_name = $2
                ",
                &[&self.provider_id, &file_name],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;

        Ok(removed_rows > 0)
    }

    pub async fn insert_overview(&self, metadata: &NetCdfOverview) -> Result<()> {
        self.db_transaction()
            .execute(
                "
                INSERT INTO ebv_provider_overviews (
                    provider_id,
                    file_name,
                    title,
                    summary,
                    spatial_reference,
                    colorizer,
                    creator_name,
                    creator_email,
                    creator_institution
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9
                );
                ",
                &[
                    &self.provider_id,
                    &metadata.file_name,
                    &metadata.title,
                    &metadata.summary,
                    &metadata.spatial_reference,
                    &metadata.colorizer,
                    &metadata.creator_name,
                    &metadata.creator_email,
                    &metadata.creator_institution,
                ],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;

        Ok(())
    }

    // TODO: use prepared statement
    pub async fn insert_group(
        &self,
        file_name: &str,
        group_path: &[&str],
        group: &NetCdfGroup,
    ) -> Result<()> {
        self.db_transaction()
            .execute(
                "
                INSERT INTO ebv_provider_groups (
                    provider_id,
                    file_name,
                    name,
                    title,
                    description,
                    data_range,
                    unit,
                    data_type
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8
                );
                ",
                &[
                    &self.provider_id,
                    &file_name,
                    &group_path,
                    &group.title,
                    &group.description,
                    &group.data_range,
                    &group.unit,
                    &group.data_type,
                ],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;

        Ok(())
    }

    // TODO: use prepared statement
    pub async fn insert_entity(&self, file_name: &str, entity: &NetCdfEntity) -> Result<()> {
        self.db_transaction()
            .execute(
                "
                INSERT INTO ebv_provider_entities (
                    provider_id,
                    file_name,
                    id,
                    name
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4
                );
                ",
                &[
                    &self.provider_id,
                    &file_name,
                    &(entity.id as i64),
                    &entity.name,
                ],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;

        Ok(())
    }

    // TODO: use prepared statement
    pub async fn insert_timestamp(&self, file_name: &str, time: &TimeInstance) -> Result<()> {
        self.db_transaction()
            .execute(
                r#"
                INSERT INTO ebv_provider_timestamps (
                    provider_id,
                    file_name,
                    "time"
                ) VALUES (
                    $1,
                    $2,
                    $3
                );
                "#,
                &[&self.provider_id, &file_name, &time],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;

        Ok(())
    }

    // TODO: use prepared statement
    pub async fn insert_loading_info(
        &self,
        file_name: &str,
        group_path: &[String],
        entity: usize,
        loading_info: &GdalMetaDataList,
    ) -> Result<()> {
        self.db_transaction()
            .execute(
                r#"
                INSERT INTO ebv_provider_loading_infos (
                    provider_id,
                    file_name,
                    group_names,
                    entity_id,
                    meta_data
                ) VALUES (
                    $1,
                    $2,
                    $3 :: TEXT[],
                    $4,
                    $5 :: "GdalMetaDataList"
                );
                "#,
                &[
                    &self.provider_id,
                    &file_name,
                    &group_path,
                    &(entity as i64),
                    &loading_info,
                ],
            )
            .await
            .boxed_context(error::CannotStoreLoadingInfo)?;

        Ok(())
    }
}
