use super::{
    NetCdfCf4DDatasetId, NetCdfCf4DProviderError, NetCdfEntity, NetCdfGroup, NetCdfOverview,
    Result, determine_data_range_and_colorizer, error,
    loading::{
        LayerCollectionIdFn, LayerCollectionParts, create_layer, create_layer_collection_from_parts,
    },
    metadata::{Creator, DataRange, NetCdfGroupMetadata, NetCdfOverviewMetadata},
    overviews::LoadingInfoMetadata,
};
use crate::layers::{
    layer::{Layer, LayerCollection, LayerCollectionListOptions, ProviderLayerId},
    listing::LayerCollectionId,
};
use async_trait::async_trait;
use bb8_postgres::{PostgresConnectionManager, bb8::PooledConnection};
use geoengine_datatypes::{
    dataset::{DataProviderId, LayerId},
    error::BoxedResultExt,
    primitives::TimeInstance,
};
use geoengine_operators::source::GdalMetaDataList;
use snafu::ResultExt;
use std::sync::Arc;
use tokio_postgres::{
    Socket, Transaction,
    tls::{MakeTlsConnect, TlsConnect},
};

#[async_trait]
/// Storage for the [`NetCdfCfDataProvider`] provider
pub trait NetCdfCfProviderDb: Send + Sync {
    async fn lock_overview(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool>;

    async fn unlock_overview(&self, provider_id: DataProviderId, file_name: &str) -> Result<()>;

    async fn overviews_exist(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool>;

    async fn store_overview_metadata(
        &self,
        provider_id: DataProviderId,
        metadata: &NetCdfOverview,
        loading_infos: Vec<LoadingInfoMetadata>,
    ) -> Result<()>;

    async fn remove_overviews(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool>;

    async fn layer_collection<ID: LayerCollectionIdFn>(
        &self,
        NetCdfDatabaseListingConfig {
            provider_id,
            collection,
            file_name,
            group_path,
            options,
        }: NetCdfDatabaseListingConfig<'_>,
        id_fn: Arc<ID>,
    ) -> Result<Option<LayerCollection>>;

    async fn layer(
        &self,
        provider_id: DataProviderId,
        layer_id: &LayerId,
        file_name: &str,
        group_path: &[String],
        entity: usize,
    ) -> Result<Option<Layer>>;

    async fn loading_info(
        &self,
        provider_id: DataProviderId,
        file_name: &str,
        group_path: &[String],
        entity: usize,
    ) -> Result<Option<GdalMetaDataList>>;

    #[cfg(test)]
    /// only for testing
    async fn test_execute_with_transaction<T>(
        &self,
        statement: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<(), tokio_postgres::Error>
    where
        T: ?Sized + tokio_postgres::ToStatement + Sync;
}

#[async_trait]
impl<Tls> NetCdfCfProviderDb for crate::contexts::PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn lock_overview(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool> {
        lock_overview(get_connection!(self.conn_pool), provider_id, file_name).await
    }

    async fn unlock_overview(&self, provider_id: DataProviderId, file_name: &str) -> Result<()> {
        unlock_overview(get_connection!(self.conn_pool), provider_id, file_name).await
    }

    async fn overviews_exist(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool> {
        overviews_exist(get_connection!(self.conn_pool), provider_id, file_name).await
    }

    async fn store_overview_metadata(
        &self,
        provider_id: DataProviderId,
        metadata: &NetCdfOverview,
        loading_infos: Vec<LoadingInfoMetadata>,
    ) -> Result<()> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = deferred_write_transaction!(connection);

        store_overview_metadata(transaction, provider_id, metadata, loading_infos).await
    }

    async fn remove_overviews(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = deferred_write_transaction!(connection);

        let res = remove_overviews(&transaction, provider_id, file_name).await;

        transaction
            .commit()
            .await
            .boxed_context(crate::datasets::external::netcdfcf::error::DatabaseTransaction)?;

        res
    }

    async fn layer_collection<ID: LayerCollectionIdFn>(
        &self,
        config: NetCdfDatabaseListingConfig<'_>,
        id_fn: Arc<ID>,
    ) -> Result<Option<LayerCollection>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction!(connection);

        layer_collection(transaction, config, id_fn).await
    }

    async fn layer(
        &self,
        provider_id: DataProviderId,
        layer_id: &LayerId,
        file_name: &str,
        group_path: &[String],
        entity_id: usize,
    ) -> Result<Option<Layer>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction!(connection);

        layer(
            transaction,
            provider_id,
            layer_id,
            file_name,
            group_path,
            entity_id,
        )
        .await
    }

    async fn loading_info(
        &self,
        provider_id: DataProviderId,
        file_name: &str,
        group_path: &[String],
        entity: usize,
    ) -> Result<Option<GdalMetaDataList>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction!(connection);

        loading_info(transaction, provider_id, file_name, group_path, entity).await
    }

    #[cfg(test)]
    async fn test_execute_with_transaction<T>(
        &self,
        statement: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<(), tokio_postgres::Error>
    where
        T: ?Sized + tokio_postgres::ToStatement + Sync,
    {
        let mut connection = self
            .conn_pool
            .get()
            .await
            .expect("failed to get connection in test");
        let transaction = connection.transaction().await?;

        transaction.execute(statement, params).await?;

        transaction.commit().await
    }
}

/// Gets a connection from the pool
macro_rules! get_connection {
    ($connection:expr) => {{
        use geoengine_datatypes::error::BoxedResultExt;
        $connection
            .get()
            .await
            .boxed_context(crate::datasets::external::netcdfcf::error::DatabaseConnection)?
    }};
}
pub(crate) use get_connection;

/// Starts a read-only snapshot-isolated transaction for a multiple read
macro_rules! readonly_transaction {
    ($connection:expr) => {{
        use geoengine_datatypes::error::BoxedResultExt;
        $connection
            .build_transaction()
            .read_only(true)
            .deferrable(true) // get snapshot isolation
            .start()
            .await
            .boxed_context(crate::datasets::external::netcdfcf::error::DatabaseTransaction)?
    }};
}
pub(crate) use readonly_transaction;

/// Starts a deferred write transaction
macro_rules! deferred_write_transaction {
    ($connection:expr) => {{
        use geoengine_datatypes::error::BoxedResultExt;

        let transaction = $connection
            .transaction()
            .await
            .boxed_context(crate::datasets::external::netcdfcf::error::DatabaseTransaction)?;

        // check constraints at the end to speed up insertions
        transaction
            .batch_execute("SET CONSTRAINTS ALL DEFERRED")
            .await
            .boxed_context(crate::datasets::external::netcdfcf::error::UnexpectedExecution)?;

        transaction
    }};
}
pub(crate) use deferred_write_transaction;

pub(crate) async fn lock_overview<Tls>(
    connection: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    provider_id: DataProviderId,
    file_name: &str,
) -> Result<bool>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let updated_rows = connection
        .execute(
            "
            INSERT INTO ebv_provider_dataset_locks (
                provider_id,
                file_name
            ) VALUES (
                $1,
                $2
            ) ON CONFLICT DO NOTHING;
        ",
            &[&provider_id, &file_name],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    Ok(updated_rows > 0)
}

pub(crate) async fn unlock_overview<Tls>(
    connection: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    provider_id: DataProviderId,
    file_name: &str,
) -> Result<()>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    connection
        .execute(
            "
            DELETE FROM ebv_provider_dataset_locks
            WHERE
                provider_id = $1 AND
                file_name = $2
            ;
        ",
            &[&provider_id, &file_name],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    Ok(())
}

pub(crate) async fn overviews_exist<Tls>(
    connection: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    provider_id: DataProviderId,
    file_name: &str,
) -> Result<bool>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let exists = connection
        .query_one(
            "
            SELECT EXISTS (
                SELECT 1
                FROM ebv_provider_overviews
                WHERE
                    provider_id = $1 AND
                    file_name = $2
                EXCEPT
                SELECT 1
                FROM ebv_provider_dataset_locks
                WHERE
                    provider_id = $1 AND
                    file_name = $2
            )
        ",
            &[&provider_id, &file_name],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?
        .get::<_, bool>(0);

    Ok(exists)
}

pub(crate) async fn store_overview_metadata(
    transaction: Transaction<'_>,
    provider_id: DataProviderId,
    metadata: &NetCdfOverview,
    loading_infos: Vec<LoadingInfoMetadata>,
) -> Result<()> {
    if remove_overviews(&transaction, provider_id, &metadata.file_name).await? {
        log::debug!(
            "Removed {file_name} from the database before re-inserting the metadata",
            file_name = metadata.file_name
        );
    }

    insert_overview(&transaction, provider_id, metadata).await?;

    insert_groups(
        &transaction,
        &provider_id,
        &metadata.file_name,
        &metadata.groups,
    )
    .await?;

    insert_entities(
        &transaction,
        &provider_id,
        &metadata.file_name,
        &metadata.entities,
    )
    .await?;

    insert_timestamps(
        &transaction,
        &provider_id,
        &metadata.file_name,
        metadata.time_coverage.time_steps(),
    )
    .await?;

    insert_loading_infos(
        &transaction,
        &provider_id,
        &metadata.file_name,
        &loading_infos,
    )
    .await?;

    transaction
        .commit()
        .await
        .boxed_context(error::DatabaseTransactionCommit)
}

pub(crate) async fn layer_collection<ID: LayerCollectionIdFn>(
    transaction: Transaction<'_>,
    NetCdfDatabaseListingConfig {
        provider_id,
        collection,
        file_name,
        group_path,
        options,
    }: NetCdfDatabaseListingConfig<'_>,
    id_fn: Arc<ID>,
) -> Result<Option<LayerCollection>> {
    let Some(overview_metadata) = overview_metadata(&transaction, provider_id, file_name).await?
    else {
        return Ok(None);
    };

    let group_metadata = group_metadata(&transaction, provider_id, file_name, group_path).await?;

    let subgroups: Vec<NetCdfGroupMetadata> = subgroup_metadata(
        &transaction,
        provider_id,
        file_name,
        group_path,
        options.offset,
        options.limit,
    )
    .await?;

    let entities = if subgroups.is_empty() {
        itertools::Either::Left(
            entities(
                &transaction,
                provider_id,
                file_name,
                options.offset,
                options.limit,
            )
            .await?,
        )
    } else {
        itertools::Either::Right(std::iter::empty())
    };

    let layer_collection = create_layer_collection_from_parts(
        LayerCollectionParts {
            provider_id,
            collection_id: collection.clone(),
            group_path,
            overview: overview_metadata,
            group: group_metadata,
            subgroups,
            entities,
        },
        id_fn.as_ref(),
    );

    Ok(Some(layer_collection))
}

pub(crate) async fn layer(
    transaction: Transaction<'_>,
    provider_id: DataProviderId,
    layer_id: &LayerId,
    file_name: &str,
    group_path: &[String],
    entity_id: usize,
) -> Result<Option<Layer>> {
    let Some(overview_metadata) = overview_metadata(&transaction, provider_id, file_name).await?
    else {
        return Ok(None);
    };

    let netcdf_entity = entity(&transaction, provider_id, file_name, entity_id)
        .await?
        .map_or(
            NetCdfEntity {
                // defensive default
                name: String::new(),
                id: entity_id,
            },
            |entity| entity,
        );

    let data_range: Option<DataRange> =
        data_range(&transaction, provider_id, file_name, group_path).await?;

    let (data_range, colorizer) =
        determine_data_range_and_colorizer(data_range, overview_metadata.colorizer)
            .boxed_context(error::CannotCreateColorizer)?;

    let time_steps: Vec<TimeInstance> = timestamps(&transaction, provider_id, file_name).await?;

    let layer = create_layer(
        ProviderLayerId {
            provider_id,
            layer_id: layer_id.clone(),
        },
        NetCdfCf4DDatasetId {
            file_name: overview_metadata.file_name,
            group_names: group_path.to_vec(),
            entity: entity_id,
        }
        .as_named_data(&provider_id)
        .context(error::CannotSerializeLayer)?,
        netcdf_entity,
        colorizer,
        &overview_metadata.creator,
        &time_steps,
        data_range,
    )?;

    Ok(Some(layer))
}

pub(crate) async fn loading_info(
    transaction: Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
    group_path: &[String],
    entity: usize,
) -> Result<Option<GdalMetaDataList>> {
    let Some(row) = transaction
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
            &[&provider_id, &file_name, &group_path, &(entity as i64)],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?
    else {
        return Ok(None);
    };

    let meta_data: GdalMetaDataList = row.get(0);

    Ok(Some(meta_data))
}

async fn overview_metadata(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
) -> Result<Option<NetCdfOverviewMetadata>> {
    let Some(row) = transaction
        .query_opt(
            r#"
            SELECT
                o.file_name,
                o.title,
                o.summary,
                o.spatial_reference :: "SpatialReference",
                o.colorizer :: "Colorizer",
                o.creator_name,
                o.creator_email,
                o.creator_institution
            FROM
                ebv_provider_overviews o
                LEFT OUTER JOIN ebv_provider_dataset_locks l ON (
                    o.provider_id = l.provider_id AND
                    o.file_name = l.file_name
                )
            WHERE
                o.provider_id = $1 AND
                o.file_name = $2 AND
                l.file_name IS NULL
        "#,
            &[&provider_id, &file_name],
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

async fn entity(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
    entity: usize,
) -> Result<Option<NetCdfEntity>> {
    let Some(row) = transaction
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
            &[&provider_id, &file_name, &(entity as i64)],
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

async fn entities(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
    offset: u32,
    limit: u32,
) -> Result<impl Iterator<Item = NetCdfEntity> + use<>> {
    let entities = transaction
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
                &provider_id,
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

async fn group_metadata(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
    group_path: &[String],
) -> Result<Option<NetCdfGroupMetadata>> {
    let Some(row) = transaction
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
            &[&provider_id, &file_name, &group_path],
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

async fn subgroup_metadata(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
    group_path: &[String],
    offset: u32,
    limit: u32,
) -> Result<Vec<NetCdfGroupMetadata>> {
    let subgroups = transaction
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
                &provider_id,
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

async fn data_range(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
    group_path: &[String],
) -> Result<Option<DataRange>> {
    let Some(row) = transaction
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
            &[&provider_id, &file_name, &group_path],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?
    else {
        return Ok(None);
    };

    let data_range = row.get::<_, Option<DataRange>>("data_range");

    Ok(data_range)
}

async fn timestamps(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
) -> Result<Vec<TimeInstance>> {
    let timestamps = transaction
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
            &[&provider_id, &file_name],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?
        .into_iter()
        .map(|row| row.get("time"))
        .collect();

    Ok(timestamps)
}

async fn insert_overview(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    metadata: &NetCdfOverview,
) -> Result<()> {
    transaction
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
                &provider_id,
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

async fn insert_groups(
    transaction: &Transaction<'_>,
    provider_id: &DataProviderId,
    file_name: &str,
    groups: &[NetCdfGroup],
) -> Result<()> {
    let group_statement = transaction
        .prepare_typed(
            "INSERT INTO ebv_provider_groups (
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
            );",
            &[
                tokio_postgres::types::Type::UUID,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT_ARRAY,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::FLOAT8_ARRAY,
                tokio_postgres::types::Type::TEXT,
                // we omit `data_type`
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    let mut group_stack = groups
        .iter()
        .map(|group| (vec![group.name.as_str()], group))
        .collect::<Vec<_>>();

    while let Some((group_path, group)) = group_stack.pop() {
        for subgroup in &group.groups {
            let mut subgroup_path = group_path.clone();
            subgroup_path.push(subgroup.name.as_str());
            group_stack.push((subgroup_path, subgroup));
        }

        transaction
            .execute(
                &group_statement,
                &[
                    &provider_id,
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
    }

    Ok(())
}

async fn insert_entities(
    transaction: &Transaction<'_>,
    provider_id: &DataProviderId,
    file_name: &str,
    entities: &[NetCdfEntity],
) -> Result<()> {
    let entity_statement = transaction
        .prepare_typed(
            "INSERT INTO
                ebv_provider_entities (
                    provider_id,
                    file_name,
                    id,
                    name
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4
                );",
            &[
                tokio_postgres::types::Type::UUID,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::INT8,
                tokio_postgres::types::Type::TEXT,
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    for entity in entities {
        transaction
            .execute(
                &entity_statement,
                &[provider_id, &file_name, &(entity.id as i64), &entity.name],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;
    }

    Ok(())
}

async fn insert_timestamps(
    transaction: &Transaction<'_>,
    provider_id: &DataProviderId,
    file_name: &str,
    time_steps: &[TimeInstance],
) -> Result<()> {
    let timestamp_statement = transaction
        .prepare_typed(
            r#"INSERT INTO
                ebv_provider_timestamps (
                    provider_id,
                    file_name,
                    "time"
                ) VALUES (
                    $1,
                    $2,
                    $3
                );"#,
            &[
                tokio_postgres::types::Type::UUID,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::INT8,
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    for time_step in time_steps {
        transaction
            .execute(
                &timestamp_statement,
                &[&provider_id, &file_name, &time_step],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;
    }

    Ok(())
}

async fn insert_loading_infos(
    transaction: &Transaction<'_>,
    provider_id: &DataProviderId,
    file_name: &str,
    loading_infos: &[LoadingInfoMetadata],
) -> Result<()> {
    let loading_info_stmt = transaction
        .prepare_typed(
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
            );"#,
            &[
                tokio_postgres::types::Type::UUID,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT_ARRAY,
                tokio_postgres::types::Type::INT8,
                // we omit `GdalMetaDataList`
            ],
        )
        .await
        .boxed_context(error::CannotStoreLoadingInfo)?;

    for loading_info in loading_infos {
        transaction
            .execute(
                &loading_info_stmt,
                &[
                    &provider_id,
                    &file_name,
                    &loading_info.group_path,
                    &(loading_info.entity_id as i64),
                    &loading_info.meta_data,
                ],
            )
            .await
            .boxed_context(error::CannotStoreLoadingInfo)?;
    }

    Ok(())
}

pub(crate) async fn remove_overviews(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
) -> Result<bool> {
    let removed_rows = transaction
        .execute(
            "
            DELETE FROM
                ebv_provider_overviews
            WHERE
                provider_id = $1 AND
                file_name = $2
            ",
            &[&provider_id, &file_name],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    Ok(removed_rows > 0)
}

pub struct NetCdfDatabaseListingConfig<'a> {
    pub provider_id: DataProviderId,
    pub collection: &'a LayerCollectionId,
    pub file_name: &'a str,
    pub group_path: &'a [String],
    pub options: &'a LayerCollectionListOptions,
}

/// A flag that indicates on-going process of an overview.
///
/// Cleans up the flag if dropped.
#[derive(Debug)]
pub struct InProgressFlag<D: NetCdfCfProviderDb + 'static + std::fmt::Debug> {
    db: Arc<D>,
    provider_id: DataProviderId,
    file_name: String,
    clean_on_drop: bool,
}

impl<D: NetCdfCfProviderDb + std::fmt::Debug> InProgressFlag<D> {
    pub async fn create(
        db: Arc<D>,
        provider_id: DataProviderId,
        file_name: String,
    ) -> Result<Self> {
        let success = db.lock_overview(provider_id, &file_name).await?;

        if !success {
            return Err(NetCdfCf4DProviderError::CannotCreateInProgressFlag);
        }

        Ok(Self {
            db,
            provider_id,
            file_name,
            clean_on_drop: true,
        })
    }

    pub async fn remove(mut self) -> Result<()> {
        self.clean_on_drop = false;

        self.db
            .unlock_overview(self.provider_id, &self.file_name)
            .await?;

        Ok(())
    }
}

impl<D: NetCdfCfProviderDb + 'static + std::fmt::Debug> Drop for InProgressFlag<D> {
    fn drop(&mut self) {
        if !self.clean_on_drop {
            return;
        }

        let db = self.db.clone();
        let provider_id = self.provider_id;
        let file_name = std::mem::take(&mut self.file_name);

        self.clean_on_drop = false;

        // We don't wait for the future to complete, as we don't want to block the thread.
        // Moreover, we cannot react to errors here, as we cannot propagate them.
        tokio::spawn(async move {
            let result = db.unlock_overview(provider_id, &file_name).await;

            if let Err(e) = result {
                log::error!("Cannot remove in-progress flag: {}", e);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        contexts::SessionContext,
        contexts::{PostgresContext, PostgresSessionContext},
        datasets::external::netcdfcf::{EBV_PROVIDER_ID, NETCDF_CF_PROVIDER_ID},
        ge_context,
    };
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_locks(_app_ctx: PostgresContext<NoTls>, ctx: PostgresSessionContext<NoTls>) {
        let db = Arc::new(ctx.db());

        // lock the overview
        let flag = InProgressFlag::create(db.clone(), NETCDF_CF_PROVIDER_ID, "file_name".into())
            .await
            .unwrap();

        // try to lock the overview again
        assert!(
            InProgressFlag::create(db.clone(), NETCDF_CF_PROVIDER_ID, "file_name".into())
                .await
                .is_err()
        );

        // lock the overview from another provider
        InProgressFlag::create(db.clone(), EBV_PROVIDER_ID, "file_name".into())
            .await
            .unwrap();

        // unlock the overview
        flag.remove().await.unwrap();

        // lock the overview again
        InProgressFlag::create(db.clone(), NETCDF_CF_PROVIDER_ID, "file_name".into())
            .await
            .unwrap();
    }
}
