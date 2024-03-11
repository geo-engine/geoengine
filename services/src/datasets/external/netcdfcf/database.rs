use super::{
    determine_data_range_and_colorizer, error,
    loading::{
        create_layer, create_layer_collection_from_parts, LayerCollectionIdFn, LayerCollectionParts,
    },
    metadata::{Creator, DataRange, NetCdfGroupMetadata, NetCdfOverviewMetadata},
    overviews::LoadingInfoMetadata,
    NetCdfCf4DDatasetId, NetCdfEntity, NetCdfGroup, NetCdfOverview, Result,
};
use crate::{
    contexts::PostgresDb,
    layers::{
        layer::{Layer, LayerCollection, LayerCollectionListOptions, ProviderLayerId},
        listing::LayerCollectionId,
    },
};
use async_trait::async_trait;
use geoengine_datatypes::{
    dataset::{DataProviderId, LayerId},
    error::BoxedResultExt,
    primitives::TimeInstance,
};
use geoengine_operators::source::GdalMetaDataList;
use snafu::ResultExt;
use std::sync::Arc;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Socket, Transaction,
};

#[async_trait]
/// Storage for the [`NetCdfCfDataProvider`] provider
pub trait NetCdfCfProviderDb: Send + Sync {
    // TODO: in-progress flag

    // TODO: remove in-progress flag

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

/// Gets a connection from the pool
macro_rules! get_connection {
    ($connection:expr) => {
        $connection
            .get()
            .await
            .boxed_context(error::DatabaseConnection)?
    };
}

/// Starts a read-only transaction for a single read
macro_rules! readonly_transaction {
    ($connection:expr) => {
        $connection
            .build_transaction()
            .read_only(true)
            .deferrable(true) // get snapshot isolation
            .start()
            .await
            .boxed_context(error::DatabaseTransaction)?
    };
}

/// Starts a read-only snapshot-isolated transaction for a multiple read
macro_rules! readonly_transaction_for_multiple_reads {
    ($connection:expr) => {
        $connection
            .build_transaction()
            .read_only(true)
            .deferrable(true) // get snapshot isolation
            .start()
            .await
            .boxed_context(error::DatabaseTransaction)?
    };
}

/// Starts a deferred write transaction
macro_rules! deferred_write_transaction {
    ($connection:expr) => {{
        let transaction = $connection
            .transaction()
            .await
            .boxed_context(error::DatabaseTransaction)?;

        // check constraints at the end to speed up insertions
        transaction
            .batch_execute("SET CONSTRAINTS ALL DEFERRED")
            .await
            .boxed_context(error::UnexpectedExecution)?;

        transaction
    }};
}

#[async_trait]
impl<Tls> NetCdfCfProviderDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn overviews_exist(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction!(connection);

        let exists = transaction
            .query_one(
                "
                SELECT EXISTS (
                    SELECT 1
                    FROM ebv_provider_overviews
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

    async fn store_overview_metadata(
        &self,
        provider_id: DataProviderId,
        metadata: &NetCdfOverview,
        loading_infos: Vec<LoadingInfoMetadata>,
    ) -> Result<()> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = deferred_write_transaction!(connection);

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

    async fn remove_overviews(&self, provider_id: DataProviderId, file_name: &str) -> Result<bool> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = deferred_write_transaction!(connection);

        remove_overviews(&transaction, provider_id, file_name).await
    }

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
    ) -> Result<Option<LayerCollection>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction_for_multiple_reads!(connection);

        let Some(overview_metadata) =
            overview_metadata(&transaction, provider_id, file_name).await?
        else {
            return Ok(None);
        };

        let group_metadata =
            group_metadata(&transaction, provider_id, file_name, group_path).await?;

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

    async fn layer(
        &self,
        provider_id: DataProviderId,
        layer_id: &LayerId,
        file_name: &str,
        group_path: &[String],
        entity_id: usize,
    ) -> Result<Option<Layer>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction_for_multiple_reads!(connection);

        let Some(overview_metadata) =
            overview_metadata(&transaction, provider_id, file_name).await?
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

        let time_steps: Vec<TimeInstance> =
            timestamps(&transaction, provider_id, file_name).await?;

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

    async fn loading_info(
        &self,
        provider_id: DataProviderId,
        file_name: &str,
        group_path: &[String],
        entity: usize,
    ) -> Result<Option<GdalMetaDataList>> {
        let mut connection = get_connection!(self.conn_pool);
        let transaction = readonly_transaction_for_multiple_reads!(connection);

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

async fn overview_metadata(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    file_name: &str,
) -> Result<Option<NetCdfOverviewMetadata>> {
    let Some(row) = transaction
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
) -> Result<impl Iterator<Item = NetCdfEntity>> {
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

async fn remove_overviews(
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

// #[async_trait]
// trait NetCdfCfProviderDbParts {
//     type Tls;

//     async fn connection(
//         &self,
//     ) -> Result<PooledConnection<'_, bb8_postgres::PostgresConnectionManager<Self::Tls>>>
//     where
//         Self::Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
//         <Self::Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
//         <Self::Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
//         <<Self::Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send;
// }

pub struct NetCdfDatabaseListingConfig<'a> {
    pub provider_id: DataProviderId,
    pub collection: &'a LayerCollectionId,
    pub file_name: &'a str,
    pub group_path: &'a [String],
    pub options: &'a LayerCollectionListOptions,
}

// #[async_trait]
// impl<Tls> NetCdfCfProviderDbParts for PostgresDb<Tls>
// where
//     Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
//     <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
//     <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
//     <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
// {
//     type Tls = Tls;

//     async fn connection(
//         &self,
//     ) -> Result<PooledConnection<'_, bb8_postgres::PostgresConnectionManager<Tls>>>
//     where
//         Self::Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
//         <Self::Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
//         <Self::Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
//         <<Self::Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
//     {
//         self.conn_pool
//             .get()
//             .await
//             .boxed_context(error::DatabaseConnection)
//     }
// }

// TODO: move
// #[cfg(feature = "pro")]
// #[async_trait]
// impl<Tls> NetCdfCfProviderDb for crate::pro::contexts::ProPostgresDb<Tls>
// where
//     Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
//     <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
//     <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
//     <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
// {
// }

//// Creates a read-only transaction that should be used for single requests
// async fn readonly_transaction<'db, 't, Tls>(
//     db_connection: &'t mut bb8_postgres::bb8::PooledConnection<'db, PostgresConnectionManager<Tls>>,
// ) -> Result<tokio_postgres::Transaction<'t>>
// where
//     Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
//     <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
//     <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
//     <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
// {
//     db_connection
//         .build_transaction()
//         .read_only(true)
//         .deferrable(true) // get snapshot isolation
//         .start()
//         .await
//         .boxed_context(error::DatabaseTransaction)
// }

// /// Creates a read-only transaction that uses snapshot isolation and should be used for multiple requests
// async fn readonly_transaction_for_multiple_reads<'db, 't, Tls>(
//     db_connection: &'t mut bb8_postgres::bb8::PooledConnection<'db, PostgresConnectionManager<Tls>>,
// ) -> Result<tokio_postgres::Transaction<'t>>
// where
//     Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
//     <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
//     <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
//     <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
// {
//     db_connection
//         .build_transaction()
//         .read_only(true)
//         .deferrable(true) // get snapshot isolation
//         .start()
//         .await
//         .boxed_context(error::DatabaseTransaction)
// }

// /// Creates a write transaction that defers constraints
// async fn deferred_write_transaction<'db, 't, Tls>(
//     db_connection: &'t mut bb8_postgres::bb8::PooledConnection<'db, PostgresConnectionManager<Tls>>,
// ) -> Result<tokio_postgres::Transaction<'t>>
// where
//     Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
//     <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
//     <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
//     <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
// {
//     let transaction = db_connection
//         .transaction()
//         .await
//         .boxed_context(error::DatabaseTransaction)?;

//     // check constraints at the end to speed up insertions
//     transaction
//         .batch_execute("SET CONSTRAINTS ALL DEFERRED")
//         .await
//         .boxed_context(error::UnexpectedExecution)?;

//     Ok(transaction)
// }
