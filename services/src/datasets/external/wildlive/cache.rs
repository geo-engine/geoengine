use super::{
    Result, WildliveLayerId,
    datasets::{ProjectFeature, StationFeature},
    error,
};
use bb8_postgres::{PostgresConnectionManager, bb8::PooledConnection};
use chrono::{NaiveDate, Utc};
use geoengine_datatypes::{dataset::DataProviderId, error::BoxedResultExt};
use tokio_postgres::{
    Socket, Transaction,
    tls::{MakeTlsConnect, TlsConnect},
};
use tonic::async_trait;
use wkt::to_wkt::ToWkt;

pub trait DatasetInDb {
    /// Table name of the dataset.
    fn table_name(&self) -> &str;
}

impl DatasetInDb for WildliveLayerId {
    fn table_name(&self) -> &str {
        match self {
            WildliveLayerId::Projects => "wildlive_projects",
            WildliveLayerId::Stations { project_id: _ } => "wildlive_stations",
            WildliveLayerId::Captures { project_id: _ } => "wildlive_captures",
        }
    }
}

#[async_trait]
/// Cache storage for the [`WildliveDataConnector`] provider
/// Caches datasets for the current day.
pub trait WildliveDbCache: Send + Sync {
    async fn has_projects(&self, provider_id: DataProviderId) -> Result<bool>;

    async fn insert_projects(
        &self,
        provider_id: DataProviderId,
        projects: &[ProjectFeature],
    ) -> Result<()>;

    async fn has_stations(&self, provider_id: DataProviderId, project_id: &str) -> Result<bool>;

    async fn insert_stations(
        &self,
        provider_id: DataProviderId,
        project_id: &str,
        stations: &[StationFeature],
    ) -> Result<()>;
}

#[async_trait]
impl<Tls> WildliveDbCache for crate::contexts::PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn has_projects(&self, provider_id: DataProviderId) -> Result<bool> {
        let current_date = Utc::now().date_naive();

        has_projects(get_connection!(self.conn_pool), provider_id, &current_date).await
    }

    async fn insert_projects(
        &self,
        provider_id: DataProviderId,
        projects: &[ProjectFeature],
    ) -> Result<()> {
        let current_date = Utc::now().date_naive();

        let mut connection = get_connection!(self.conn_pool);
        let transaction = deferred_write_transaction!(connection);

        delete_old_cached_projects(&transaction, provider_id, &current_date).await?;

        insert_projects(&transaction, provider_id, &current_date, projects).await?;

        transaction
            .commit()
            .await
            .boxed_context(error::UnexpectedExecution)
    }

    async fn has_stations(&self, provider_id: DataProviderId, project_id: &str) -> Result<bool> {
        let current_date = Utc::now().date_naive();

        has_stations(
            get_connection!(self.conn_pool),
            provider_id,
            &current_date,
            project_id,
        )
        .await
    }

    async fn insert_stations(
        &self,
        provider_id: DataProviderId,
        project_id: &str,
        stations: &[StationFeature],
    ) -> Result<()> {
        let current_date = Utc::now().date_naive();

        let mut connection = get_connection!(self.conn_pool);
        let transaction = deferred_write_transaction!(connection);

        delete_old_cached_stations(&transaction, provider_id, &current_date).await?;

        insert_stations(
            &transaction,
            provider_id,
            &current_date,
            project_id,
            stations,
        )
        .await?;

        transaction
            .commit()
            .await
            .boxed_context(error::UnexpectedExecution)
    }
}

/// Gets a connection from the pool
macro_rules! get_connection {
    ($connection:expr) => {{
        use geoengine_datatypes::error::BoxedResultExt;
        $connection
            .get()
            .await
            .boxed_context(crate::datasets::external::wildlive::error::UnexpectedExecution)?
    }};
}
pub(super) use get_connection;

/// Starts a deferred write transaction
macro_rules! deferred_write_transaction {
    ($connection:expr) => {{
        use geoengine_datatypes::error::BoxedResultExt;

        let transaction = $connection
            .transaction()
            .await
            .boxed_context(crate::datasets::external::wildlive::error::UnexpectedExecution)?;

        // check constraints at the end to speed up insertions
        transaction
            .batch_execute("SET CONSTRAINTS ALL DEFERRED")
            .await
            .boxed_context(crate::datasets::external::wildlive::error::UnexpectedExecution)?;

        transaction
    }};
}
pub(super) use deferred_write_transaction;

pub(crate) async fn delete_old_cached_projects(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    current_date: &NaiveDate,
) -> Result<()> {
    transaction
        .execute(
            // `cache_date <=` to delete potential conflicting entries
            "
            DELETE FROM wildlive_projects
            WHERE
                provider_id = $1 AND
                cache_date <= $2
            ;
        ",
            &[&provider_id, &current_date],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    Ok(())
}

pub(crate) async fn delete_old_cached_stations(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    current_date: &NaiveDate,
) -> Result<()> {
    transaction
        .execute(
            "
            DELETE FROM wildlive_stations
            WHERE
                provider_id = $1 AND
                cache_date < $2
            ;
        ",
            &[&provider_id, &current_date],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    Ok(())
}

pub(crate) async fn has_projects<Tls>(
    connection: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    provider_id: DataProviderId,
    current_date: &NaiveDate,
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
                    FROM wildlive_stations
                    WHERE
                        provider_id = $1 AND
                        cache_date = $2
                )
                ",
            &[&provider_id, &current_date],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?
        .get::<_, bool>(0);

    Ok(exists)
}

pub(crate) async fn has_stations<Tls>(
    connection: PooledConnection<'_, PostgresConnectionManager<Tls>>,
    provider_id: DataProviderId,
    current_date: &NaiveDate,
    project_id: &str,
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
                    FROM wildlive_stations
                    WHERE
                        provider_id = $1 AND
                        cache_date = $2 AND
                        project_id = $3
                )
                ",
            &[&provider_id, &current_date, &project_id],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?
        .get::<_, bool>(0);

    Ok(exists)
}

pub(crate) async fn insert_projects(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    current_date: &NaiveDate,
    projects: &[ProjectFeature],
) -> Result<()> {
    let insert_statement = transaction
        .prepare_typed(
            "INSERT INTO
                wildlive_projects (
                    provider_id,
                    cache_date,
                    project_id,
                    name,
                    description,
                    geom
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    public.ST_GeomFromText($6)
                );",
            &[
                tokio_postgres::types::Type::UUID,
                tokio_postgres::types::Type::DATE,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT, // geometry
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    for feature in projects {
        transaction
            .execute(
                &insert_statement,
                &[
                    &provider_id,
                    &current_date,
                    &feature.id,
                    &feature.name,
                    &feature.description,
                    &feature.geom.to_wkt().to_string(),
                ],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;
    }

    Ok(())
}

pub(crate) async fn insert_stations(
    transaction: &Transaction<'_>,
    provider_id: DataProviderId,
    current_date: &NaiveDate,
    project_id: &str,
    stations: &[StationFeature],
) -> Result<()> {
    let insert_statement = transaction
        .prepare_typed(
            "INSERT INTO
                wildlive_stations (
                    provider_id,
                    cache_date,
                    project_id,
                    station_id,
                    name,
                    description,
                    location,
                    geom
                ) VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    public.ST_GeomFromText($8)
                );",
            &[
                tokio_postgres::types::Type::UUID,
                tokio_postgres::types::Type::DATE,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::TEXT, // geometry
            ],
        )
        .await
        .boxed_context(error::UnexpectedExecution)?;

    for feature in stations {
        transaction
            .execute(
                &insert_statement,
                &[
                    &provider_id,
                    &current_date,
                    &project_id,
                    &feature.id,
                    &feature.name,
                    &feature.description,
                    &feature.location,
                    &feature.geom.to_wkt().to_string(),
                ],
            )
            .await
            .boxed_context(error::UnexpectedExecution)?;
    }

    Ok(())
}
