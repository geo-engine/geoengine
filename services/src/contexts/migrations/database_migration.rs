use crate::error::{Result, UnexpectedDatabaseVersionDuringMigration};
use async_trait::async_trait;
use bb8_postgres::{bb8::PooledConnection, PostgresConnectionManager};
use log::info;
use snafu::ensure;
use tokio_postgres::{
    error::SqlState,
    tls::{MakeTlsConnect, TlsConnect},
    Socket, Transaction,
};

pub type DatabaseVersion = String;

/// The logic for migrating the database from one version to another.
#[async_trait]
pub trait Migration: Send + Sync {
    /// The previous version of the database. `None` if this is the first migration.
    /// The database must be in this version in order for the migration to be applied.
    fn prev_version(&self) -> Option<DatabaseVersion>;

    /// The new version of the database after applying this migration.
    fn version(&self) -> DatabaseVersion;

    /// Apply the migration to the database.
    ///
    /// Note: The migration shall not update the version and not commit the transaction. This is done by the migration framework.
    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()>;
}

/// The current version of the database. `None` if the database is empty.
async fn determine_current_database_version<Tls>(
    conn: &PooledConnection<'_, PostgresConnectionManager<Tls>>,
) -> Result<Option<DatabaseVersion>>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let stmt = match conn
        .prepare("SELECT database_version from geoengine;")
        .await
    {
        Ok(stmt) => stmt,
        Err(e) => {
            if let Some(code) = e.code() {
                if *code == SqlState::UNDEFINED_TABLE {
                    return Ok(None);
                }
            }
            return Err(crate::error::Error::TokioPostgres { source: e });
        }
    };

    let row = conn.query_one(&stmt, &[]).await?;

    Ok(Some(row.get(0)))
}

#[derive(Debug, PartialEq)]
pub enum MigrationResult {
    CreatedDatabase,
    MigratedDatabase,
    AlreadyUpToDate,
}

/// Initialize the database.
/// If the database is empty, the [`migration_if_uninitialized`] is applied.
/// Otherwise, the [`migrations`] are applied.
pub async fn initialize_database<Tls>(
    connection: &mut PooledConnection<'_, PostgresConnectionManager<Tls>>,
    migration_if_uninitialized: Box<dyn Migration>,
    migrations: &[Box<dyn Migration>],
) -> Result<MigrationResult>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    if determine_current_database_version(connection)
        .await?
        .is_none()
    {
        create_database_schema(connection, migration_if_uninitialized).await?;

        Ok(MigrationResult::CreatedDatabase)
    } else {
        migrate_database(connection, migrations).await
    }
}

/// Create the database schema.
async fn create_database_schema<Tls>(
    connection: &mut PooledConnection<'_, PostgresConnectionManager<Tls>>,
    create_schema: Box<dyn Migration>,
) -> Result<()>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let tx = connection.build_transaction().start().await?;

    create_schema.migrate(&tx).await?;

    tx.commit().await?;

    info!("Created schema. Version is: {}", create_schema.version());

    Ok(())
}

/// Migrate the database to the latest version. If the database is empty, the initial migration is applied.
///
/// # Panics
///
/// Panics if there is an error in the migration logic.
///
pub async fn migrate_database<Tls>(
    connection: &mut PooledConnection<'_, PostgresConnectionManager<Tls>>,
    migrations: &[Box<dyn Migration>],
) -> Result<MigrationResult>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let pre_migration_version = determine_current_database_version(connection).await?;
    info!("Current database version: {:?}", pre_migration_version);

    // start with the first migration after the current version
    let applicable_migrations = migrations
        .iter()
        .skip_while(|m| m.prev_version() != pre_migration_version);

    let mut current_version = pre_migration_version.clone();

    for migration in applicable_migrations {
        ensure!(
            migration.prev_version() == current_version,
            UnexpectedDatabaseVersionDuringMigration {
                expected: migration.prev_version().unwrap_or_default(),
                found: current_version.unwrap_or_default()
            }
        );

        info!("Applying migration: {}", migration.version());

        let tx = connection.build_transaction().start().await?;

        migration.migrate(&tx).await?;

        tx.execute(
            "UPDATE geoengine SET database_version = $1",
            &[&migration.version()],
        )
        .await?;

        tx.commit().await?;

        current_version = Some(migration.version());
    }

    let current_version = determine_current_database_version(connection)
        .await?
        .expect("after migration, there should be a current version.");

    let latest_version = migrations
        .last()
        .expect("there should be at least one migration, namely the inital migration.")
        .version();

    ensure!(
        current_version == latest_version,
        UnexpectedDatabaseVersionDuringMigration {
            expected: latest_version,
            found: current_version
        }
    );

    Ok(match pre_migration_version {
        None => MigrationResult::CreatedDatabase,
        Some(v) if v == latest_version => MigrationResult::AlreadyUpToDate,
        _ => MigrationResult::MigratedDatabase,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::get_config_element,
        contexts::migrations::{all_migrations, CurrentSchemaMigration, Migration0015LogQuota},
        util::postgres::DatabaseConnectionConfig,
    };
    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn it_migrates() -> Result<()> {
        struct TestMigration;

        #[async_trait]
        impl Migration for TestMigration {
            fn prev_version(&self) -> Option<DatabaseVersion> {
                Some("0000_initial".to_string())
            }

            fn version(&self) -> DatabaseVersion {
                "0001_mock".to_string()
            }

            async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
                tx.batch_execute(
                    "
                CREATE TABLE mock (id INT);
                INSERT INTO mock (id) VALUES (0), (1);
                ",
                )
                .await?;

                Ok(())
            }
        }

        struct FollowUpMigration;

        #[async_trait]
        impl Migration for FollowUpMigration {
            fn prev_version(&self) -> Option<DatabaseVersion> {
                Some("0001_mock".to_string())
            }

            fn version(&self) -> DatabaseVersion {
                "0002_follow_up".to_string()
            }

            async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
                tx.batch_execute("ALTER TABLE mock ADD COLUMN foo text DEFAULT 'placeholder';")
                    .await?;

                Ok(())
            }
        }

        let migrations: Vec<Box<dyn Migration>> = vec![
            Box::new(Migration0015LogQuota),
            Box::new(TestMigration),
            Box::new(FollowUpMigration),
        ];

        let postgres_config = get_config_element::<crate::config::Postgres>()?;
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        migrate_database(&mut conn, &migrations).await?;

        let stmt = conn.prepare("SELECT * FROM mock;").await?;

        let rows = conn.query(&stmt, &[]).await?;

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i32>(0), 0);
        assert_eq!(rows[0].get::<_, String>(1), "placeholder".to_string());
        assert_eq!(rows[1].get::<_, i32>(0), 1);
        assert_eq!(rows[1].get::<_, String>(1), "placeholder".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn it_performs_all_migrations() -> Result<()> {
        let postgres_config = get_config_element::<crate::config::Postgres>()?;
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        migrate_database(&mut conn, &all_migrations()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn it_uses_the_current_schema_if_the_database_is_empty() -> Result<()> {
        let postgres_config = get_config_element::<crate::config::Postgres>()?;
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        initialize_database(
            &mut conn,
            Box::new(CurrentSchemaMigration),
            &all_migrations(),
        )
        .await?;

        Ok(())
    }
}
