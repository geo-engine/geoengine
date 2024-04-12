use async_trait::async_trait;
use bb8_postgres::{bb8::PooledConnection, PostgresConnectionManager};
use log::info;
use snafu::ensure;
use tokio_postgres::{
    error::SqlState,
    tls::{MakeTlsConnect, TlsConnect},
    Socket, Transaction,
};

use crate::error::{Result, UnexpectedDatabaseVersionDuringMigration};

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

/// Migrate the database to the latest version. If the database is empty, the initial migration is applied.
///
/// # Panics
///
/// Panics if there is an error in the migration logic.
pub async fn migrate_database<Tls>(
    conn: &mut PooledConnection<'_, PostgresConnectionManager<Tls>>,
    migrations: &[Box<dyn Migration>],
    prefix: Option<&str>,
) -> Result<MigrationResult>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let prefix = prefix.unwrap_or_default();

    let pre_migration_version = determine_current_database_version(conn).await?;
    info!(
        target: prefix,
        "{prefix}Current database version: {:?}",
        pre_migration_version
    );

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

        info!(target: prefix, "Applying migration: {}", migration.version());

        let tx = conn.build_transaction().start().await?;

        migration.migrate(&tx).await?;

        let stmt = tx
            .prepare("UPDATE geoengine SET database_version = $1")
            .await?;

        tx.execute(&stmt, &[&migration.version()]).await?;

        tx.commit().await?;

        current_version = Some(migration.version());
    }

    let current_version = determine_current_database_version(conn)
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
    use std::str::FromStr;

    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use geoengine_datatypes::test_data;
    use tokio_postgres::NoTls;

    use crate::{
        contexts::{
            migrations::{all_migrations, migration_0000_initial::Migration0000Initial},
            PostgresDb,
        },
        projects::{ProjectDb, ProjectListOptions},
        util::config::get_config_element,
        workflows::{registry::WorkflowRegistry, workflow::WorkflowId},
    };

    use super::*;

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
            Box::new(Migration0000Initial),
            Box::new(TestMigration),
            Box::new(FollowUpMigration),
        ];

        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into()?, NoTls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        migrate_database(&mut conn, &migrations, None).await?;

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
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into()?, NoTls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        migrate_database(&mut conn, &all_migrations(), None).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_migrates_data() -> Result<()> {
        // This test creates the initial schema and fills it with test data.
        // Then, it migrates the database to the newest version.
        // Finally, it tries to load the test data again via the Db implementations.

        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into()?, NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        // initial schema
        migrate_database(&mut conn, &[Box::new(Migration0000Initial)], None).await?;

        // insert test data on initial schema
        let test_data_sql = std::fs::read_to_string(test_data!("migrations/test_data.sql"))?;
        conn.batch_execute(&test_data_sql).await?;

        // migrate to latest schema
        migrate_database(&mut conn, &all_migrations(), None).await?;

        // drop the connection because the pool is limited to one connection, s.t. we can reuse the temporary schema
        drop(conn);

        // create `PostgresDb` on migrated database and test methods
        let db = PostgresDb::new(pool.clone());

        let projects = db
            .list_projects(ProjectListOptions {
                order: crate::projects::OrderBy::NameAsc,
                offset: 0,
                limit: 10,
            })
            .await
            .unwrap();

        assert!(!projects.is_empty());

        db.load_workflow(&WorkflowId::from_str("38ddfc17-016e-4910-8adf-b1af36a8590c").unwrap())
            .await
            .unwrap();

        // TODO: test more methods and more Dbs

        Ok(())
    }
}
