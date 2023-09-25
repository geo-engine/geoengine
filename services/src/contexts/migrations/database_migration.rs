use async_trait::async_trait;
use bb8_postgres::{bb8::PooledConnection, PostgresConnectionManager};
use log::info;
use snafu::ensure;
use tokio_postgres::{
    error::SqlState,
    tls::{MakeTlsConnect, TlsConnect},
    Socket,
};

use crate::error::{Result, UnexpectedDatabaseVersionDuringMigration};

use super::migrations;

pub type DatabaseVersion = String;

/// The logic for migrating the database from one version to another.
#[async_trait]
pub trait Migration<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    /// The previous version of the database. `None` if this is the first migration.
    /// The database must be in this version in order for the migration to be applied.
    fn prev_version(&self) -> Option<DatabaseVersion>;

    /// The new version of the database after applying this migration.
    fn version(&self) -> DatabaseVersion;

    /// Apply the migration to the database. This has to be an atomic operation, i.e. all changes must be rolled back if the operation fails.
    /// If this operation succeeds, the database is in the new version.
    async fn migrate(
        &self,
        conn: &mut PooledConnection<'_, PostgresConnectionManager<Tls>>,
        config: &crate::util::config::Postgres,
    ) -> Result<()>;
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
pub async fn migrate_database<Tls>(
    conn: &mut PooledConnection<'_, PostgresConnectionManager<Tls>>,
) -> Result<MigrationResult>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let migrations = migrations::<Tls>();

    let pre_migration_version = determine_current_database_version(conn).await?;
    info!("Current database version: {:?}", pre_migration_version);

    // start with the first migration after the current version
    let applicable_migrations = migrations
        .iter()
        .skip_while(|m| m.prev_version() != pre_migration_version);

    let mut already_applied_migration = false;

    for migration in applicable_migrations {
        if already_applied_migration {
            // in migration chain: check that the previous migration was applied
            //  TODO: this check only makes sense if the database_version is managed by the migrations, but we could also do this here and get rid of the check
            let current_version = determine_current_database_version(conn).await?;

            ensure!(
                migration.prev_version() == current_version,
                UnexpectedDatabaseVersionDuringMigration {
                    expected: migration.prev_version().unwrap_or_default(),
                    found: current_version.unwrap_or_default()
                }
            );
        }

        info!("Applying migration: {}", migration.version());

        migration
            .migrate(
                conn,
                &crate::util::config::get_config_element::<crate::util::config::Postgres>()?,
            )
            .await?;

        already_applied_migration = true;
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
    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use tokio_postgres::NoTls;

    use crate::{
        contexts::migrations::migration_0000_initial::Migration0000Initial,
        util::config::get_config_element,
    };

    use super::*;

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into()?, NoTls);

        let pool = Pool::builder().build(pg_mgr).await?;

        let mut conn = pool.get().await?;

        let m = Migration0000Initial;

        let postgres_config = get_config_element::<crate::util::config::Postgres>()?;
        m.migrate(&mut conn, &postgres_config).await?;

        Ok(())
    }
}
