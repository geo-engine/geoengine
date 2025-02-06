use super::database_migration::{DatabaseVersion, Migration};
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the multiband raster colorizer
pub struct Migration0015LogQuota;

#[async_trait]
impl Migration for Migration0015LogQuota {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        // Upon migration `0015_log_quota`, we did a major refactoring and removed the deprecated pro migrations.
        // Hence, we added a snapshot of the database schema to this migration instead of just the migration itself.
        // This is the state of the database schema at commit `071ba4e636a709f05ecb18b6f01bd19f313b0c94`.
        // Furthermore, we deleted all prior migrations, so we can't determine the previous version here.
        //
        // If you have a database version prior to `0015_log_quota`, you will need to migrate to `0015_log_quota` first.
        // Use commit `071ba4e636a709f05ecb18b6f01bd19f313b0c94` as a reference.
        // Then, you can migrate to the latest version.
        //
        None
    }

    fn version(&self) -> DatabaseVersion {
        "0015_log_quota".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        let config = crate::config::get_config_element::<crate::config::Postgres>()?;

        let schema_name = &config.schema;

        if schema_name != "pg_temp" {
            tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema_name};",))
                .await?;
        }

        tx.batch_execute(include_str!("migration_0015_snapshot.sql"))
            .await?;

        tx
            .execute(
                "INSERT INTO geoengine (clear_database_on_start, database_version) VALUES ($1, '0015_log_quota');",
            &[&config.clear_database_on_start])
            .await?;

        Ok(())
    }
}
