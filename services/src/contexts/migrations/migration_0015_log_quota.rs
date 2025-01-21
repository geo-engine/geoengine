use super::database_migration::{DatabaseVersion, Migration};
use crate::error::Result;
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the multiband raster colorizer
pub struct Migration0015LogQuota;

#[async_trait]
impl Migration for Migration0015LogQuota {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        None // must have migrated to 0015 with old version
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
                "INSERT INTO geoengine (clear_database_on_start, database_version) VALUES ($1, '0000_initial');",
            &[&config.clear_database_on_start])
            .await?;

        Ok(())
    }
}
