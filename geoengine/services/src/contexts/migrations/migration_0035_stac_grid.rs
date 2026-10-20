use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::migrations::Migration0034PlotsSingleRasterSource, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;
pub struct Migration0035StacGrid;

#[async_trait]
impl Migration for Migration0035StacGrid {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0034PlotsSingleRasterSource.version())
    }
    fn version(&self) -> DatabaseVersion {
        "0035_stac_grid".into()
    }
    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0035_stac_grid.sql"))
            .await?;
        Ok(())
    }
}
