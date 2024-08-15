use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

/// This migration adds the multiband raster colorizer
pub struct Migration0012MultibandColorizer;

#[async_trait]
impl Migration for Migration0012MultibandColorizer {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some("0011_remove_xgb".into())
    }

    fn version(&self) -> DatabaseVersion {
        "0012_multiband_colorizer".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(
            r#"
            ALTER TYPE "RasterColorizerType"
            ADD VALUE 'MultiBand';

            ALTER TYPE "RasterColorizer"
            ADD ATTRIBUTE red_band bigint,
            ADD ATTRIBUTE red_min double precision,
            ADD ATTRIBUTE red_max double precision,
            ADD ATTRIBUTE red_scale double precision,
            ADD ATTRIBUTE green_band bigint,
            ADD ATTRIBUTE green_min double precision,
            ADD ATTRIBUTE green_max double precision,
            ADD ATTRIBUTE green_scale double precision,
            ADD ATTRIBUTE blue_band bigint,
            ADD ATTRIBUTE blue_min double precision,
            ADD ATTRIBUTE blue_max double precision,
            ADD ATTRIBUTE blue_scale double precision,
            ADD ATTRIBUTE no_data_color "RgbaColor";
        "#,
        )
        .await?;

        Ok(())
    }
}
