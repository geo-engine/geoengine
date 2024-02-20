use crate::contexts::{DatabaseVersion, Migration};
use crate::error::Result;
use async_trait::async_trait;
use geoengine_datatypes::operations::image::Colorizer;
use geoengine_datatypes::raster::RasterDataType;
use geoengine_datatypes::spatial_reference::SpatialReference;
use tokio_postgres::Transaction;

/// All migrations that are available. The migrations are applied in the order they are defined here, starting from the current version of the database.
///
/// NEW MIGRATIONS HAVE TO BE REGISTERED HERE!
///
pub fn all_migrations() -> Vec<Box<dyn Migration>> {
    vec![Box::new(Migration0000Initial)]
}

struct Migration0000Initial;

#[async_trait]
impl Migration for Migration0000Initial {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        None
    }

    fn version(&self) -> DatabaseVersion {
        "0001_initial".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        // TODO: find more robust solution to get current schema
        let schema_name: String = tx
            .query_one(
                "SELECT SPLIT_PART(setting, ',', 1) FROM pg_settings WHERE name='search_path';",
                &[],
            )
            .await?
            .get(0);

        if schema_name != "pg_temp" {
            tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema_name};",))
                .await?;
        }

        tx.batch_execute(include_str!("schema/migration_0000_initial.sql"))
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NetCdfGroupMetadata {
    pub name: String,
    pub title: String,
    pub description: String,
    // TODO: would actually be nice if it were inside dataset/entity
    pub data_type: Option<RasterDataType>,
    pub data_range: Option<(f64, f64)>,
    // TODO: would actually be nice if it were inside dataset/entity
    pub unit: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NetCdfOverviewMetadata {
    pub file_name: String,
    pub title: String,
    pub summary: String,
    pub spatial_reference: SpatialReference,
    pub colorizer: Colorizer,
    pub creator_name: Option<String>,
    pub creator_email: Option<String>,
    pub creator_institution: Option<String>,
}
