use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::{
    error::Result,
    layers::{
        add_from_directory::UNSORTED_COLLECTION_ID, storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
    },
};

use super::database_migration::{DatabaseVersion, Migration};

pub struct Migration0000Initial;

#[async_trait]
impl Migration for Migration0000Initial {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        None
    }

    fn version(&self) -> DatabaseVersion {
        "0000_initial".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        let config = crate::util::config::get_config_element::<crate::util::config::Postgres>()?;

        let schema_name = &config.schema;

        if schema_name != "pg_temp" {
            tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema_name};",))
                .await?;
        }

        tx.batch_execute(include_str!("migration_0000_initial.sql"))
            .await?;

        let stmt = tx
            .prepare(
                "
            INSERT INTO geoengine (clear_database_on_start, database_version) VALUES ($1, '0000_initial');",
            )
            .await?;

        tx.execute(&stmt, &[&config.clear_database_on_start])
            .await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO layer_collections (
                id,
                name,
                description,
                properties
            ) VALUES (
                $1,
                'Layers',
                'All available Geo Engine layers',
                ARRAY[]::"PropertyType"[]
            );"#,
            )
            .await?;

        tx.execute(&stmt, &[&INTERNAL_LAYER_DB_ROOT_COLLECTION_ID])
            .await?;

        let stmt = tx
            .prepare(
                r#"INSERT INTO layer_collections (
                id,
                name,
                description,
                properties
            ) VALUES (
                $1,
                'Unsorted',
                'Unsorted Layers',
                ARRAY[]::"PropertyType"[]
            );"#,
            )
            .await?;

        tx.execute(&stmt, &[&UNSORTED_COLLECTION_ID]).await?;

        let stmt = tx
            .prepare(
                r#"
            INSERT INTO collection_children (parent, child) 
            VALUES ($1, $2);"#,
            )
            .await?;

        tx.execute(
            &stmt,
            &[
                &INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
                &UNSORTED_COLLECTION_ID,
            ],
        )
        .await?;

        Ok(())
    }
}
