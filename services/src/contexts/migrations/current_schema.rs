use super::{
    all_migrations,
    database_migration::{DatabaseVersion, Migration},
};
use crate::{
    error::Result,
    layers::{
        add_from_directory::UNSORTED_COLLECTION_ID, storage::INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
    },
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// Migration to create the current schema instead of starting by version `0000`.
pub struct CurrentSchemaMigration;

impl CurrentSchemaMigration {
    pub async fn create_current_schema(
        &self,
        tx: &Transaction<'_>,
        config: &crate::config::Postgres,
    ) -> Result<()> {
        let schema_name = &config.schema;

        if schema_name != "pg_temp" {
            tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema_name};",))
                .await?;
        }

        tx.batch_execute(include_str!("current_schema.sql")).await?;

        Ok(())
    }

    pub async fn populate_current_schema(
        &self,
        tx: &Transaction<'_>,
        config: &crate::config::Postgres,
    ) -> Result<()> {
        tx.execute(
            "
            INSERT INTO geoengine (
                clear_database_on_start,
                database_version
            ) VALUES (
                $1,
                $2
            );
            ",
            &[&config.clear_database_on_start, &self.version()],
        )
        .await?;

        tx.execute(
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
            );
            "#,
            &[&INTERNAL_LAYER_DB_ROOT_COLLECTION_ID],
        )
        .await?;

        tx.execute(
            r#"
            INSERT INTO layer_collections (
                id,
                name,
                description,
                properties
            ) VALUES (
                $1,
                'Unsorted',
                'Unsorted Layers',
                ARRAY[]::"PropertyType"[]
            );
            "#,
            &[&UNSORTED_COLLECTION_ID],
        )
        .await?;

        tx.execute(
            "
            INSERT INTO collection_children (
                parent,
                child
            ) VALUES (
                $1,
                $2
            );
            ",
            &[
                &INTERNAL_LAYER_DB_ROOT_COLLECTION_ID,
                &UNSORTED_COLLECTION_ID,
            ],
        )
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Migration for CurrentSchemaMigration {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        None
    }

    fn version(&self) -> DatabaseVersion {
        all_migrations()
            .last()
            .expect("to have at least one migration")
            .version()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        let config = crate::config::get_config_element::<crate::config::Postgres>()?;

        self.create_current_schema(tx, &config).await?;

        self.populate_current_schema(tx, &config).await
    }
}
