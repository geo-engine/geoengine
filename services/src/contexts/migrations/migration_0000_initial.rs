use async_trait::async_trait;
use tokio_postgres::Transaction;
use uuid::Uuid;

use crate::error::Result;

use super::database_migration::{DatabaseVersion, Migration};

const INTERNAL_LAYER_DB_ROOT_COLLECTION_ID: Uuid =
    Uuid::from_u128(0x0510_2bb3_a855_4a37_8a8a_3002_6a91_fef1);
const UNSORTED_COLLECTION_ID: Uuid = Uuid::from_u128(0xffb2_dd9e_f5ad_427c_b7f1_c9a0_c7a0_ae3f);

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
        let config = crate::config::get_config_element::<crate::config::Postgres>()?;

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
                "
            INSERT INTO collection_children (parent, child) 
            VALUES ($1, $2);",
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
