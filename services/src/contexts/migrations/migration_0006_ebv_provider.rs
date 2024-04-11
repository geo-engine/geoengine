use super::database_migration::{DatabaseVersion, Migration};
use crate::{contexts::Migration0005GbifColumnSelection, error::Result};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration adds the gbif provider column selection
pub struct Migration0006EbvProvider;

#[async_trait]
impl Migration for Migration0006EbvProvider {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0005GbifColumnSelection.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0006_ebv_provider".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0006_ebv_provider.sql"))
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::migrations::all_migrations;
    use crate::{contexts::migrate_database, util::config::get_config_element};
    use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
    use tokio_postgres::NoTls;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_adds_a_database_config() {
        let postgres_config = get_config_element::<crate::util::config::Postgres>().unwrap();
        let pg_mgr = PostgresConnectionManager::new(postgres_config.try_into().unwrap(), NoTls);

        let pool = Pool::builder().max_size(1).build(pg_mgr).await.unwrap();

        let mut conn = pool.get().await.unwrap();

        // initial schema
        migrate_database(&mut conn, &all_migrations()[..6])
            .await
            .unwrap();

        conn.batch_execute(
            r#"
            INSERT INTO layer_providers (
                id,
                type_name,
                name,
                definition,
                priority
            ) VALUES (
                '1c01dbb9-e3ab-f9a2-06f5-228ba4b6bf7a',
                'EBV',
                'EBV Description',
                (
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    (
                        'EBV',
                        'EBV Description',
                        'test_path',
                        'test_base_url',
                        0,
                        'test_overviews',
                        0
                    )::"EbvPortalDataProviderDefinition",
                    NULL,
                    NULL,
                    NULL,
                    NULL
                )::"DataProviderDefinition",
                0
            );
            "#,
        )
        .await
        .unwrap();

        // perform this migration
        migrate_database(&mut conn, &[Box::new(Migration0006EbvProvider)])
            .await
            .unwrap();

        // verify that we can access the field
        assert!(!conn
            .query(
                "
                    SELECT (definition).ebv_portal_data_provider_definition.data
                    FROM layer_providers
                    WHERE NOT ((definition).ebv_portal_data_provider_definition IS NULL)
                ",
                &[],
            )
            .await
            .unwrap()
            .is_empty());
    }
}
