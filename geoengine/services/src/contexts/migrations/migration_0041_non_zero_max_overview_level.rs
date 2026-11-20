use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0040_stac_coarser_assets::Migration0040StacCoarserAssets,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration sets the `max_overview_level` 0 of stored spatial grid descriptors to 1. Both
/// meant that the files of a dataset have no overviews; the level is at least 1 from now on, so
/// that 0 is no longer a value. `NULL` (unknown levels) and all other levels stay as they are.
pub struct Migration0041NonZeroMaxOverviewLevel;

#[async_trait]
impl Migration for Migration0041NonZeroMaxOverviewLevel {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0040StacCoarserAssets.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0041_non_zero_max_overview_level".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!(
            "migration_0041_non_zero_max_overview_level.sql"
        ))
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bb8_postgres::{PostgresConnectionManager, bb8::Pool};
    use tokio_postgres::NoTls;

    use crate::{
        config::get_config_element,
        contexts::{
            migrate_database,
            migrations::{
                Migration0016MergeProviders,
                database_migration::tests::create_migration_0015_snapshot, migrations_by_range,
            },
        },
        util::postgres::DatabaseConnectionConfig,
    };

    use super::*;

    #[tokio::test]
    async fn it_sets_stored_zero_levels_to_one() {
        let postgres_config = get_config_element::<crate::config::Postgres>().unwrap();
        let db_config = DatabaseConnectionConfig::from(postgres_config);
        let pg_mgr = PostgresConnectionManager::new(db_config.pg_config(), NoTls);
        let pool = Pool::builder().max_size(1).build(pg_mgr).await.unwrap();
        let mut conn = pool.get().await.unwrap();

        create_migration_0015_snapshot(&mut conn).await.unwrap();
        migrate_database(
            &mut conn,
            &migrations_by_range(
                &Migration0016MergeProviders.version(),
                &Migration0040StacCoarserAssets.version(),
            ),
        )
        .await
        .unwrap();

        conn.batch_execute(include_str!("migration_0041_test_data.sql"))
            .await
            .unwrap();

        let tx = conn.transaction().await.unwrap();
        Migration0041NonZeroMaxOverviewLevel
            .migrate(&tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // every place of the dataset that had 0 has 1
        let levels = |name: &'static str| {
            let conn = &conn;
            async move {
                let row = conn
                    .query_one(
                        r"SELECT
                            (result_descriptor).raster.spatial_grid.max_overview_level,
                            (meta_data).gdal_meta_data_regular.result_descriptor.spatial_grid.max_overview_level,
                            (meta_data).gdal_static.result_descriptor.spatial_grid.max_overview_level,
                            (meta_data).gdal_metadata_net_cdf_cf.result_descriptor.spatial_grid.max_overview_level,
                            (meta_data).gdal_meta_data_list.result_descriptor.spatial_grid.max_overview_level,
                            (meta_data).gdal_multi_band.result_descriptor.spatial_grid.max_overview_level
                        FROM datasets WHERE display_name = $1",
                        &[&name],
                    )
                    .await
                    .unwrap();
                (0..6)
                    .map(|column| row.get::<usize, Option<i32>>(column))
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(levels("zero").await, vec![Some(1); 6]);
        // other levels and unknown levels stay
        assert_eq!(
            levels("kept").await,
            vec![Some(8), None, Some(1), None, None, None]
        );

        // the datasets of the STAC provider, in their order
        let stac: Vec<(String, Option<i32>)> = conn
            .query(
                r"SELECT d.name, (d.spatial_grid).max_overview_level
                FROM layer_providers,
                    unnest((definition).stac_data_provider_definition.datasets)
                    WITH ORDINALITY AS d
                ORDER BY d.ordinality",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(
            stac,
            vec![
                ("a".to_string(), Some(1)),
                ("b".to_string(), Some(4)),
                ("c".to_string(), None)
            ]
        );
    }
}
