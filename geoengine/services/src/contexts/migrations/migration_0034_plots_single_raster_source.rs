use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0033_gdal_multiband_cache_ttl::Migration0033GdalMultibandCacheTtl,
    error::Result,
};
use async_trait::async_trait;
use tokio_postgres::Transaction;

/// This migration rewrites stored `Statistics` and `BoxPlot` workflows to a single raster source.
///
/// Both operators now compute their results for the bands of one (multi-band) raster instead of a list of rasters.
/// Workflows with exactly one raster source get this raster as their source, and their `columnNames`,
/// formerly aliases for the rasters, are cleared because they now select bands by name.
/// Workflows with several raster sources cannot be expressed anymore and are left unchanged,
/// so they fail to load afterwards.
///
/// The workflow ids stay the same, because other tables refer to them.
/// Thus, the id of a rewritten workflow is no longer the hash of its content.
pub struct Migration0034PlotsSingleRasterSource;

#[async_trait]
impl Migration for Migration0034PlotsSingleRasterSource {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0033GdalMultibandCacheTtl.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0034_plots_single_raster_source".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!(
            "migration_0034_plots_single_raster_source.sql"
        ))
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bb8_postgres::{PostgresConnectionManager, bb8::Pool};
    use serde_json::json;
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
    async fn it_rewrites_plots_with_a_single_raster_source() {
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
                &Migration0033GdalMultibandCacheTtl.version(),
            ),
        )
        .await
        .unwrap();

        assert_eq!(
            conn.execute(include_str!("migration_0034_test_data.sql"), &[])
                .await
                .unwrap(),
            5
        );

        let tx = conn.transaction().await.unwrap();

        Migration0034PlotsSingleRasterSource
            .migrate(&tx)
            .await
            .unwrap();

        let workflows: Vec<serde_json::Value> = tx
            .query("SELECT workflow::jsonb FROM workflows ORDER BY id", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();

        let gdal_source = json!({"type": "GdalSource", "params": {"data": "ndvi"}});

        assert_eq!(
            workflows,
            vec![
                // single raster: rewritten
                json!({"type": "Plot", "operator": {"type": "Statistics", "params": {"columnNames": [], "percentiles": []}, "sources": {"source": gdal_source}}}),
                json!({"type": "Plot", "operator": {"type": "BoxPlot", "params": {"columnNames": []}, "sources": {"source": gdal_source}}}),
                // several rasters: unchanged
                json!({"type": "Plot", "operator": {"type": "Statistics", "params": {"columnNames": [], "percentiles": []}, "sources": {"source": [gdal_source, gdal_source]}}}),
                // vector source: unchanged
                json!({"type": "Plot", "operator": {"type": "Statistics", "params": {"columnNames": ["x"], "percentiles": []}, "sources": {"source": {"type": "OgrSource", "params": {"data": "points"}}}}}),
                // other plot: unchanged
                json!({"type": "Plot", "operator": {"type": "Histogram", "params": {"attributeName": "band"}, "sources": {"source": gdal_source}}}),
            ]
        );
    }
}
