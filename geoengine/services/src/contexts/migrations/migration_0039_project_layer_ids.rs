use super::database_migration::{DatabaseVersion, Migration};
use crate::{
    contexts::migrations::migration_0038_stac_max_concurrent_cell_searches::Migration0038StacMaxConcurrentCellSearches,
    error::Result,
    processing_graphs::processing_graph::{ProcessingGraph, ProcessingGraphId},
};
use async_trait::async_trait;
use snafu::ResultExt;
use tokio_postgres::Transaction;
use uuid::Uuid;

/// Turns project layers and plots into layers.
///
/// Each layer and plot gets an id that is stable across project versions, plus a description,
/// properties, metadata and an optional reference to the catalog layer it was copied from.
/// Plots also get a visibility.
/// Existing layers get the same id in all versions if they have the same position and
/// processing graph.
/// Rows that refer to a processing graph that does not exist are removed, because the processing
/// graph id becomes a foreign key.
///
/// The processing graph id of a project layer is the hash of its processing graph.
/// Some processing graphs are stored under another id, because earlier migrations rewrote them in
/// place (e.g. `0034_plots_single_raster_source`).
/// These are registered under their hash, and the project layers refer to that id.
pub struct Migration0039ProjectLayerIds;

#[async_trait]
impl Migration for Migration0039ProjectLayerIds {
    fn prev_version(&self) -> Option<DatabaseVersion> {
        Some(Migration0038StacMaxConcurrentCellSearches.version())
    }

    fn version(&self) -> DatabaseVersion {
        "0039_project_layer_ids".into()
    }

    async fn migrate(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.batch_execute(include_str!("migration_0039_project_layer_ids.sql"))
            .await?;

        register_project_layer_processing_graphs_under_hash(tx).await
    }
}

async fn register_project_layer_processing_graphs_under_hash(tx: &Transaction<'_>) -> Result<()> {
    let rows = tx
        .query(
            "SELECT g.id, g.processing_graph
            FROM processing_graphs g
            WHERE g.id IN (
                SELECT processing_graph_id FROM project_version_layers
                UNION
                SELECT processing_graph_id FROM project_version_plots
            )",
            &[],
        )
        .await?;

    for row in rows {
        let stored_id: Uuid = row.get(0);
        let Ok(processing_graph) = serde_json::from_value::<ProcessingGraph>(row.get(1)) else {
            // A graph that cannot be parsed cannot be hashed either. Keep its stored id.
            tracing::warn!(
                processing_graph_id = %stored_id,
                "cannot parse the processing graph of a project layer"
            );
            continue;
        };

        let hash_id = ProcessingGraphId::from_hash(&processing_graph);
        if hash_id.0 == stored_id {
            continue;
        }

        tx.execute(
            "INSERT INTO processing_graphs (id, processing_graph) VALUES ($1, $2)
            ON CONFLICT DO NOTHING;",
            &[
                &hash_id,
                &serde_json::to_value(&processing_graph).context(crate::error::SerdeJson)?,
            ],
        )
        .await?;

        for table in ["project_version_layers", "project_version_plots"] {
            tx.execute(
                &format!(
                    "UPDATE {table} SET processing_graph_id = $1 WHERE processing_graph_id = $2;"
                ),
                &[&hash_id, &stored_id],
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use bb8_postgres::{PostgresConnectionManager, bb8::Pool};
    use tokio_postgres::NoTls;
    use uuid::Uuid;

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
    async fn it_assigns_stable_layer_ids() {
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
                &Migration0038StacMaxConcurrentCellSearches.version(),
            ),
        )
        .await
        .unwrap();

        conn.batch_execute(include_str!("migration_0039_test_data.sql"))
            .await
            .unwrap();

        let tx = conn.transaction().await.unwrap();

        Migration0039ProjectLayerIds.migrate(&tx).await.unwrap();

        let layers: Vec<(String, String, i32, Uuid)> = tx
            .query(
                "SELECT v.description, l.name, l.layer_index, l.layer_id
                FROM project_version_layers l
                    JOIN project_versions v ON (l.project_version_id = v.id)
                ORDER BY v.description, l.layer_index",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect();

        // the orphan layer of version 2 is removed
        assert_eq!(
            layers
                .iter()
                .map(|(version, name, index, _)| (version.as_str(), name.as_str(), *index))
                .collect::<Vec<_>>(),
            vec![
                ("Version 1", "Raster", 0),
                ("Version 1", "Vector", 1),
                ("Version 2", "Raster renamed", 0),
            ]
        );

        // the raster layer keeps its id across versions, the vector layer has another id
        assert_eq!(layers[0].3, layers[2].3);
        assert_ne!(layers[0].3, layers[1].3);

        let plot_ids: Vec<Uuid> = tx
            .query(
                "SELECT layer_id FROM project_version_plots ORDER BY project_version_id",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| row.get(0))
            .collect();

        assert_eq!(plot_ids.len(), 2);
        assert_eq!(plot_ids[0], plot_ids[1]);
        assert_ne!(plot_ids[0], layers[0].3);

        // all project layers refer to their processing graph by its hash
        let processing_graphs: Vec<(Uuid, serde_json::Value)> = tx
            .query(
                "SELECT g.id, g.processing_graph
                FROM project_version_layers l JOIN processing_graphs g
                    ON (l.processing_graph_id = g.id)
                UNION ALL
                SELECT g.id, g.processing_graph
                FROM project_version_plots p JOIN processing_graphs g
                    ON (p.processing_graph_id = g.id)",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();

        assert_eq!(processing_graphs.len(), 5);
        for (id, processing_graph) in processing_graphs {
            let processing_graph: ProcessingGraph =
                serde_json::from_value(processing_graph).unwrap();
            assert_eq!(ProcessingGraphId::from_hash(&processing_graph).0, id);
        }
    }
}
