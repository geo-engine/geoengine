use super::error::PostgresProjectDbError;
use super::error::ProjectDbError;
use crate::contexts::PostgresDb;
use crate::error::Result;
use crate::layers::layer::{Layer, ProviderLayerId};
use crate::permissions::Permission;
use crate::permissions::TxPermissionDb;
use crate::processing_graphs::processing_graph::{ProcessingGraph, ProcessingGraphId};
use crate::projects::LoadVersion;
use crate::projects::ProjectLayer;
use crate::projects::VecUpdate;
use crate::projects::error::{
    AccessFailedProjectDbError, Bb8ProjectDbError, InvalidProcessingGraphProjectDbError,
    ProjectNotFoundProjectDbError,
};
use crate::projects::parse_layer_uuid;
use crate::projects::{
    CreateProject, Project, ProjectDb, ProjectId, ProjectListOptions, ProjectListing,
    ProjectVersion, ProjectVersionId, UpdateProject,
};
use crate::users::UserId;
use crate::util::Identifier;
use async_trait::async_trait;
use bb8_postgres::tokio_postgres::Transaction;
use bb8_postgres::{
    tokio_postgres::Socket, tokio_postgres::tls::MakeTlsConnect, tokio_postgres::tls::TlsConnect,
};
use geoengine_datatypes::dataset::{DataProviderId, LayerId};
use geoengine_datatypes::error::BoxedResultExt;
use geoengine_datatypes::util::HashMapTextTextDbType;
use snafu::{ResultExt, ensure};
use std::collections::HashMap;
use tokio_postgres::Row;
use uuid::Uuid;

#[async_trait]
impl<Tls> ProjectDb for PostgresDb<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static + std::fmt::Debug,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    async fn list_projects(
        &self,
        options: ProjectListOptions,
    ) -> Result<Vec<ProjectListing>, ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        let stmt = trans
            .prepare(&format!(
                "
        SELECT p.id, p.project_id, p.name, p.description, p.changed 
        FROM user_permitted_projects u JOIN project_versions p ON (u.project_id = p.project_id)
        WHERE
            u.user_id = $1
            AND p.changed >= ALL (SELECT changed FROM project_versions WHERE project_id = p.project_id)
        ORDER BY p.{}
        LIMIT $2
        OFFSET $3;",
                options.order.to_sql_string()
            ))
            .await.context(PostgresProjectDbError)?;

        let project_rows = trans
            .query(
                &stmt,
                &[
                    &self.session.user.id,
                    &i64::from(options.limit),
                    &i64::from(options.offset),
                ],
            )
            .await
            .context(PostgresProjectDbError)?;

        let project_listings = project_listings_from_rows(&trans, project_rows).await?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project_listings)
    }

    async fn create_project(&self, create: CreateProject) -> Result<ProjectId, ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

        let project: Project = Project::from_create_project(create);

        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        let version_id = insert_project(&trans, &project).await?;

        let stmt = trans
            .prepare(
                "INSERT INTO 
                    project_version_authors (project_version_id, user_id) 
                VALUES 
                    ($1, $2);",
            )
            .await
            .context(PostgresProjectDbError)?;

        trans
            .execute(&stmt, &[&version_id, &self.session.user.id])
            .await
            .context(PostgresProjectDbError)?;

        let stmt = trans
            .prepare(
                "INSERT INTO permissions (role_id, permission, project_id) VALUES ($1, $2, $3);",
            )
            .await
            .context(PostgresProjectDbError)?;

        trans
            .execute(
                &stmt,
                &[&self.session.user.id, &Permission::Owner, &project.id],
            )
            .await
            .context(PostgresProjectDbError)?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project.id)
    }

    async fn load_project(&self, project: ProjectId) -> Result<Project, ProjectDbError> {
        self.load_project_version(project, LoadVersion::Latest)
            .await
    }

    #[allow(clippy::too_many_lines)]
    async fn update_project(&self, update: UpdateProject) -> Result<Project, ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;

        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        self.ensure_permission_in_tx(update.id.into(), Permission::Owner, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project: update.id })?;

        let project = self.load_project(update.id).await?; // TODO: move inside transaction?

        let project = update_project(&trans, &project, update).await?;

        let stmt = trans
            .prepare(
                "INSERT INTO 
                    project_version_authors (project_version_id, user_id) 
                VALUES 
                    ($1, $2);",
            )
            .await
            .context(PostgresProjectDbError)?;

        trans
            .execute(&stmt, &[&project.version.id, &self.session.user.id])
            .await
            .context(PostgresProjectDbError)?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project)
    }

    async fn delete_project(&self, project: ProjectId) -> Result<(), ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;
        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        self.ensure_permission_in_tx(project.into(), Permission::Owner, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project })?;

        let stmt = trans
            .prepare("DELETE FROM projects WHERE id = $1;")
            .await
            .context(PostgresProjectDbError)?;

        let rows_affected = trans
            .execute(&stmt, &[&project])
            .await
            .context(PostgresProjectDbError)?;

        trans.commit().await.context(PostgresProjectDbError)?;

        ensure!(
            rows_affected == 1,
            ProjectNotFoundProjectDbError { project }
        );

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn load_project_version(
        &self,
        project: ProjectId,
        version: LoadVersion,
    ) -> Result<Project, ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;
        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        self.ensure_permission_in_tx(project.into(), Permission::Read, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project })?;

        let rows = if let LoadVersion::Version(version) = version {
            let stmt = trans
                .prepare(
                    "
            SELECT 
                p.project_id, 
                p.id, 
                p.name, 
                p.description,
                p.bounds,
                p.time_step,
                p.changed,
                a.user_id
            FROM 
                project_versions p JOIN project_version_authors a ON (p.id = a.project_version_id)
            WHERE p.project_id = $1 AND p.id = $2",
                )
                .await
                .context(PostgresProjectDbError)?;

            let rows = trans
                .query(&stmt, &[&project, &version])
                .await
                .context(PostgresProjectDbError)?;

            if rows.is_empty() {
                return Err(ProjectDbError::ProjectVersionNotFound { project, version });
            }

            rows
        } else {
            let stmt = trans
                .prepare(
                    "
            SELECT  
                p.project_id, 
                p.id, 
                p.name, 
                p.description,
                p.bounds,
                p.time_step,
                p.changed,
                a.user_id
            FROM 
                project_versions p JOIN project_version_authors a ON (p.id = a.project_version_id)
            WHERE project_id = $1 AND p.changed >= ALL(
                SELECT changed FROM project_versions WHERE project_id = $1
            )",
                )
                .await
                .context(PostgresProjectDbError)?;

            let rows = trans
                .query(&stmt, &[&project])
                .await
                .context(PostgresProjectDbError)?;

            if rows.is_empty() {
                return Err(ProjectDbError::ProjectNotFound { project });
            }

            rows
        };

        let row = &rows[0];

        let project_id = ProjectId(row.get(0));
        let version_id = ProjectVersionId(row.get(1));
        let name = row.get(2);
        let description = row.get(3);
        let bounds = row.get(4);
        let time_step = row.get(5);
        let changed = row.get(6);
        let _author_id = UserId(row.get(7));

        let project = Project {
            id: project_id,
            version: ProjectVersion {
                id: version_id,
                changed,
            },
            name,
            description,
            layers: load_layers(&trans, LayerTable::Layers, &version_id).await?,
            plots: load_layers(&trans, LayerTable::Plots, &version_id).await?,
            bounds,
            time_step,
        };

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project)
    }

    async fn load_project_layer(&self, layer: &LayerId) -> Result<ProjectLayer, ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;
        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        let (project, project_layer) =
            load_latest_project_layer(&trans, layer)
                .await?
                .ok_or_else(|| ProjectDbError::LayerNotFound {
                    layer: layer.clone(),
                })?;

        self.ensure_permission_in_tx(project.into(), Permission::Read, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project })?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(project_layer)
    }

    async fn list_project_versions(
        &self,
        project: ProjectId,
    ) -> Result<Vec<ProjectVersion>, ProjectDbError> {
        let mut conn = self.conn_pool.get().await.context(Bb8ProjectDbError)?;
        let trans = conn
            .build_transaction()
            .start()
            .await
            .context(PostgresProjectDbError)?;

        self.ensure_permission_in_tx(project.into(), Permission::Read, &trans)
            .await
            .boxed_context(AccessFailedProjectDbError { project })?;

        let stmt = trans
            .prepare(
                "
                SELECT 
                    p.id, p.changed, a.user_id
                FROM 
                    project_versions p JOIN project_version_authors a ON (p.id = a.project_version_id)
                WHERE 
                    project_id = $1 
                ORDER BY 
                    p.changed DESC, a.user_id DESC",
            )
            .await.context(PostgresProjectDbError)?;

        let rows = trans
            .query(&stmt, &[&project])
            .await
            .context(PostgresProjectDbError)?;

        trans.commit().await.context(PostgresProjectDbError)?;

        Ok(rows
            .iter()
            .map(|row| ProjectVersion {
                id: ProjectVersionId(row.get(0)),
                changed: row.get(1),
            })
            .collect())
    }
}

pub async fn list_plots(
    trans: &Transaction<'_>,
    project_version_id: &ProjectVersionId,
) -> Result<Vec<String>, ProjectDbError> {
    let stmt: tokio_postgres::Statement = trans
        .prepare(
            "
                    SELECT name
                    FROM project_version_plots
                    WHERE project_version_id = $1;
                ",
        )
        .await
        .context(PostgresProjectDbError)?;

    let plot_rows = trans
        .query(&stmt, &[project_version_id])
        .await
        .context(PostgresProjectDbError)?;
    let plot_names = plot_rows.iter().map(|row| row.get(0)).collect();

    Ok(plot_names)
}

/// The tables that store the layers and the plots of a project version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayerTable {
    Layers,
    Plots,
}

impl LayerTable {
    fn table(self) -> &'static str {
        match self {
            LayerTable::Layers => "project_version_layers",
            LayerTable::Plots => "project_version_plots",
        }
    }

    fn index_column(self) -> &'static str {
        match self {
            LayerTable::Layers => "layer_index",
            LayerTable::Plots => "plot_index",
        }
    }

    /// Plots have no symbology, so their column is always `NULL`.
    fn symbology_column(self) -> &'static str {
        match self {
            LayerTable::Layers => "l.symbology",
            LayerTable::Plots => "NULL::\"Symbology\"",
        }
    }
}

/// The columns that [`project_layer_from_row`] reads, for a table aliased `l` that is joined with
/// `processing_graphs` aliased `g`.
fn project_layer_columns(table: LayerTable) -> String {
    format!(
        "l.layer_id, l.name, l.description, g.processing_graph, {symbology},
        l.properties, l.metadata, l.visibility, l.source_provider_id, l.source_layer_id",
        symbology = table.symbology_column()
    )
}

fn project_layer_from_row(row: &Row, offset: usize) -> Result<ProjectLayer, ProjectDbError> {
    let processing_graph: ProcessingGraph = serde_json::from_value(row.get(offset + 3))
        .context(InvalidProcessingGraphProjectDbError)?;

    let source_provider_id: Option<Uuid> = row.get(offset + 8);
    let source_layer_id: Option<String> = row.get(offset + 9);
    let source = source_provider_id
        .zip(source_layer_id)
        .map(|(provider_id, layer_id)| ProviderLayerId {
            provider_id: DataProviderId(provider_id),
            layer_id: LayerId(layer_id),
        });

    Ok(ProjectLayer {
        layer: Layer {
            id: ProjectLayer::provider_layer_id(row.get(offset)),
            name: row.get(offset + 1),
            description: row.get(offset + 2),
            processing_graph,
            symbology: row.get(offset + 4),
            properties: row.get(offset + 5),
            metadata: row.get::<_, HashMapTextTextDbType>(offset + 6).into(),
        },
        visibility: row.get(offset + 7),
        source,
    })
}

pub async fn load_layers(
    trans: &Transaction<'_>,
    table: LayerTable,
    project_version_id: &ProjectVersionId,
) -> Result<Vec<ProjectLayer>, ProjectDbError> {
    let stmt = trans
        .prepare(&format!(
            "
            SELECT {columns}
            FROM {table} l JOIN processing_graphs g ON (l.processing_graph_id = g.id)
            WHERE l.project_version_id = $1
            ORDER BY l.{index} ASC",
            columns = project_layer_columns(table),
            table = table.table(),
            index = table.index_column(),
        ))
        .await
        .context(PostgresProjectDbError)?;

    let rows = trans
        .query(&stmt, &[project_version_id])
        .await
        .context(PostgresProjectDbError)?;

    rows.iter()
        .map(|row| project_layer_from_row(row, 0))
        .collect()
}

/// Loads a layer or plot from the latest version of its project.
///
/// Returns the project of the layer, so that the caller can check the permissions.
pub async fn load_latest_project_layer(
    trans: &Transaction<'_>,
    layer: &LayerId,
) -> Result<Option<(ProjectId, ProjectLayer)>, ProjectDbError> {
    let layer_uuid = parse_layer_uuid(layer)?;

    for table in [LayerTable::Layers, LayerTable::Plots] {
        let stmt = trans
            .prepare(&format!(
                "
                SELECT l.project_id, {columns}
                FROM {table} l JOIN processing_graphs g ON (l.processing_graph_id = g.id)
                WHERE l.layer_id = $1 AND l.project_version_id = (
                    SELECT v.id
                    FROM project_versions v
                    WHERE v.project_id = l.project_id
                    ORDER BY v.changed DESC
                    LIMIT 1
                )",
                columns = project_layer_columns(table),
                table = table.table(),
            ))
            .await
            .context(PostgresProjectDbError)?;

        let row = trans
            .query_opt(&stmt, &[&layer_uuid])
            .await
            .context(PostgresProjectDbError)?;

        if let Some(row) = row {
            return Ok(Some((
                ProjectId(row.get(0)),
                project_layer_from_row(&row, 1)?,
            )));
        }
    }

    Ok(None)
}

pub async fn insert_layers(
    trans: &Transaction<'_>,
    table: LayerTable,
    project_id: &ProjectId,
    project_version_id: &ProjectVersionId,
    layers: &[ProjectLayer],
) -> Result<(), ProjectDbError> {
    let (symbology_column, symbology_value) = match table {
        LayerTable::Layers => (", symbology", ", $13"),
        LayerTable::Plots => ("", ""),
    };

    let stmt = trans
        .prepare(&format!(
            "
            INSERT INTO {table} (
                project_id,
                project_version_id,
                {index},
                layer_id,
                name,
                description,
                processing_graph_id,
                properties,
                metadata,
                visibility,
                source_provider_id,
                source_layer_id
                {symbology_column})
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12 {symbology_value});",
            table = table.table(),
            index = table.index_column(),
        ))
        .await
        .context(PostgresProjectDbError)?;

    // The id of a processing graph is its hash. Older processing graphs may be stored under
    // another id, so make sure that the graph is also registered under its hash.
    let register_stmt = trans
        .prepare(
            "INSERT INTO processing_graphs (id, processing_graph) VALUES ($1, $2)
            ON CONFLICT DO NOTHING;",
        )
        .await
        .context(PostgresProjectDbError)?;

    for (idx, layer) in layers.iter().enumerate() {
        let index = idx as i32;
        let processing_graph_id = ProcessingGraphId::from_hash(&layer.layer.processing_graph);
        trans
            .execute(
                &register_stmt,
                &[
                    &processing_graph_id,
                    &serde_json::to_value(&layer.layer.processing_graph)
                        .context(InvalidProcessingGraphProjectDbError)?,
                ],
            )
            .await
            .context(PostgresProjectDbError)?;

        let layer_uuid = layer.layer_uuid()?;
        let metadata = HashMapTextTextDbType::from(&layer.layer.metadata);
        let source_provider_id = layer.source.as_ref().map(|source| source.provider_id);
        let source_layer_id = layer
            .source
            .as_ref()
            .map(|source| source.layer_id.0.clone());

        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![
            project_id,
            project_version_id,
            &index,
            &layer_uuid,
            &layer.layer.name,
            &layer.layer.description,
            &processing_graph_id,
            &layer.layer.properties,
            &metadata,
            &layer.visibility,
            &source_provider_id,
            &source_layer_id,
        ];
        if table == LayerTable::Layers {
            params.push(&layer.layer.symbology);
        }

        trans
            .execute(&stmt, &params)
            .await
            .context(PostgresProjectDbError)?;
    }

    Ok(())
}

/// Loads the processing graphs that the layers and plots of an update refer to.
async fn load_update_processing_graphs(
    trans: &Transaction<'_>,
    update: &UpdateProject,
) -> Result<HashMap<ProcessingGraphId, ProcessingGraph>, ProjectDbError> {
    let ids: Vec<Uuid> = update
        .layers
        .iter()
        .chain(update.plots.iter())
        .flatten()
        .filter_map(|layer_update| match layer_update {
            VecUpdate::UpdateOrInsert(layer) => Some(layer.processing_graph.0),
            VecUpdate::None(_) | VecUpdate::Delete(_) => None,
        })
        .collect();

    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let stmt = trans
        .prepare("SELECT id, processing_graph FROM processing_graphs WHERE id = ANY($1);")
        .await
        .context(PostgresProjectDbError)?;

    let rows = trans
        .query(&stmt, &[&ids])
        .await
        .context(PostgresProjectDbError)?;

    rows.iter()
        .map(|row| {
            let processing_graph =
                serde_json::from_value(row.get(1)).context(InvalidProcessingGraphProjectDbError)?;
            Ok((ProcessingGraphId(row.get(0)), processing_graph))
        })
        .collect()
}

pub async fn project_listings_from_rows(
    tx: &tokio_postgres::Transaction<'_>,
    project_rows: Vec<Row>,
) -> Result<Vec<ProjectListing>, ProjectDbError> {
    let mut project_listings = vec![];
    for project_row in project_rows {
        let project_version_id = ProjectVersionId(project_row.get(0));
        let project_id = ProjectId(project_row.get(1));
        let name = project_row.get(2);
        let description = project_row.get(3);
        let changed = project_row.get(4);

        let stmt = tx
            .prepare(
                "
                SELECT name
                FROM project_version_layers
                WHERE project_version_id = $1;",
            )
            .await
            .context(PostgresProjectDbError)?;

        let layer_rows = tx
            .query(&stmt, &[&project_version_id])
            .await
            .context(PostgresProjectDbError)?;
        let layer_names = layer_rows.iter().map(|row| row.get(0)).collect();

        project_listings.push(ProjectListing {
            id: project_id,
            name,
            description,
            layer_names,
            plot_names: list_plots(tx, &project_version_id).await?,
            changed,
        });
    }
    Ok(project_listings)
}

pub async fn insert_project(
    trans: &Transaction<'_>,
    project: &Project,
) -> Result<ProjectVersionId, ProjectDbError> {
    let stmt = trans
        .prepare("INSERT INTO projects (id) VALUES ($1);")
        .await
        .context(PostgresProjectDbError)?;

    trans
        .execute(&stmt, &[&project.id])
        .await
        .context(PostgresProjectDbError)?;

    let stmt = trans
        .prepare(
            "INSERT INTO project_versions (
                    id,
                    project_id,
                    name,
                    description,
                    bounds,
                    time_step,
                    changed)
                    VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP);",
        )
        .await
        .context(PostgresProjectDbError)?;

    let version_id = ProjectVersionId::new();

    trans
        .execute(
            &stmt,
            &[
                &version_id,
                &project.id,
                &project.name,
                &project.description,
                &project.bounds,
                &project.time_step,
            ],
        )
        .await
        .context(PostgresProjectDbError)?;

    Ok(version_id)
}

pub async fn update_project(
    trans: &Transaction<'_>,
    project: &Project,
    update: UpdateProject,
) -> Result<Project, ProjectDbError> {
    let processing_graphs = load_update_processing_graphs(trans, &update).await?;

    let project = project.update_project(update, &processing_graphs)?;

    let stmt = trans
        .prepare(
            "
            INSERT INTO project_versions (
                id,
                project_id,
                name,
                description,
                bounds,
                time_step,
                changed)
            VALUES ($1, $2, $3, $4, $5, $6, $7);",
        )
        .await
        .context(PostgresProjectDbError)?;

    trans
        .execute(
            &stmt,
            &[
                &project.version.id,
                &project.id,
                &project.name,
                &project.description,
                &project.bounds,
                &project.time_step,
                &project.version.changed,
            ],
        )
        .await
        .context(PostgresProjectDbError)?;

    insert_layers(
        trans,
        LayerTable::Layers,
        &project.id,
        &project.version.id,
        &project.layers,
    )
    .await?;

    insert_layers(
        trans,
        LayerTable::Plots,
        &project.id,
        &project.version.id,
        &project.plots,
    )
    .await?;

    Ok(project)
}
