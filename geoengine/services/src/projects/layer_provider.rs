use crate::error::{Error, Result};
use crate::layers::layer::{
    CollectionItem, Layer, LayerCollection, LayerCollectionListOptions, LayerCollectionListing,
    LayerListing, ProviderLayerCollectionId,
};
use crate::layers::listing::{
    LayerCollectionId, LayerCollectionProvider, ProviderCapabilities, SearchCapabilities,
};
use crate::layers::storage::PROJECTS_PROVIDER_ID;
use crate::projects::{OrderBy, ProjectDb, ProjectId, ProjectListOptions};
use async_trait::async_trait;
use geoengine_datatypes::dataset::LayerId;
use std::str::FromStr;

/// The root collection of the projects data connector, which lists the projects.
pub const PROJECTS_ROOT_COLLECTION_ID: &str = "projects";

/// The projects data connector.
///
/// It lists the projects a user can read as collections, and their layers and plots as layers.
/// Layers inherit the permissions of their project and always come from its latest version.
pub struct ProjectsLayerProvider<'a, D>(pub &'a D);

impl<D> ProjectsLayerProvider<'_, D>
where
    D: ProjectDb,
{
    async fn load_root_collection(
        &self,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let projects = self
            .0
            .list_projects(ProjectListOptions {
                order: OrderBy::NameAsc,
                offset: options.offset,
                limit: options.limit,
            })
            .await?;

        let items = projects
            .into_iter()
            .map(|project| {
                CollectionItem::Collection(LayerCollectionListing {
                    r#type: Default::default(),
                    id: ProviderLayerCollectionId {
                        provider_id: PROJECTS_PROVIDER_ID,
                        collection_id: LayerCollectionId(project.id.to_string()),
                    },
                    name: project.name,
                    description: project.description,
                    properties: vec![],
                })
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: PROJECTS_PROVIDER_ID,
                collection_id: LayerCollectionId(PROJECTS_ROOT_COLLECTION_ID.to_string()),
            },
            name: self.name().to_string(),
            description: self.description().to_string(),
            items,
            entry_label: Some("Project".to_string()),
            properties: vec![],
        })
    }

    async fn load_project_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        let project_id =
            ProjectId::from_str(&collection.0).map_err(|_| Error::IdStringMustBeUuid {
                found: collection.0.clone(),
            })?;

        let project = self.0.load_project(project_id).await?;

        let items = project
            .layers
            .into_iter()
            .chain(project.plots)
            .skip(options.offset as usize)
            .take(options.limit as usize)
            .map(|project_layer| {
                CollectionItem::Layer(LayerListing {
                    r#type: Default::default(),
                    id: project_layer.layer.id,
                    name: project_layer.layer.name,
                    description: project_layer.layer.description,
                    properties: project_layer.layer.properties,
                })
            })
            .collect();

        Ok(LayerCollection {
            id: ProviderLayerCollectionId {
                provider_id: PROJECTS_PROVIDER_ID,
                collection_id: collection.clone(),
            },
            name: project.name,
            description: project.description,
            items,
            entry_label: None,
            properties: vec![],
        })
    }
}

#[async_trait]
impl<D> LayerCollectionProvider for ProjectsLayerProvider<'_, D>
where
    D: ProjectDb,
{
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            listing: true,
            search: SearchCapabilities::none(),
        }
    }

    fn name(&self) -> &'static str {
        "Projects"
    }

    fn description(&self) -> &'static str {
        "The layers and plots of your projects and of the projects shared with you"
    }

    async fn load_layer_collection(
        &self,
        collection: &LayerCollectionId,
        options: LayerCollectionListOptions,
    ) -> Result<LayerCollection> {
        if collection.0 == PROJECTS_ROOT_COLLECTION_ID {
            self.load_root_collection(options).await
        } else {
            self.load_project_collection(collection, options).await
        }
    }

    async fn get_root_layer_collection_id(&self) -> Result<LayerCollectionId> {
        Ok(LayerCollectionId(PROJECTS_ROOT_COLLECTION_ID.to_string()))
    }

    async fn load_layer(&self, id: &LayerId) -> Result<Layer> {
        Ok(self.0.load_project_layer(id).await?.layer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{ApplicationContext, PostgresContext, Session, SessionContext};
    use crate::ge_context;
    use crate::permissions::{Permission, PermissionDb};
    use crate::processing_graphs::{
        processing_graph::{ProcessingGraph, ProcessingGraphId},
        registry::ProcessingGraphRegistry,
    };
    use crate::projects::{
        LayerUpdate, PlotUpdate, PointSymbology, Project, ProjectLayerUpdate, Symbology,
        UpdateProject,
    };
    use crate::users::{UserAuth, UserSession};
    use crate::util::tests::{create_project_helper, read_body_string, send_test_request};
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::Coordinate2D;
    use geoengine_operators::engine::{PlotOperator, TypedOperator, VectorOperator};
    use geoengine_operators::mock::{MockPointSource, MockPointSourceParams};
    use geoengine_operators::plot::{Statistics, StatisticsParams};
    use serde_json::Value;
    use tokio_postgres::NoTls;

    fn layer_update(
        name: &str,
        processing_graph: ProcessingGraphId,
        symbology: Option<Symbology>,
    ) -> ProjectLayerUpdate {
        ProjectLayerUpdate {
            id: None,
            name: name.to_string(),
            description: format!("{name} description"),
            processing_graph,
            symbology,
            properties: vec![],
            metadata: Default::default(),
            visibility: Default::default(),
            source: None,
        }
    }

    /// Creates a project with a point layer and a statistics plot of the points.
    async fn create_project_with_layer_and_plot(
        app_ctx: &PostgresContext<NoTls>,
        session: &UserSession,
    ) -> Project {
        let ctx = app_ctx.session_context(session.clone());
        let db = ctx.db();

        let points = || {
            MockPointSource {
                params: MockPointSourceParams::new(vec![Coordinate2D::new(1., 2.); 3]),
            }
            .typed()
        };

        let layer_graph = db
            .register_processing_graph(ProcessingGraph {
                operator: TypedOperator::Vector(points()),
            })
            .await
            .unwrap();

        let plot_graph = db
            .register_processing_graph(ProcessingGraph {
                operator: Statistics {
                    params: StatisticsParams {
                        column_names: vec![],
                        percentiles: vec![],
                    },
                    sources: points().into(),
                }
                .typed()
                .into(),
            })
            .await
            .unwrap();

        let project = create_project_helper(&ctx).await;

        db.update_project(UpdateProject {
            id: project,
            name: None,
            description: None,
            layers: Some(vec![LayerUpdate::UpdateOrInsert(layer_update(
                "Points",
                layer_graph,
                Some(PointSymbology::default().into()),
            ))]),
            plots: Some(vec![PlotUpdate::UpdateOrInsert(layer_update(
                "Statistics",
                plot_graph,
                None,
            ))]),
            bounds: None,
            time_step: None,
        })
        .await
        .unwrap()
    }

    async fn get_json(
        app_ctx: &PostgresContext<NoTls>,
        session: &UserSession,
        uri: &str,
    ) -> (u16, Value) {
        let req = test::TestRequest::get()
            .uri(uri)
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx.clone()).await;
        let status = res.status().as_u16();
        let body = read_body_string(res).await;
        (
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        )
    }

    fn item_names(collection: &Value) -> Vec<&str> {
        collection["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["name"].as_str().unwrap())
            .collect()
    }

    #[ge_context::test]
    async fn it_lists_the_projects_data_connector(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let (status, root) =
            get_json(&app_ctx, &session, "/layers/collections?offset=0&limit=20").await;

        assert_eq!(status, 200, "{root}");
        assert!(root["items"].as_array().unwrap().iter().any(|item| {
            item["id"]["providerId"] == PROJECTS_PROVIDER_ID.to_string()
                && item["id"]["collectionId"] == PROJECTS_ROOT_COLLECTION_ID
        }));
    }

    #[ge_context::test]
    async fn it_loads_project_layers_with_the_permissions_of_the_project(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let owner = app_ctx.create_anonymous_session().await.unwrap();
        let other = app_ctx.create_anonymous_session().await.unwrap();

        let project = create_project_with_layer_and_plot(&app_ctx, &owner).await;
        let layer_id = &project.layers[0].layer.id.layer_id;
        let plot_id = &project.plots[0].layer.id.layer_id;

        let root_uri =
            format!("/layers/collections/{PROJECTS_PROVIDER_ID}/projects?offset=0&limit=20");
        let project_uri = format!(
            "/layers/collections/{PROJECTS_PROVIDER_ID}/{}?offset=0&limit=20",
            project.id
        );
        let layer_uri = format!("/layers/{PROJECTS_PROVIDER_ID}/{layer_id}");
        let plot_uri = format!("/layers/{PROJECTS_PROVIDER_ID}/{plot_id}");

        // the owner sees the project and its layers
        let (status, root) = get_json(&app_ctx, &owner, &root_uri).await;
        assert_eq!(status, 200, "{root}");
        assert_eq!(item_names(&root), vec!["Test"]);

        let (status, collection) = get_json(&app_ctx, &owner, &project_uri).await;
        assert_eq!(status, 200, "{collection}");
        assert_eq!(item_names(&collection), vec!["Points", "Statistics"]);

        let (status, layer) = get_json(&app_ctx, &owner, &layer_uri).await;
        assert_eq!(status, 200, "{layer}");
        assert_eq!(layer["name"], "Points");
        assert_eq!(layer["description"], "Points description");
        assert_eq!(layer["id"]["providerId"], PROJECTS_PROVIDER_ID.to_string());
        assert_eq!(layer["id"]["layerId"], layer_id.0);
        assert_eq!(layer["symbology"]["type"], "point");

        let (status, plot) = get_json(&app_ctx, &owner, &plot_uri).await;
        assert_eq!(status, 200, "{plot}");
        assert_eq!(plot["name"], "Statistics");
        assert_eq!(plot["symbology"], Value::Null);

        // other users see neither the project nor its layers
        let (status, root) = get_json(&app_ctx, &other, &root_uri).await;
        assert_eq!(status, 200, "{root}");
        assert_eq!(item_names(&root), Vec::<&str>::new());

        let (status, _) = get_json(&app_ctx, &other, &project_uri).await;
        assert_ne!(status, 200);

        let (status, _) = get_json(&app_ctx, &other, &layer_uri).await;
        assert_ne!(status, 200);

        // until the project is shared with them
        app_ctx
            .session_context(owner.clone())
            .db()
            .add_permission(other.user.id.into(), project.id, Permission::Read)
            .await
            .unwrap();

        let (status, root) = get_json(&app_ctx, &other, &root_uri).await;
        assert_eq!(status, 200, "{root}");
        assert_eq!(item_names(&root), vec!["Test"]);

        let (status, layer) = get_json(&app_ctx, &other, &layer_uri).await;
        assert_eq!(status, 200, "{layer}");
        assert_eq!(layer["name"], "Points");
    }

    #[ge_context::test]
    async fn it_returns_the_latest_version_of_a_project_layer(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let project = create_project_with_layer_and_plot(&app_ctx, &session).await;
        let layer = &project.layers[0];

        let mut renamed = layer_update(
            "Renamed",
            ProcessingGraphId::from_hash(&layer.layer.processing_graph),
            layer.layer.symbology.clone(),
        );
        renamed.id = Some(layer.layer.id.layer_id.clone());

        app_ctx
            .session_context(session.clone())
            .db()
            .update_project(UpdateProject {
                id: project.id,
                name: None,
                description: None,
                layers: Some(vec![LayerUpdate::UpdateOrInsert(renamed)]),
                plots: None,
                bounds: None,
                time_step: None,
            })
            .await
            .unwrap();

        let (status, loaded) = get_json(
            &app_ctx,
            &session,
            &format!("/layers/{PROJECTS_PROVIDER_ID}/{}", layer.layer.id.layer_id),
        )
        .await;
        assert_eq!(status, 200, "{loaded}");
        assert_eq!(loaded["name"], "Renamed");
    }

    #[ge_context::test]
    async fn it_queries_project_layers_via_wfs(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let project = create_project_with_layer_and_plot(&app_ctx, &session).await;
        let layer_id = &project.layers[0].layer.id.layer_id;

        let type_names = format!("layer:{PROJECTS_PROVIDER_ID}:{layer_id}");
        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("typeNames", type_names.as_str()),
            ("bbox", "-90,-180,90,180"),
            ("srsName", "EPSG:4326"),
        ];

        let (status, features) = get_json(
            &app_ctx,
            &session,
            &format!(
                "/wfs/{layer_id}?{}",
                serde_urlencoded::to_string(params).unwrap()
            ),
        )
        .await;

        assert_eq!(status, 200, "{features}");
        assert_eq!(features["type"], "FeatureCollection");
        assert_eq!(features["features"].as_array().unwrap().len(), 3);
    }

    #[ge_context::test]
    async fn it_plots_project_plots(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let project = create_project_with_layer_and_plot(&app_ctx, &session).await;
        let plot_id = &project.plots[0].layer.id.layer_id;

        let params = &[
            ("bbox", "-180,-90,180,90"),
            ("crs", "EPSG:4326"),
            ("time", "2020-01-01T00:00:00.0Z"),
        ];

        let (status, plot) = get_json(
            &app_ctx,
            &session,
            &format!(
                "/plot/{PROJECTS_PROVIDER_ID}/{plot_id}?{}",
                serde_urlencoded::to_string(params).unwrap()
            ),
        )
        .await;

        assert_eq!(status, 200, "{plot}");
        assert_eq!(plot["plotType"], "Statistics");
    }
}
