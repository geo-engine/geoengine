use crate::contexts::ApplicationContext;
use crate::error::Result;
use crate::handlers::SessionContext;
use crate::projects::{CreateProject, ProjectDb, ProjectId, ProjectListOptions, UpdateProject};
use crate::util::extractors::{ValidatedJson, ValidatedQuery};
use crate::util::IdResponse;
use actix_web::{web, FromRequest, HttpResponse, Responder};

pub(crate) fn init_project_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/project").route(web::post().to(create_project_handler::<C>)))
        .service(web::resource("/projects").route(web::get().to(list_projects_handler::<C>)))
        .service(
            web::resource("/project/{project}")
                .route(web::get().to(load_project_handler::<C>))
                .route(web::patch().to(update_project_handler::<C>))
                .route(web::delete().to(delete_project_handler::<C>)),
        );
}

/// Create a new project for the user.
#[utoipa::path(
    tag = "Projects",
    post,
    path = "/project",
    request_body = CreateProject,
    responses(
        (status = 200, response = crate::api::model::responses::IdResponse)
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn create_project_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    create: ValidatedJson<CreateProject>,
) -> Result<impl Responder> {
    let create = create.into_inner();
    let id = app_ctx
        .session_context(session)
        .db()
        .create_project(create)
        .await?;
    Ok(web::Json(IdResponse::from(id)))
}

/// List all projects accessible to the user that match the selected criteria.
#[utoipa::path(
    tag = "Projects",
    get,
    path = "/projects",
    responses(
        (status = 200, description = "List of projects the user can access", body = [ProjectListing],
            example = json!([
                {
                    "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
                    "name": "Test",
                    "description": "Foo",
                    "layerNames": [],
                    "plotNames": [],
                    "changed": "2021-04-26T14:03:51.984537900Z"
                }
            ])
        )
    ),
    params(ProjectListOptions),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn list_projects_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    options: ValidatedQuery<ProjectListOptions>,
) -> Result<impl Responder> {
    let options = options.into_inner();
    let listing = app_ctx
        .session_context(session)
        .db()
        .list_projects(options)
        .await?;
    Ok(web::Json(listing))
}

/// Retrieves details about the latest version of a project.
#[utoipa::path(
    tag = "Projects",
    get,
    path = "/project/{project}",
    responses(
        (status = 200, description = "Project loaded from database", body = Project,
            example = json!({
                "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
                "version": {
                    "id": "8f4b8683-f92c-4129-a16f-818aeeee484e",
                    "changed": "2021-04-26T14:05:39.677390600Z",
                    "author": "5b4466d2-8bab-4ed8-a182-722af3c80958"
                },
                "name": "Test",
                "description": "Foo",
                "layers": [],
                "plots": [],
                "bounds": {
                    "spatialReference": "EPSG:4326",
                    "boundingBox": {
                        "lowerLeftCoordinate": {
                            "x": 0.0,
                            "y": 0.0
                        },
                        "upperRightCoordinate": {
                            "x": 1.0,
                            "y": 1.0
                        }
                    },
                    "timeInterval": {
                        "start": 0,
                        "end": 1
                    }
                },
                "timeStep": {
                    "granularity": "months",
                    "step": 1
                }
            })
        )
    ),
    params(
        ("project" = ProjectId, description = "Project id")
    ),
    security(
        ("session_token" = [])
    )
)]
async fn load_project_handler<C: ApplicationContext>(
    project: web::Path<ProjectId>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    let id = app_ctx
        .session_context(session)
        .db()
        .load_project(project.into_inner())
        .await?;
    Ok(web::Json(id))
}

/// Updates a project.
/// This will create a new version.
#[utoipa::path(
    tag = "Projects",
    patch,
    path = "/project/{project}",
    request_body = UpdateProject,
    responses(
        (status = 200, description = "OK")
    ),
    params(
        ("project" = ProjectId, description = "Project id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn update_project_handler<C: ApplicationContext>(
    project: web::Path<ProjectId>,
    session: C::Session,
    app_ctx: web::Data<C>,
    update: ValidatedJson<UpdateProject>,
) -> Result<impl Responder> {
    let mut update = update.into_inner();
    update.id = project.into_inner(); // TODO: avoid passing project id in path AND body
    app_ctx
        .session_context(session)
        .db()
        .update_project(update)
        .await?;
    Ok(HttpResponse::Ok())
}

/// Deletes a project.
#[utoipa::path(
    tag = "Projects",
    delete,
    path = "/project/{project}",
    responses(
        (status = 200, description = "OK")
    ),
    params(
        ("project" = ProjectId, description = "Project id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn delete_project_handler<C: ApplicationContext>(
    project: web::Path<ProjectId>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    app_ctx
        .session_context(session)
        .db()
        .delete_project(*project)
        .await?;
    Ok(HttpResponse::Ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::datatypes::Colorizer;
    use crate::contexts::{Session, SimpleApplicationContext, SimpleSession};
    use crate::handlers::ErrorResponse;
    use crate::util::tests::{
        check_allowed_http_methods, create_project_helper, send_test_request, update_project_helper,
    };
    use crate::util::Identifier;
    use crate::workflows::workflow::WorkflowId;
    use crate::{
        contexts::InMemoryContext,
        projects::{
            LayerUpdate, LayerVisibility, Plot, PlotUpdate, Project, ProjectId, ProjectLayer,
            ProjectListing, RasterSymbology, STRectangle, Symbology, UpdateProject,
        },
    };
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::primitives::{TimeGranularity, TimeStep};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use serde_json::json;

    async fn create_test_helper(method: Method) -> ServiceResponse {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session_id = ctx.session().id();

        let create = CreateProject {
            name: "Test".to_string(),
            description: "Foo".to_string(),
            bounds: STRectangle::new(SpatialReference::epsg_4326(), 0., 0., 1., 1., 0, 1).unwrap(),
            time_step: Some(TimeStep {
                step: 1,
                granularity: TimeGranularity::Months,
            }),
        };

        let req = test::TestRequest::default()
            .method(method)
            .uri("/project")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(&create);
        send_test_request(req, app_ctx).await
    }

    #[tokio::test]
    async fn create() {
        let res = create_test_helper(Method::POST).await;

        assert_eq!(res.status(), 200);

        let _project: IdResponse<ProjectId> = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn create_invalid_method() {
        check_allowed_http_methods(create_test_helper, &[Method::POST]).await;
    }

    #[tokio::test]
    async fn create_invalid_body() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session_id = ctx.session().id();

        let req = test::TestRequest::post()
            .uri("/project")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_payload("no json");
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        )
        .await;
    }

    #[tokio::test]
    async fn create_missing_fields() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session_id = ctx.session().id();

        let create = json!({
            "description": "Foo".to_string(),
            "bounds": STRectangle::new(SpatialReference::epsg_4326(), 0., 0., 1., 1., 0, 1).unwrap(),
        });

        let req = test::TestRequest::post()
            .uri("/project")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())))
            .set_json(&create);
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `name` at line 1 column 195",
        )
        .await;
    }

    #[tokio::test]
    async fn create_missing_header() {
        let app_ctx = InMemoryContext::test_default();

        let create = json!({
            "description": "Foo".to_string(),
            "bounds": STRectangle::new(SpatialReference::epsg_4326(), 0., 0., 1., 1., 0, 1).unwrap(),
        });

        let req = test::TestRequest::post().uri("/project").set_json(&create);
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Header with authorization token not provided.",
        )
        .await;
    }

    async fn list_test_helper(method: Method) -> ServiceResponse {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session = ctx.session();
        let _ = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!(
                "/projects?{}",
                &serde_urlencoded::to_string([
                    (
                        "permissions",
                        serde_json::json! {["Read", "Write", "Owner"]}
                            .to_string()
                            .as_str()
                    ),
                    // omitted ("filter", "None"),
                    ("order", "NameDesc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        send_test_request(req, app_ctx).await
    }

    #[tokio::test]
    async fn list() {
        let res = list_test_helper(Method::GET).await;

        assert_eq!(res.status(), 200);

        let result: Vec<ProjectListing> = test::read_body_json(res).await;
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn list_invalid_method() {
        check_allowed_http_methods(list_test_helper, &[Method::GET]).await;
    }

    #[tokio::test]
    async fn list_missing_header() {
        let app_ctx = InMemoryContext::test_default();

        create_project_helper(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/projects?{}",
                &serde_urlencoded::to_string([
                    (
                        "permissions",
                        serde_json::json! {["Read", "Write", "Owner"]}
                            .to_string()
                            .as_str()
                    ),
                    // omitted ("filter", "None"),
                    ("order", "NameDesc"),
                    ("offset", "0"),
                    ("limit", "2"),
                ])
                .unwrap()
            ))
            .append_header((header::CONTENT_LENGTH, 0));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Header with authorization token not provided.",
        )
        .await;
    }

    async fn load_test_helper(method: Method) -> ServiceResponse {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        send_test_request(req, app_ctx).await
    }

    #[tokio::test]
    async fn load() {
        let res = load_test_helper(Method::GET).await;

        assert_eq!(res.status(), 200);

        let _project: Project = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn load_invalid_method() {
        check_allowed_http_methods(
            load_test_helper,
            &[Method::GET, Method::PATCH, Method::DELETE],
        )
        .await;
    }

    #[tokio::test]
    async fn load_missing_header() {
        let app_ctx = InMemoryContext::test_default();

        let project = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error: Header with authorization token not provided.",
        )
        .await;
    }

    #[tokio::test]
    async fn load_not_found() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session_id = ctx.session().id();

        let req = test::TestRequest::get()
            .uri("/project")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(res, 405, "MethodNotAllowed", "HTTP method not allowed.").await;
    }

    async fn update_test_helper(
        method: Method,
    ) -> (InMemoryContext, SimpleSession, ProjectId, ServiceResponse) {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let update = update_project_helper(project);

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(&update);
        let res = send_test_request(req, app_ctx.clone()).await;

        (app_ctx, session, project, res)
    }

    #[tokio::test]
    async fn update() {
        let (app_ctx, session, project, res) = update_test_helper(Method::PATCH).await;

        assert_eq!(res.status(), 200);

        let loaded = app_ctx
            .session_context(session)
            .db()
            .load_project(project)
            .await
            .unwrap();
        assert_eq!(loaded.name, "TestUpdate");
        assert_eq!(loaded.layers.len(), 1);
    }

    #[tokio::test]
    async fn update_invalid_body() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::patch()
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_payload("no json");
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        )
        .await;
    }

    #[tokio::test]
    async fn update_missing_fields() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let update = json!({
            "name": "TestUpdate",
            "description": None::<String>,
            "layers": vec![LayerUpdate::UpdateOrInsert(ProjectLayer {
                workflow: WorkflowId::new(),
                name: "L1".to_string(),
                visibility: Default::default(),
                symbology: Symbology::Raster(RasterSymbology {
                    opacity: 1.0,
                    colorizer: Colorizer::Rgba,
                })
            })],
            "bounds": None::<String>,
            "time_step": None::<String>,
        });

        let req = test::TestRequest::patch()
            .uri(&format!("/project/{project}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(&update);
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `id` at line 1 column 260",
        )
        .await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn update_layers() {
        async fn update_and_load_latest(
            app_ctx: &InMemoryContext,
            session: &SimpleSession,
            project_id: ProjectId,
            update: UpdateProject,
        ) -> Vec<ProjectLayer> {
            let req = test::TestRequest::patch()
                .uri(&format!("/project/{project_id}"))
                .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
                .set_json(&update);
            let res = send_test_request(req, app_ctx.clone()).await;

            assert_eq!(res.status(), 200);

            let loaded = app_ctx
                .session_context(session.clone())
                .db()
                .load_project(project_id)
                .await
                .unwrap();

            loaded.layers
        }

        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let layer_1 = ProjectLayer {
            workflow: WorkflowId::new(),
            name: "L1".to_string(),
            visibility: LayerVisibility {
                data: true,
                legend: false,
            },
            symbology: Symbology::Raster(RasterSymbology {
                opacity: 1.0,
                colorizer: Colorizer::Rgba,
            }),
        };

        let layer_2 = ProjectLayer {
            workflow: WorkflowId::new(),
            name: "L2".to_string(),
            visibility: LayerVisibility {
                data: false,
                legend: true,
            },
            symbology: Symbology::Raster(RasterSymbology {
                opacity: 1.0,
                colorizer: Colorizer::Rgba,
            }),
        };

        // add first layer
        assert_eq!(
            update_and_load_latest(
                &app_ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: Some(vec![LayerUpdate::UpdateOrInsert(layer_1.clone())]),
                    plots: None,
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![layer_1.clone()]
        );

        // add second layer
        assert_eq!(
            update_and_load_latest(
                &app_ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: Some(vec![
                        LayerUpdate::None(Default::default()),
                        LayerUpdate::UpdateOrInsert(layer_2.clone())
                    ]),
                    plots: None,
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![layer_1.clone(), layer_2.clone()]
        );

        // remove first layer
        assert_eq!(
            update_and_load_latest(
                &app_ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: Some(vec![
                        LayerUpdate::Delete(Default::default()),
                        LayerUpdate::None(Default::default()),
                    ]),
                    plots: None,
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![layer_2.clone()]
        );

        // clear layers
        assert_eq!(
            update_and_load_latest(
                &app_ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: Some(vec![]),
                    plots: None,
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![]
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn update_plots() {
        async fn update_and_load_latest(
            app_ctx: &InMemoryContext,
            session: &SimpleSession,
            project_id: ProjectId,
            update: UpdateProject,
        ) -> Vec<Plot> {
            let req = test::TestRequest::patch()
                .uri(&format!("/project/{project_id}"))
                .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
                .set_json(&update);
            let res = send_test_request(req, app_ctx.clone()).await;

            assert_eq!(res.status(), 200);

            let loaded = app_ctx
                .session_context(session.clone())
                .db()
                .load_project(project_id)
                .await
                .unwrap();

            loaded.plots
        }

        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let plot_1 = Plot {
            workflow: WorkflowId::new(),
            name: "P1".to_string(),
        };

        let plot_2 = Plot {
            workflow: WorkflowId::new(),
            name: "P2".to_string(),
        };

        // add first plot
        assert_eq!(
            update_and_load_latest(
                &app_ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: None,
                    plots: Some(vec![PlotUpdate::UpdateOrInsert(plot_1.clone())]),
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![plot_1.clone()]
        );

        // add second plot
        assert_eq!(
            update_and_load_latest(
                &app_ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: None,
                    plots: Some(vec![
                        PlotUpdate::None(Default::default()),
                        PlotUpdate::UpdateOrInsert(plot_2.clone())
                    ]),
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![plot_1.clone(), plot_2.clone()]
        );

        // remove first plot
        assert_eq!(
            update_and_load_latest(
                &app_ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: None,
                    plots: Some(vec![
                        PlotUpdate::Delete(Default::default()),
                        PlotUpdate::None(Default::default()),
                    ]),
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![plot_2.clone()]
        );

        // clear plots
        assert_eq!(
            update_and_load_latest(
                &app_ctx,
                &session,
                project,
                UpdateProject {
                    id: project,
                    name: None,
                    description: None,
                    layers: None,
                    plots: Some(vec![]),
                    bounds: None,
                    time_step: None,
                }
            )
            .await,
            vec![]
        );
    }

    #[tokio::test]
    async fn delete() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::delete()
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert!(ctx.db().load_project(project).await.is_err());

        let req = test::TestRequest::delete()
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "ProjectDeleteFailed",
            "Failed to delete the project.",
        )
        .await;
    }
}
