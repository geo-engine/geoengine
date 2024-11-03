use crate::api::model::responses::{ErrorResponse, IdResponse};
use crate::contexts::{ApplicationContext, SessionContext};
use crate::error::Result;
use crate::projects::error::ProjectDbError;
use crate::projects::{
    CreateProject, LoadVersion, ProjectDb, ProjectId, ProjectListOptions, ProjectVersionId,
    UpdateProject,
};
use crate::util::extractors::{ValidatedJson, ValidatedQuery};
use actix_web::{web, FromRequest, HttpResponse, Responder, ResponseError};
use geoengine_datatypes::util::helpers::ge_report;
use snafu::prelude::*;
use snafu::ResultExt;
use std::fmt;
use strum::IntoStaticStr;

pub(crate) fn init_project_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/projects").route(web::get().to(list_projects_handler::<C>)))
        .service(
            web::scope("/project")
                .service(web::resource("").route(web::post().to(create_project_handler::<C>)))
                .service(
                    web::resource("/{project}")
                        .route(web::get().to(load_project_latest_handler::<C>))
                        .route(web::patch().to(update_project_handler::<C>))
                        .route(web::delete().to(delete_project_handler::<C>)),
                )
                .service(
                    web::resource("/{project}/versions")
                        .route(web::get().to(project_versions_handler::<C>)),
                )
                .service(
                    web::resource("/{project}/{version}")
                        .route(web::get().to(load_project_version_handler::<C>)),
                ),
        );
}

#[derive(Snafu, IntoStaticStr)]
#[snafu(visibility(pub(crate)), context(suffix(false)), module(error))]
pub enum ProjectHandlerError {
    #[snafu(display("Could not create project: {source}"))]
    CreateProject { source: ProjectDbError },
    #[snafu(display("Could not list projects: {source}"))]
    ListProjects { source: ProjectDbError },
    #[snafu(display("Could not update project: {source}"))]
    UpdateProject { source: ProjectDbError },
    #[snafu(display("Could not delete project: {source}"))]
    DeleteProject { source: ProjectDbError },
    #[snafu(display("Could not load project version: {source}"))]
    LoadProjectVersion { source: ProjectDbError },
    #[snafu(display("Could not load latest project version: {source}"))]
    LoadLatestProjectVersion { source: ProjectDbError },
    #[snafu(display("Could not list project versions: {source}"))]
    ListProjectVersions { source: ProjectDbError },
}

impl ProjectHandlerError {
    pub fn source(&self) -> &ProjectDbError {
        match self {
            ProjectHandlerError::CreateProject { source }
            | ProjectHandlerError::ListProjects { source }
            | ProjectHandlerError::UpdateProject { source }
            | ProjectHandlerError::DeleteProject { source }
            | ProjectHandlerError::LoadProjectVersion { source }
            | ProjectHandlerError::LoadLatestProjectVersion { source }
            | ProjectHandlerError::ListProjectVersions { source } => source,
        }
    }
}

impl ResponseError for ProjectHandlerError {
    fn status_code(&self) -> actix_http::StatusCode {
        match self.source() {
            ProjectDbError::Postgres { .. } | ProjectDbError::Bb8 { .. } => {
                actix_http::StatusCode::INTERNAL_SERVER_ERROR
            }
            _ => actix_http::StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse<actix_http::body::BoxBody> {
        HttpResponse::build(self.status_code()).json(ErrorResponse::from(self))
    }
}

impl fmt::Debug for ProjectHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ge_report(self))
    }
}

/// Create a new project for the user.
#[utoipa::path(
    tag = "Projects",
    post,
    path = "/project",
    request_body = CreateProject,
    responses(
        (status = 200, response = IdResponse::<ProjectId>)
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn create_project_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    create: ValidatedJson<CreateProject>,
) -> Result<web::Json<IdResponse<ProjectId>>, ProjectHandlerError> {
    let create = create.into_inner();
    let id = app_ctx
        .session_context(session)
        .db()
        .create_project(create)
        .await
        .context(error::CreateProject)?;
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
) -> Result<impl Responder, ProjectHandlerError> {
    let options = options.into_inner();
    let listing = app_ctx
        .session_context(session)
        .db()
        .list_projects(options)
        .await
        .context(error::ListProjects)?;
    Ok(web::Json(listing))
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
) -> Result<impl Responder, ProjectHandlerError> {
    let mut update = update.into_inner();
    update.id = project.into_inner(); // TODO: avoid passing project id in path AND body
    app_ctx
        .session_context(session)
        .db()
        .update_project(update)
        .await
        .context(error::UpdateProject)?;
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
) -> Result<impl Responder, ProjectHandlerError> {
    app_ctx
        .session_context(session)
        .db()
        .delete_project(*project)
        .await
        .context(error::DeleteProject)?;
    Ok(HttpResponse::Ok())
}

/// Retrieves details about the given version of a project.
#[utoipa::path(
    tag = "Projects",
    get,
    path = "/project/{project}/{version}",
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
        ("project" = ProjectId, description = "Project id"),
        ("version" = ProjectVersionId, description = "Version id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn load_project_version_handler<C: ApplicationContext>(
    project: web::Path<(ProjectId, ProjectVersionId)>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder, ProjectHandlerError> {
    let project = project.into_inner();
    let id = app_ctx
        .session_context(session)
        .db()
        .load_project_version(project.0, LoadVersion::Version(project.1))
        .await
        .context(error::LoadProjectVersion)?;
    Ok(web::Json(id))
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
pub(crate) async fn load_project_latest_handler<C: ApplicationContext>(
    project: web::Path<ProjectId>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder, ProjectHandlerError> {
    let id = app_ctx
        .session_context(session)
        .db()
        .load_project_version(project.into_inner(), LoadVersion::Latest)
        .await
        .context(error::LoadLatestProjectVersion)?;
    Ok(web::Json(id))
}

/// Lists all available versions of a project.
#[utoipa::path(
    tag = "Projects",
    get,
    path = "/project/{project}/versions",
    responses(
        (status = 200, description = "OK", body = [ProjectVersion],
            example = json!([
                {
                    "id": "8f4b8683-f92c-4129-a16f-818aeeee484e",
                    "changed": "2021-04-26T14:05:39.677390600Z",
                    "author": "5b4466d2-8bab-4ed8-a182-722af3c80958"
                },
                {
                    "id": "ced041c7-4b1d-4d13-b076-94596be6a36a",
                    "changed": "2021-04-26T14:13:10.901912700Z",
                    "author": "5b4466d2-8bab-4ed8-a182-722af3c80958"
                }
            ])
        )
    ),
    params(
        ("project" = ProjectId, description = "Project id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn project_versions_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    project: web::Path<ProjectId>,
) -> Result<impl Responder, ProjectHandlerError> {
    let versions = app_ctx
        .session_context(session)
        .db()
        .list_project_versions(project.into_inner())
        .await
        .context(error::ListProjectVersions)?;
    Ok(web::Json(versions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::{PostgresContext, Session, SimpleApplicationContext, SimpleSession};
    use crate::ge_context;
    use crate::projects::{
        LayerUpdate, LayerVisibility, Plot, PlotUpdate, Project, ProjectId, ProjectLayer,
        ProjectListing, RasterSymbology, STRectangle, Symbology, UpdateProject,
    };
    use crate::util::tests::{
        check_allowed_http_methods, create_project_helper, send_test_request, update_project_helper,
    };
    use crate::util::Identifier;
    use crate::workflows::workflow::WorkflowId;
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::operations::image::{Colorizer, RasterColorizer};
    use geoengine_datatypes::primitives::{TimeGranularity, TimeStep};
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use serde_json::json;
    use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
    use tokio_postgres::{NoTls, Socket};

    async fn create_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let ctx = app_ctx.default_session_context().await.unwrap();

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

    #[ge_context::test]
    async fn create(app_ctx: PostgresContext<NoTls>) {
        let res = create_test_helper(app_ctx, Method::POST).await;

        assert_eq!(res.status(), 200);

        let _project: IdResponse<ProjectId> = test::read_body_json(res).await;
    }

    #[ge_context::test]
    async fn create_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| create_test_helper(app_ctx.clone(), method),
            &[Method::POST],
        )
        .await;
    }

    #[ge_context::test]
    async fn create_invalid_body(app_ctx: PostgresContext<NoTls>) {
        let ctx = app_ctx.default_session_context().await.unwrap();

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

    #[ge_context::test]
    async fn create_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let ctx = app_ctx.default_session_context().await.unwrap();

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
            "Error in user input: missing field `name` at line 1 column 195",
        )
        .await;
    }

    #[ge_context::test]
    async fn create_missing_header(app_ctx: PostgresContext<NoTls>) {
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

    async fn list_test_helper(app_ctx: PostgresContext<NoTls>, method: Method) -> ServiceResponse {
        let ctx = app_ctx.default_session_context().await.unwrap();

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

    #[ge_context::test]
    async fn list(app_ctx: PostgresContext<NoTls>) {
        let res = list_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);

        let result: Vec<ProjectListing> = test::read_body_json(res).await;
        assert_eq!(result.len(), 1);
    }

    #[ge_context::test]
    async fn list_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| list_test_helper(app_ctx.clone(), method),
            &[Method::GET],
        )
        .await;
    }

    #[ge_context::test]
    async fn list_missing_header(app_ctx: PostgresContext<NoTls>) {
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

    async fn load_test_helper(app_ctx: PostgresContext<NoTls>, method: Method) -> ServiceResponse {
        let ctx = app_ctx.default_session_context().await.unwrap();

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn load(app_ctx: PostgresContext<NoTls>) {
        let res = load_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);

        let _project: Project = test::read_body_json(res).await;
    }

    #[ge_context::test]
    async fn load_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| load_test_helper(app_ctx.clone(), method),
            &[Method::GET, Method::PATCH, Method::DELETE],
        )
        .await;
    }

    #[ge_context::test]
    async fn load_missing_header(app_ctx: PostgresContext<NoTls>) {
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

    #[ge_context::test]
    async fn load_not_found(app_ctx: PostgresContext<NoTls>) {
        let ctx = app_ctx.default_session_context().await.unwrap();

        let session_id = ctx.session().id();

        let req = test::TestRequest::get()
            .uri("/project")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(res, 405, "MethodNotAllowed", "HTTP method not allowed.").await;
    }

    async fn update_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> (SimpleSession, ProjectId, ServiceResponse) {
        let ctx = app_ctx.default_session_context().await.unwrap();

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

        (session, project, res)
    }

    #[ge_context::test]
    async fn update(app_ctx: PostgresContext<NoTls>) {
        let (session, project, res) = update_test_helper(app_ctx.clone(), Method::PATCH).await;

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

    #[ge_context::test]
    async fn update_invalid_body(app_ctx: PostgresContext<NoTls>) {
        let ctx = app_ctx.default_session_context().await.unwrap();

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
            "Error in user input: expected ident at line 1 column 2",
        )
        .await;
    }

    #[ge_context::test]
    async fn update_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let ctx = app_ctx.default_session_context().await.unwrap();

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
                    raster_colorizer: RasterColorizer::SingleBand {
                        band: 0,
                        band_colorizer: Colorizer::test_default(),
                    }
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
            "Error in user input: missing field `id` at line 1 column 492",
        )
        .await;
    }

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn update_layers(app_ctx: PostgresContext<NoTls>) {
        async fn update_and_load_latest<Tls>(
            app_ctx: &PostgresContext<Tls>,
            session: &SimpleSession,
            project_id: ProjectId,
            update: UpdateProject,
        ) -> Vec<ProjectLayer>
        where
            Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + std::fmt::Debug + 'static,
            <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
            <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
            <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
        {
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

        let ctx = app_ctx.default_session_context().await.unwrap();

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
                raster_colorizer: RasterColorizer::SingleBand {
                    band: 0,
                    band_colorizer: Colorizer::test_default(),
                },
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
                raster_colorizer: RasterColorizer::SingleBand {
                    band: 0,
                    band_colorizer: Colorizer::test_default(),
                },
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

    #[ge_context::test]
    #[allow(clippy::too_many_lines)]
    async fn update_plots(app_ctx: PostgresContext<NoTls>) {
        async fn update_and_load_latest<Tls>(
            app_ctx: &PostgresContext<Tls>,
            session: &SimpleSession,
            project_id: ProjectId,
            update: UpdateProject,
        ) -> Vec<Plot>
        where
            Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + std::fmt::Debug + 'static,
            <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
            <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
            <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
        {
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

        let ctx = app_ctx.default_session_context().await.unwrap();

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

    #[ge_context::test]
    async fn delete(app_ctx: PostgresContext<NoTls>) {
        let ctx = app_ctx.default_session_context().await.unwrap();

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
            "DeleteProject",
            &format!("Could not delete project: Project {project} does not exist"),
        )
        .await;
    }

    #[ge_context::test]
    async fn load_version(app_ctx: PostgresContext<NoTls>) {
        let ctx = app_ctx.default_session_context().await.unwrap();

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let db = ctx.db();

        db.update_project(update_project_helper(project))
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let body: Project = test::read_body_json(res).await;

        assert_eq!(body.name, "TestUpdate");

        let versions = db.list_project_versions(project).await.unwrap();
        let version_id = versions.first().unwrap().id;

        let req = test::TestRequest::get()
            .uri(&format!("/project/{project}/{version_id}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let body: Project = test::read_body_json(res).await;
        assert_eq!(body.name, "TestUpdate");
    }

    #[ge_context::test]
    async fn load_version_not_found(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.default_session().await.unwrap();
        let project = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/project/{project}/00000000-0000-0000-0000-000000000000"
            ))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 400);
    }

    #[ge_context::test]
    async fn list_versions(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.default_session().await.unwrap();

        let project = create_project_helper(&app_ctx).await;

        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        db.update_project(update_project_helper(project))
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/project/{project}/versions"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        send_test_request(req, app_ctx).await;
    }
}
