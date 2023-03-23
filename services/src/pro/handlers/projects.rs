use crate::contexts::ApplicationContext;
use crate::contexts::SessionContext;
use crate::error::Result;
use crate::handlers;
use crate::pro::contexts::ProApplicationContext;
use crate::pro::contexts::ProGeoEngineDb;
use crate::pro::projects::LoadVersion;
use crate::pro::projects::ProProjectDb;
use crate::projects::{ProjectId, ProjectVersionId};

use actix_web::FromRequest;
use actix_web::{web, Responder};

pub(crate) fn init_project_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProApplicationContext,
    C::Session: FromRequest,
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    cfg.service(
        web::resource("/projects")
            .route(web::get().to(handlers::projects::list_projects_handler::<C>)),
    )
    .service(
        web::scope("/project")
            .service(
                web::resource("")
                    .route(web::post().to(handlers::projects::create_project_handler::<C>)),
            )
            .service(
                web::resource("/{project}")
                    .route(web::get().to(load_project_latest_handler::<C>))
                    .route(web::patch().to(handlers::projects::update_project_handler::<C>))
                    .route(web::delete().to(handlers::projects::delete_project_handler::<C>)),
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
pub(crate) async fn load_project_version_handler<C: ProApplicationContext>(
    project: web::Path<(ProjectId, ProjectVersionId)>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let project = project.into_inner();
    let id = app_ctx
        .session_context(session)
        .db()
        .load_project_version(project.0, LoadVersion::Version(project.1))
        .await?;
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
pub(crate) async fn load_project_latest_handler<C: ProApplicationContext>(
    project: web::Path<ProjectId>,
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let id = app_ctx
        .session_context(session)
        .db()
        .load_project_version(project.into_inner(), LoadVersion::Latest)
        .await?;
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
pub(crate) async fn project_versions_handler<C: ProApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
    project: web::Path<ProjectId>,
) -> Result<impl Responder>
where
    <<C as ApplicationContext>::SessionContext as SessionContext>::GeoEngineDB: ProGeoEngineDb,
{
    let versions = app_ctx
        .session_context(session)
        .db()
        .list_project_versions(project.into_inner())
        .await?;
    Ok(web::Json(versions))
}

#[cfg(test)]
mod tests {
    use super::*;

    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::util::test::TestDefault;

    use crate::contexts::SessionContext;
    use crate::{
        handlers::ErrorResponse,
        pro::{
            contexts::ProInMemoryContext,
            util::tests::{create_project_helper, send_pro_test_request},
        },
        projects::{Project, ProjectDb, ProjectVersion},
        util::tests::{check_allowed_http_methods, update_project_helper},
    };

    #[tokio::test]
    async fn load_version() {
        let app_ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&app_ctx).await;

        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        db.update_project(update_project_helper(project))
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/project/{project}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_pro_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let body: Project = test::read_body_json(res).await;

        assert_eq!(body.name, "TestUpdate");

        let versions = db.list_project_versions(project).await.unwrap();
        let version_id = versions.first().unwrap().id;

        let req = test::TestRequest::get()
            .uri(&format!("/project/{project}/{version_id}"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_pro_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let body: Project = test::read_body_json(res).await;
        assert_eq!(body.name, "Test");
    }

    #[tokio::test]
    async fn load_version_not_found() {
        let app_ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/project/{project}/00000000-0000-0000-0000-000000000000"
            ))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_pro_test_request(req, app_ctx).await;

        ErrorResponse::assert(res, 400, "ProjectLoadFailed", "The project failed to load.").await;
    }

    async fn versions_test_helper(method: Method) -> ServiceResponse {
        let app_ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&app_ctx).await;

        let ctx = app_ctx.session_context(session.clone());

        let db = ctx.db();

        db.update_project(update_project_helper(project))
            .await
            .unwrap();

        let req = test::TestRequest::default()
            .method(method)
            .uri(&format!("/project/{project}/versions"))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        send_pro_test_request(req, app_ctx).await
    }

    #[tokio::test]
    async fn versions() {
        let res = versions_test_helper(Method::GET).await;

        assert_eq!(res.status(), 200);

        let _body: Vec<ProjectVersion> = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn versions_invalid_method() {
        check_allowed_http_methods(versions_test_helper, &[Method::GET]).await;
    }

    #[tokio::test]
    async fn versions_missing_header() {
        let app_ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&app_ctx).await;

        let ctx = app_ctx.session_context(session);

        let db = ctx.db();

        db.update_project(update_project_helper(project))
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/project/{project}/versions"))
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(project);
        let res = send_pro_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        )
        .await;
    }
}
