use crate::error::Result;
use crate::handlers;
use crate::pro::contexts::ProContext;
use crate::pro::projects::LoadVersion;
use crate::pro::projects::{ProProjectDb, UserProjectPermission};
use crate::projects::{ProjectId, ProjectVersionId};

use actix_web::{web, HttpResponse, Responder};

pub(crate) fn init_project_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
    C::ProjectDB: ProProjectDb,
{
    cfg.service(
        web::resource("/projects")
            .route(web::get().to(handlers::projects::list_projects_handler::<C>)),
    )
    .service(
        web::scope("/project")
            .service(web::resource("/versions").route(web::get().to(project_versions_handler::<C>)))
            .service(
                web::resource("/permission/add").route(web::post().to(add_permission_handler::<C>)),
            )
            .service(
                web::resource("/permission")
                    .route(web::delete().to(remove_permission_handler::<C>)),
            )
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
                web::resource("/{project}/permissions")
                    .route(web::get().to(list_permissions_handler::<C>)),
            )
            .service(
                web::resource("/{project}/{version}")
                    .route(web::get().to(load_project_version_handler::<C>)),
            ),
    );
}

/// Retrieves details about a [project](crate::projects::project::Project).
/// If no version is specified, it loads the latest version.
///
/// # Example
///
/// ```text
/// GET /project/df4ad02e-0d61-4e29-90eb-dc1259c1f5b9/[version]
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
/// Response:
/// ```text
/// {
///   "id": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
///   "version": {
///     "id": "8f4b8683-f92c-4129-a16f-818aeeee484e",
///     "changed": "2021-04-26T14:05:39.677390600Z",
///     "author": "5b4466d2-8bab-4ed8-a182-722af3c80958"
///   },
///   "name": "Test",
///   "description": "Foo",
///   "layers": [],
///   "plots": [],
///   "bounds": {
///     "spatialReference": "EPSG:4326",
///     "boundingBox": {
///       "lowerLeftCoordinate": {
///         "x": 0.0,
///         "y": 0.0
///       },
///       "upperRightCoordinate": {
///         "x": 1.0,
///         "y": 1.0
///       }
///     },
///     "timeInterval": {
///       "start": 0,
///       "end": 1
///     }
///   },
///   "timeStep": {
///     "granularity": "Months",
///     "step": 1
///   }
/// }
/// ```
pub(crate) async fn load_project_version_handler<C: ProContext>(
    project: web::Path<(ProjectId, ProjectVersionId)>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder>
where
    C::ProjectDB: ProProjectDb,
{
    let project = project.into_inner();
    let id = ctx
        .project_db_ref()
        .await
        .load_version(&session, project.0, LoadVersion::Version(project.1))
        .await?;
    Ok(web::Json(id))
}

pub(crate) async fn load_project_latest_handler<C: ProContext>(
    project: web::Path<ProjectId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder>
where
    C::ProjectDB: ProProjectDb,
{
    let id = ctx
        .project_db_ref()
        .await
        .load_version(&session, project.into_inner(), LoadVersion::Latest)
        .await?;
    Ok(web::Json(id))
}

/// Lists all [versions](crate::projects::project::ProjectVersion) of a project.
///
/// # Example
///
/// ```text
/// GET /project/versions
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9"
/// ```
/// Response:
/// ```text
/// [
///   {
///     "id": "8f4b8683-f92c-4129-a16f-818aeeee484e",
///     "changed": "2021-04-26T14:05:39.677390600Z",
///     "author": "5b4466d2-8bab-4ed8-a182-722af3c80958"
///   },
///   {
///     "id": "ced041c7-4b1d-4d13-b076-94596be6a36a",
///     "changed": "2021-04-26T14:13:10.901912700Z",
///     "author": "5b4466d2-8bab-4ed8-a182-722af3c80958"
///   }
/// ]
/// ```
pub(crate) async fn project_versions_handler<C: ProContext>(
    session: C::Session,
    ctx: web::Data<C>,
    project: web::Json<ProjectId>,
) -> Result<impl Responder>
where
    C::ProjectDB: ProProjectDb,
{
    let versions = ctx
        .project_db_ref_mut()
        .await
        .versions(&session, project.into_inner())
        .await?;
    Ok(web::Json(versions))
}

/// Add a [permission](crate::projects::project::ProjectPermission) for another user
/// if the session user is the owner of the target project.
///
/// # Example
///
/// ```text
/// POST /project/permission/add
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// {
///   "user": "3cbe632e-c50a-46d0-8490-f12621347bb1",
///   "project": "aaed86a1-49d4-482d-b993-39159bb853df",
///   "permission": "Read"
/// }
/// ```
pub(crate) async fn add_permission_handler<C: ProContext>(
    session: C::Session,
    ctx: web::Data<C>,
    permission: web::Json<UserProjectPermission>,
) -> Result<impl Responder>
where
    C::ProjectDB: ProProjectDb,
{
    ctx.project_db_ref_mut()
        .await
        .add_permission(&session, permission.into_inner())
        .await?;
    Ok(HttpResponse::Ok())
}

/// Removes a [permission](crate::projects::project::ProjectPermission) of another user
/// if the session user is the owner of the target project.
///
/// # Example
///
/// ```text
/// DELETE /project/permission
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// {
///   "user": "3cbe632e-c50a-46d0-8490-f12621347bb1",
///   "project": "aaed86a1-49d4-482d-b993-39159bb853df",
///   "permission": "Read"
/// }
/// ```
pub(crate) async fn remove_permission_handler<C: ProContext>(
    session: C::Session,
    ctx: web::Data<C>,
    permission: web::Json<UserProjectPermission>,
) -> Result<impl Responder>
where
    C::ProjectDB: ProProjectDb,
{
    ctx.project_db_ref_mut()
        .await
        .remove_permission(&session, permission.into_inner())
        .await?;
    Ok(HttpResponse::Ok())
}

/// Shows the access rights the user has for a given project.
///
/// # Example
///
/// ```text
/// GET /project/df4ad02e-0d61-4e29-90eb-dc1259c1f5b9/permissions
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
/// Response:
/// ```text
/// [
///   {
///     "user": "5b4466d2-8bab-4ed8-a182-722af3c80958",
///     "project": "df4ad02e-0d61-4e29-90eb-dc1259c1f5b9",
///     "permission": "Owner"
///   }
/// ]
/// ```
pub(crate) async fn list_permissions_handler<C: ProContext>(
    project: web::Path<ProjectId>,
    session: C::Session,
    ctx: web::Data<C>,
) -> Result<impl Responder>
where
    C::ProjectDB: ProProjectDb,
{
    let permissions = ctx
        .project_db_ref_mut()
        .await
        .list_permissions(&session, project.into_inner())
        .await?;
    Ok(web::Json(permissions))
}

#[cfg(test)]
mod tests {
    use super::*;

    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::util::test::TestDefault;

    use crate::{
        contexts::Context,
        handlers::ErrorResponse,
        pro::{
            contexts::ProInMemoryContext,
            projects::ProjectPermission,
            users::{UserCredentials, UserDb, UserRegistration},
            util::tests::{create_project_helper, send_pro_test_request},
        },
        projects::{Project, ProjectDb, ProjectVersion},
        util::{
            tests::{check_allowed_http_methods, update_project_helper},
            user_input::UserInput,
        },
    };

    #[tokio::test]
    async fn load_version() {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        ctx.project_db()
            .write()
            .await
            .update(
                &session,
                update_project_helper(project).validated().unwrap(),
            )
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/project/{}", project))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let body: Project = test::read_body_json(res).await;

        assert_eq!(body.name, "TestUpdate");

        let versions = ctx
            .project_db()
            .read()
            .await
            .versions(&session, project)
            .await
            .unwrap();
        let version_id = versions.first().unwrap().id;

        let req = test::TestRequest::get()
            .uri(&format!("/project/{}/{}", project, version_id))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_pro_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        let body: Project = test::read_body_json(res).await;
        assert_eq!(body.name, "Test");
    }

    #[tokio::test]
    async fn load_version_not_found() {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/project/{}/00000000-0000-0000-0000-000000000000",
                project
            ))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(res, 400, "ProjectLoadFailed", "The project failed to load.").await;
    }

    #[tokio::test]
    async fn add_permission() {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        let req = test::TestRequest::post()
            .uri("/project/permission/add")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())))
            .set_json(&permission);
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert!(ctx
            .project_db()
            .write()
            .await
            .load(&session, project)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn add_permission_missing_header() {
        let ctx = ProInMemoryContext::test_default();

        let (_, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        let req = test::TestRequest::post()
            .uri("/project/permission/add")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&permission);
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        )
        .await;
    }

    #[tokio::test]
    async fn remove_permission() {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(&session, permission.clone())
            .await
            .unwrap();

        let req = test::TestRequest::delete()
            .uri("/project/permission")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())))
            .set_json(&permission);
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let target_user_session = ctx
            .user_db_ref_mut()
            .await
            .login(UserCredentials {
                email: "foo2@bar.de".to_string(),
                password: "secret1234".to_string(),
            })
            .await
            .unwrap();

        assert!(ctx
            .project_db()
            .write()
            .await
            .load(&target_user_session, project)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn remove_permission_missing_header() {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(&session, permission.clone())
            .await
            .unwrap();

        let req = test::TestRequest::delete()
            .uri("/project/permission")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&permission);
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        )
        .await;
    }

    #[tokio::test]
    async fn list_permissions() {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(&session, permission.clone())
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/project/{}/permissions", project))
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())))
            .set_json(&permission);
        let res = send_pro_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        let result: Vec<UserProjectPermission> = test::read_body_json(res).await;
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn list_permissions_missing_header() {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        let target_user = ctx
            .user_db()
            .write()
            .await
            .register(
                UserRegistration {
                    email: "foo2@bar.de".to_string(),
                    password: "secret1234".to_string(),
                    real_name: "Foo2 Bar".to_string(),
                }
                .validated()
                .unwrap(),
            )
            .await
            .unwrap();

        let permission = UserProjectPermission {
            user: target_user,
            project,
            permission: ProjectPermission::Read,
        };

        ctx.project_db()
            .write()
            .await
            .add_permission(&session, permission.clone())
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri(&format!("/project/{}/permissions", project))
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&permission);
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        )
        .await;
    }

    async fn versions_test_helper(method: Method) -> ServiceResponse {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        ctx.project_db()
            .write()
            .await
            .update(
                &session,
                update_project_helper(project).validated().unwrap(),
            )
            .await
            .unwrap();

        let req = test::TestRequest::default()
            .method(method)
            .uri("/project/versions")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())))
            .set_json(&project);
        send_pro_test_request(req, ctx).await
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
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        ctx.project_db()
            .write()
            .await
            .update(
                &session,
                update_project_helper(project).validated().unwrap(),
            )
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri("/project/versions")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&project);
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        )
        .await;
    }
}
