use crate::error;
use crate::error::Result;
use crate::pro::contexts::ProContext;
use crate::pro::users::UserCredentials;
use crate::pro::users::UserDb;
use crate::pro::users::UserRegistration;
use crate::pro::users::UserSession;
use crate::projects::ProjectId;
use crate::projects::STRectangle;
use crate::util::user_input::UserInput;
use crate::util::IdResponse;

use actix_web::{web, HttpResponse, Responder};
use snafu::ResultExt;

pub(crate) fn init_user_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
{
    cfg.route("/user", web::post().to(register_user_handler::<C>))
        .route("/anonymous", web::post().to(anonymous_handler::<C>))
        .route("/login", web::post().to(login_handler::<C>))
        .route("/logout", web::post().to(logout_handler::<C>))
        .route(
            "/session/project/{project}",
            web::post().to(session_project_handler::<C>),
        )
        .route("/session/view", web::post().to(session_view_handler::<C>));
}

/// Registers a user by providing [`UserRegistration`] parameters.
///
/// # Example
///
/// ```text
/// POST /user
///
/// {
///   "email": "foo@bar.de",
///   "password": "secret123",
///   "realName": "Foo Bar"
/// }
/// ```
/// Response:
/// ```text
/// {
///   "id": "5b4466d2-8bab-4ed8-a182-722af3c80958"
/// }
/// ```
///
/// # Errors
///
/// This call fails if the [`UserRegistration`] is invalid
/// or an account with the given e-mail already exists.
pub(crate) async fn register_user_handler<C: ProContext>(
    user: web::Json<UserRegistration>,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let user = user.into_inner().validated()?;
    let id = ctx.user_db_ref_mut().await.register(user).await?;
    Ok(web::Json(IdResponse::from(id)))
}

/// Creates a session by providing [`UserCredentials`].
///
/// # Example
///
/// ```text
/// POST /login
///
/// {
///   "email": "foo@bar.de",
///   "password": "secret123"
/// }
/// ```
/// Response:
/// ```text
/// {
///   "id": "208fa24e-7a92-4f57-a3fe-d1177d9f18ad",
///   "user": {
///     "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
///     "email": "foo@bar.de",
///     "realName": "Foo Bar"
///   },
///   "created": "2021-04-26T13:47:10.579724800Z",
///   "validUntil": "2021-04-26T14:47:10.579775400Z",
///   "project": null,
///   "view": null
/// }
/// ```
///
/// # Errors
///
/// This call fails if the [`UserCredentials`] are invalid.
pub(crate) async fn login_handler<C: ProContext>(
    user: web::Json<UserCredentials>,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let session = ctx
        .user_db_ref_mut()
        .await
        .login(user.into_inner())
        .await
        .map_err(Box::new)
        .context(error::Authorization)?;
    Ok(web::Json(session))
}

/// Ends a session.
///
/// # Example
///
/// ```text
/// POST /logout
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
///
/// # Errors
///
/// This call fails if the session is invalid.
pub(crate) async fn logout_handler<C: ProContext>(
    session: UserSession,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    ctx.user_db_ref_mut().await.logout(session.id).await?;
    Ok(HttpResponse::Ok())
}

/// Creates session for anonymous user.
///
/// # Example
///
/// ```text
/// POST /anonymous
/// ```
/// Response:
/// ```text
/// {
///   "id": "2fee8652-3192-4d3e-8adc-14257064224a",
///   "user": {
///     "id": "744b83ff-2c5b-401a-b4bf-2ba7213ad5d5",
///     "email": null,
///     "realName": null
///   },
///   "created": "2021-04-18T16:54:55.728758Z",
///   "validUntil": "2021-04-18T17:54:55.730196200Z",
///   "project": null,
///   "view": null
/// }
/// ```
pub(crate) async fn anonymous_handler<C: ProContext>(ctx: web::Data<C>) -> Result<impl Responder> {
    let session = ctx.user_db_ref_mut().await.anonymous().await?;
    Ok(web::Json(session))
}

/// Sets the active project of the session.
///
/// # Example
///
/// ```text
/// POST /session/project/c8d88d83-d409-46f7-bab2-815bba87ccd8
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
///
/// # Errors
///
/// This call fails if the session is invalid.
pub(crate) async fn session_project_handler<C: ProContext>(
    project: web::Path<ProjectId>,
    session: UserSession,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    ctx.user_db_ref_mut()
        .await
        .set_session_project(&session, project.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

// TODO: /view instead of /session/view
/// Sets the active view of the session.
///
/// # Example
///
/// ```text
/// POST /session/view
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
///
/// {
///   "spatialReference": "",
///   "boundingBox": {
///     "lowerLeftCoordinate": { "x": 0, "y": 0 },
///     "upperRightCoordinate": { "x": 1, "y": 1 }
///   },
///   "timeInterval": {
///     "start": 0,
///     "end": 1
///   }
/// }
/// ```
///
/// # Errors
///
/// This call fails if the session is invalid.
pub(crate) async fn session_view_handler<C: ProContext>(
    session: C::Session,
    ctx: web::Data<C>,
    view: web::Json<STRectangle>,
) -> Result<impl Responder> {
    ctx.user_db_ref_mut()
        .await
        .set_session_view(&session, view.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::contexts::Session;
    use crate::handlers::ErrorResponse;
    use crate::pro::util::tests::{create_project_helper, create_session_helper};
    use crate::pro::{contexts::ProInMemoryContext, projects::ProProjectDb, users::UserId};
    use crate::util::tests::{check_allowed_http_methods, read_body_string, send_pro_test_request};
    use crate::util::user_input::Validated;

    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use serde_json::json;

    async fn register_test_helper<C: ProContext>(
        ctx: C,
        method: Method,
        email: &str,
    ) -> ServiceResponse
    where
        C::ProjectDB: ProProjectDb,
    {
        let user = UserRegistration {
            email: email.to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        let req = test::TestRequest::default()
            .method(method)
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&user);
        send_pro_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn register() {
        let ctx = ProInMemoryContext::default();

        let res = register_test_helper(ctx, Method::POST, "foo@bar.de").await;

        assert_eq!(res.status(), 200);

        let _user: IdResponse<UserId> = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn register_fail() {
        let ctx = ProInMemoryContext::default();

        let res = register_test_helper(ctx, Method::POST, "notanemail").await;

        ErrorResponse::assert(
            res,
            400,
            "RegistrationFailed",
            "Registration failed: Invalid e-mail address",
        )
        .await;
    }

    #[tokio::test]
    async fn register_duplicate_email() {
        let ctx = ProInMemoryContext::default();

        register_test_helper(ctx.clone(), Method::POST, "foo@bar.de").await;

        // register user
        let res = register_test_helper(ctx, Method::POST, "foo@bar.de").await;

        ErrorResponse::assert(
            res,
            409,
            "Duplicate",
            "Tried to create duplicate: E-mail already exists",
        )
        .await;
    }

    #[tokio::test]
    async fn register_invalid_method() {
        let ctx = ProInMemoryContext::default();

        check_allowed_http_methods(
            |method| register_test_helper(ctx.clone(), method, "foo@bar.de"),
            &[Method::POST],
        )
        .await;
    }

    #[tokio::test]
    async fn register_invalid_body() {
        let ctx = ProInMemoryContext::default();

        // register user
        let req = test::TestRequest::post()
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_payload("no json");
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        )
        .await;
    }

    #[tokio::test]
    async fn register_missing_fields() {
        let ctx = ProInMemoryContext::default();

        let user = json!({
            "password": "secret123",
            "real_name": " Foo Bar",
        });

        // register user
        let req = test::TestRequest::post()
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&user);
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `email` at line 1 column 47",
        )
        .await;
    }

    #[tokio::test]
    async fn register_invalid_type() {
        let ctx = ProInMemoryContext::default();

        // register user
        let req = test::TestRequest::post()
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::CONTENT_TYPE, "text/html"));
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        )
        .await;
    }

    async fn login_test_helper(method: Method, password: &str) -> ServiceResponse {
        let ctx = ProInMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: password.to_string(),
        };

        let req = test::TestRequest::default()
            .method(method)
            .uri("/login")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&credentials);
        send_pro_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn login() {
        let res = login_test_helper(Method::POST, "secret123").await;

        assert_eq!(res.status(), 200);

        let _id: UserSession = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn login_fail() {
        let res = login_test_helper(Method::POST, "wrong").await;

        ErrorResponse::assert(
            res,
            401,
            "LoginFailed",
            "User does not exist or password is wrong.",
        )
        .await;
    }

    #[tokio::test]
    async fn login_invalid_method() {
        check_allowed_http_methods(
            |method| login_test_helper(method, "secret123"),
            &[Method::POST],
        )
        .await;
    }

    #[tokio::test]
    async fn login_invalid_body() {
        let ctx = ProInMemoryContext::default();

        let req = test::TestRequest::post()
            .uri("/login")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_payload("no json");
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        )
        .await;
    }

    #[tokio::test]
    async fn login_missing_fields() {
        let ctx = ProInMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = json!({
            "email": "foo@bar.de",
        });

        let req = test::TestRequest::post()
            .uri("/login")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&credentials);
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `password` at line 1 column 22",
        )
        .await;
    }

    async fn logout_test_helper(method: Method) -> ServiceResponse {
        let ctx = ProInMemoryContext::default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@bar.de".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db().write().await.register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@bar.de".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx
            .user_db()
            .write()
            .await
            .login(credentials)
            .await
            .unwrap();

        let req = test::TestRequest::default()
            .method(method)
            .uri("/logout")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_payload("no json");
        send_pro_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn logout() {
        let res = logout_test_helper(Method::POST).await;

        assert_eq!(res.status(), 200);
        assert_eq!(read_body_string(res).await, "");
    }

    #[tokio::test]
    async fn logout_missing_header() {
        let ctx = ProInMemoryContext::default();

        let req = test::TestRequest::post().uri("/logout");
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
    async fn logout_wrong_token() {
        let ctx = ProInMemoryContext::default();

        let req = test::TestRequest::post().uri("/logout").append_header((
            header::AUTHORIZATION,
            Bearer::new("6ecff667-258e-4108-9dc9-93cb8c64793c"),
        ));
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(res, 401, "InvalidSession", "The session id is invalid.").await;
    }

    #[tokio::test]
    async fn logout_wrong_scheme() {
        let ctx = ProInMemoryContext::default();

        let req = test::TestRequest::post()
            .uri("/logout")
            .append_header((header::AUTHORIZATION, "7e855f3c-b0cd-46d1-b5b3-19e6e3f9ea5"));
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "InvalidAuthorizationScheme",
            "Authentication scheme must be Bearer.",
        )
        .await;
    }

    #[tokio::test]
    async fn logout_invalid_token() {
        let ctx = ProInMemoryContext::default();

        let req = test::TestRequest::post()
            .uri("/logout")
            .append_header((header::AUTHORIZATION, format!("Bearer {}", "no uuid")));
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            401,
            "InvalidUuid",
            "Identifier does not have the right format.",
        )
        .await;
    }

    #[tokio::test]
    async fn logout_invalid_method() {
        check_allowed_http_methods(logout_test_helper, &[Method::POST]).await;
    }

    #[tokio::test]
    async fn session() {
        let ctx = ProInMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let req = test::TestRequest::get()
            .uri("/session")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_pro_test_request(req, ctx.clone()).await;

        let session: UserSession = test::read_body_json(res).await;

        ctx.user_db()
            .write()
            .await
            .logout(session.id)
            .await
            .unwrap();

        let req = test::TestRequest::get()
            .uri("/session")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(res, 401, "InvalidSession", "The session id is invalid.").await;
    }

    #[tokio::test]
    async fn session_view_project() {
        let ctx = ProInMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let req = test::TestRequest::post()
            .uri(&format!("/session/project/{}", project.to_string()))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            ctx.user_db()
                .read()
                .await
                .session(session.id)
                .await
                .unwrap()
                .project,
            Some(project)
        );

        let rect =
            STRectangle::new_unchecked(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1);
        let req = test::TestRequest::post()
            .append_header((header::CONTENT_LENGTH, 0))
            .uri("/session/view")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(&rect);
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            ctx.user_db()
                .read()
                .await
                .session(session.id())
                .await
                .unwrap()
                .view,
            Some(rect)
        );
    }
}
