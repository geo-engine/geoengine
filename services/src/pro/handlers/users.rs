use crate::contexts::AdminSession;
use crate::error;
use crate::error::Result;
use crate::pro::contexts::ProContext;
use crate::pro::users::UserDb;
use crate::pro::users::UserId;
use crate::pro::users::UserRegistration;
use crate::pro::users::UserSession;
use crate::pro::users::{AuthCodeResponse, UserCredentials};
use crate::projects::ProjectId;
use crate::projects::STRectangle;
use crate::util::config;
use crate::util::user_input::UserInput;
use crate::util::IdResponse;

use crate::pro::users::OidcError::OidcDisabled;
use actix_web::Either;
use actix_web::{web, HttpResponse, Responder};
use serde::Deserialize;
use serde::Serialize;
use snafu::ensure;
use snafu::ResultExt;
use utoipa::ToSchema;

pub(crate) fn init_user_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ProContext,
{
    cfg.service(web::resource("/user").route(web::post().to(register_user_handler::<C>)))
        .service(web::resource("/anonymous").route(web::post().to(anonymous_handler::<C>)))
        .service(web::resource("/login").route(web::post().to(login_handler::<C>)))
        .service(web::resource("/logout").route(web::post().to(logout_handler::<C>)))
        .service(web::resource("/session").route(web::get().to(session_handler::<C>)))
        .service(
            web::resource("/session/project/{project}")
                .route(web::post().to(session_project_handler::<C>)),
        )
        .service(web::resource("/session/view").route(web::post().to(session_view_handler::<C>)))
        .service(web::resource("/quota").route(web::get().to(quota_handler::<C>)))
        .service(
            web::resource("/quotas/{user}")
                .route(web::get().to(get_user_quota_handler::<C>))
                .route(web::post().to(update_user_quota_handler::<C>)),
        )
        .service(web::resource("/oidcInit").route(web::post().to(oidc_init::<C>)))
        .service(web::resource("/oidcLogin").route(web::post().to(oidc_login::<C>)));
}

/// Registers a user.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/user",
    request_body = UserRegistration,
    responses(
        (status = 200, description = "The id of the created user", body = UserId,
            example = json!({
                "id": "5b4466d2-8bab-4ed8-a182-722af3c80958"
            })
        )
    ))]
pub(crate) async fn register_user_handler<C: ProContext>(
    user: web::Json<UserRegistration>,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    ensure!(
        config::get_config_element::<crate::pro::util::config::User>()?.user_registration,
        error::UserRegistrationDisabled
    );

    let user = user.into_inner().validated()?;
    let id = ctx.user_db_ref().register(user).await?;
    Ok(web::Json(IdResponse::from(id)))
}

/// Creates a session by providing user credentials. The session's id serves as a Bearer token for requests.
#[utoipa::path(
tag = "Session",
post,
path = "/login",
request_body = UserCredentials,
responses(
    (status = 200, description = "The created session", body = UserSession,
        example = json!({
            "id": "208fa24e-7a92-4f57-a3fe-d1177d9f18ad",
            "user": {
                "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
                "email": "foo@example.com",
                "realName": "Foo Bar"
            },
            "created": "2021-04-26T13:47:10.579724800Z",
            "validUntil": "2021-04-26T14:47:10.579775400Z",
            "project": null,
            "view": null,
            "roles": [
                "fa5be363-bc0d-4bfa-85c7-ebb5cd9a8783",
                "4e8081b6-8aa6-4275-af0c-2fa2da557d28"
            ]
        })
    )
))]
pub(crate) async fn login_handler<C: ProContext>(
    user: web::Json<UserCredentials>,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    let session = ctx
        .user_db_ref()
        .login(user.into_inner())
        .await
        .map_err(Box::new)
        .context(error::Authorization)?;
    Ok(web::Json(session))
}

/// Ends a session.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/logout",
    responses(
        (status = 200, description = "The Session was deleted.")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn logout_handler<C: ProContext>(
    session: UserSession,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    ctx.user_db_ref().logout(session.id).await?;
    Ok(HttpResponse::Ok())
}

/// Retrieves details about the current session.
#[utoipa::path(
    tag = "Session",
    get,
    path = "/session",
    responses(
        (status = 200, description = "The current session", body = UserSession,
            example = json!({
                "id": "208fa24e-7a92-4f57-a3fe-d1177d9f18ad",
                "user": {
                    "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
                    "email": "foo@example.com",
                    "realName": "Foo Bar"
                },
                "created": "2021-04-26T13:47:10.579724800Z",
                "validUntil": "2021-04-26T14:47:10.579775400Z",
                "project": null,
                "view": null,
                "roles": [
                    "fa5be363-bc0d-4bfa-85c7-ebb5cd9a8783",
                    "4e8081b6-8aa6-4275-af0c-2fa2da557d28"
                ]
            })
        )
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::unused_async)] // the function signature of request handlers requires it
pub(crate) async fn session_handler<C: ProContext>(session: C::Session) -> impl Responder {
    web::Json(session)
}

/// Creates session for anonymous user. The session's id serves as a Bearer token for requests.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/anonymous",
    responses(
        (status = 200, description = "The created session", body = UserSession,
            example = json!({
                "id": "208fa24e-7a92-4f57-a3fe-d1177d9f18ad",
                "user": {
                    "id": "5b4466d2-8bab-4ed8-a182-722af3c80958",
                    "email": null,
                    "realName": null
                },
                "created": "2021-04-26T13:47:10.579724800Z",
                "validUntil": "2021-04-26T14:47:10.579775400Z",
                "project": null,
                "view": null,
                "roles": [
                    "8a27e61f-cc4d-4d0b-ae8c-4f1c91d07f5a",
                    "fd8e87bf-515c-4f36-8da6-1a53702ff102"
                ]
            })
        )
    )
)]
pub(crate) async fn anonymous_handler<C: ProContext>(ctx: web::Data<C>) -> Result<impl Responder> {
    if !config::get_config_element::<crate::util::config::Session>()?.anonymous_access {
        return Err(error::Error::Authorization {
            source: Box::new(error::Error::AnonymousAccessDisabled),
        });
    }

    let session = ctx.user_db_ref().anonymous().await?;
    Ok(web::Json(session))
}

/// Sets the active project of the session.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/session/project/{project}",
    responses(
        (status = 200, description = "The project of the session was updated."),
    ),
    params(
        ("project" = ProjectId, description = "Project id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn session_project_handler<C: ProContext>(
    project: web::Path<ProjectId>,
    session: UserSession,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    ctx.user_db_ref()
        .set_session_project(&session, project.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

// TODO: /view instead of /session/view
#[utoipa::path(
    tag = "Session",
    post,
    path = "/session/view",
    request_body = STRectangle,
    responses(
        (status = 200, description = "The view of the session was updated."),
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn session_view_handler<C: ProContext>(
    session: C::Session,
    ctx: web::Data<C>,
    view: web::Json<STRectangle>,
) -> Result<impl Responder> {
    ctx.user_db_ref()
        .set_session_view(&session, view.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct Quota {
    pub available: i64,
    pub used: u64,
}

/// Retrieves the available and used quota of the current user.
#[utoipa::path(
    tag = "User",
    get,
    path = "/quota",
    responses(
        (status = 200, description = "The available and used quota of the user", body = Quota,
            example = json!({
                "available": 4321,
                "used": 1234,
            })
        )
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn quota_handler<C: ProContext>(
    ctx: web::Data<C>,
    session: C::Session,
) -> Result<impl Responder> {
    let available = ctx.user_db_ref().quota_available(&session).await?;
    let used = ctx.user_db_ref().quota_used(&session).await?;

    Ok(web::Json(Quota { available, used }))
}

/// Retrieves the available and used quota of a specific user.
#[utoipa::path(
    tag = "User",
    get,
    path = "/quotas/{user}",
    responses(
        (status = 200, description = "The available and used quota of the user", body = Quota,
            example = json!({
                "available": 4321,
                "used": 1234,
            })
        )
    ),
    params(
        ("user" = UserId, description = "User id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn get_user_quota_handler<C: ProContext>(
    ctx: web::Data<C>,
    session: Either<AdminSession, C::Session>,
    user: web::Path<UserId>,
) -> Result<impl Responder> {
    let user = user.into_inner();

    if let Either::Right(session) = session {
        if session.user.id != user {
            return Err(error::Error::Authorization {
                source: Box::new(error::Error::OperationRequiresAdminPrivilige),
            });
        }
    }

    let available = ctx.user_db_ref().quota_available_by_user(&user).await?;
    let used = ctx.user_db_ref().quota_used_by_user(&user).await?;
    Ok(web::Json(Quota { available, used }))
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct UpdateQuota {
    pub available: i64,
}

/// Update the available quota of a specific user.
#[utoipa::path(
    tag = "User",
    post,
    path = "/quotas/{user}",
    request_body = UpdateQuota,
    responses(
        (status = 200, description = "Quota was updated")
    ),
    params(
        ("user" = UserId, description = "User id")
    ),
    security(
        ("session_token" = [])
    )
)]
pub(crate) async fn update_user_quota_handler<C: ProContext>(
    ctx: web::Data<C>,
    _session: AdminSession,
    user: web::Path<UserId>,
    update: web::Json<UpdateQuota>,
) -> Result<impl Responder> {
    let user = user.into_inner();

    let update = update.into_inner();

    ctx.user_db_ref()
        .update_quota_available_by_user(&user, update.available)
        .await?;

    Ok(actix_web::HttpResponse::Ok())
}

/// Initializes the Open Id Connect login procedure by requesting a parametrized url to the configured Id Provider.
///
/// # Example
///
/// ```text
/// POST /oidcInit
///
/// ```
/// Response:
/// ```text
/// {
///   "url": "http://someissuer.com/authorize?client_id=someclient&redirect_uri=someuri&response_type=code&scope=somescope&state=somestate&nonce=somenonce&codechallenge=somechallenge&code_challenge_method=S256"
/// }
/// ```
///
/// # Errors
///
/// This call fails if Open ID Connect is disabled, misconfigured or the Id Provider is unreachable.
pub(crate) async fn oidc_init<C: ProContext>(ctx: web::Data<C>) -> Result<impl Responder> {
    ensure!(
        config::get_config_element::<crate::pro::util::config::Oidc>()?.enabled,
        crate::pro::users::OidcDisabled
    );
    let request_db = ctx.oidc_request_db().ok_or(OidcDisabled)?;
    let oidc_client = request_db.get_client().await?;

    let result = ctx
        .oidc_request_db()
        .ok_or(OidcDisabled)?
        .generate_request(oidc_client)
        .await?;

    Ok(web::Json(result))
}

/// Creates a session for a user via a login with Open Id Connect.
/// This call must be preceded by a call to oidcInit and match the parameters of that call.
///
/// # Example
///
/// ```text
/// POST /oidcLogin
///
/// {
///   "session_state": "somesessionstate",
///   "code": "somecode",
///   "state": "somestate"
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
/// This call fails if the [`AuthCodeResponse`] is invalid,
/// if a previous oidcLogin call with the same state was already successfully or unsuccessfully resolved,
/// if the Open Id Connect configuration is invalid,
/// or if the Id Provider is unreachable.
pub(crate) async fn oidc_login<C: ProContext>(
    response: web::Json<AuthCodeResponse>,
    ctx: web::Data<C>,
) -> Result<impl Responder> {
    ensure!(
        config::get_config_element::<crate::pro::util::config::Oidc>()?.enabled,
        crate::pro::users::OidcDisabled
    );
    let request_db = ctx.oidc_request_db().ok_or(OidcDisabled)?;
    let oidc_client = request_db.get_client().await?;

    let (user, duration) = request_db
        .resolve_request(oidc_client, response.into_inner())
        .await?;

    let session = ctx.user_db_ref().login_external(user, duration).await?;

    Ok(web::Json(session))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::contexts::Session;
    use crate::handlers::ErrorResponse;
    use crate::pro::util::tests::mock_oidc::{
        mock_jwks, mock_provider_metadata, mock_token_response, MockTokenConfig, SINGLE_STATE,
    };
    use crate::pro::util::tests::{
        create_project_helper, create_session_helper, register_ndvi_workflow_helper,
        send_pro_test_request,
    };
    use crate::pro::{contexts::ProInMemoryContext, users::UserId};
    use crate::util::tests::{check_allowed_http_methods, read_body_string};
    use crate::util::user_input::Validated;

    use crate::pro::users::{AuthCodeRequestURL, OidcRequestDb};
    use crate::pro::util::config::Oidc;
    use actix_http::header::CONTENT_TYPE;
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::operations::image::{Colorizer, DefaultColors, RgbaColor};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;
    use httptest::matchers::request;
    use httptest::responders::status_code;
    use httptest::{Expectation, Server};
    use serde_json::json;
    use serial_test::serial;

    async fn register_test_helper<C: ProContext>(
        ctx: C,
        method: Method,
        email: &str,
    ) -> ServiceResponse {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn register() {
        let ctx = ProInMemoryContext::test_default();

        let res = register_test_helper(ctx, Method::POST, "foo@example.com").await;

        assert_eq!(res.status(), 200);

        let _user: IdResponse<UserId> = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn register_fail() {
        let ctx = ProInMemoryContext::test_default();

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
        let ctx = ProInMemoryContext::test_default();

        register_test_helper(ctx.clone(), Method::POST, "foo@example.com").await;

        // register user
        let res = register_test_helper(ctx, Method::POST, "foo@example.com").await;

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
        let ctx = ProInMemoryContext::test_default();

        check_allowed_http_methods(
            |method| register_test_helper(ctx.clone(), method, "foo@example.com"),
            &[Method::POST],
        )
        .await;
    }

    #[tokio::test]
    async fn register_invalid_body() {
        let ctx = ProInMemoryContext::test_default();

        // register user
        let req = test::TestRequest::post()
            .uri("/user")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_payload("no json");
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        )
        .await;
    }

    #[tokio::test]
    async fn register_missing_fields() {
        let ctx = ProInMemoryContext::test_default();

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
        let ctx = ProInMemoryContext::test_default();

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
        let ctx = ProInMemoryContext::test_default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db_ref().register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".to_string(),
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
        let ctx = ProInMemoryContext::test_default();

        let req = test::TestRequest::post()
            .uri("/login")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::CONTENT_TYPE, mime::APPLICATION_JSON))
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
        let ctx = ProInMemoryContext::test_default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db_ref().register(user).await.unwrap();

        let credentials = json!({
            "email": "foo@example.com",
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
            "missing field `password` at line 1 column 27",
        )
        .await;
    }

    async fn logout_test_helper(method: Method) -> ServiceResponse {
        let ctx = ProInMemoryContext::test_default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: " Foo Bar".to_string(),
            },
        };

        ctx.user_db_ref().register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx.user_db_ref().login(credentials).await.unwrap();

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
        let ctx = ProInMemoryContext::test_default();

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
        let ctx = ProInMemoryContext::test_default();

        let req = test::TestRequest::post().uri("/logout").append_header((
            header::AUTHORIZATION,
            Bearer::new("6ecff667-258e-4108-9dc9-93cb8c64793c"),
        ));
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(res, 401, "InvalidSession", "The session id is invalid.").await;
    }

    #[tokio::test]
    async fn logout_wrong_scheme() {
        let ctx = ProInMemoryContext::test_default();

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
        let ctx = ProInMemoryContext::test_default();

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
        let ctx = ProInMemoryContext::test_default();

        let session = create_session_helper(&ctx).await;

        let req = test::TestRequest::get()
            .uri("/session")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_pro_test_request(req, ctx.clone()).await;

        let session: UserSession = test::read_body_json(res).await;

        ctx.user_db_ref().logout(session.id).await.unwrap();

        let req = test::TestRequest::get()
            .uri("/session")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(res, 401, "InvalidSession", "The session id is invalid.").await;
    }

    #[tokio::test]
    async fn session_view_project() {
        let ctx = ProInMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        let req = test::TestRequest::post()
            .uri(&format!("/session/project/{project}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            ctx.user_db_ref().session(session.id).await.unwrap().project,
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
            ctx.user_db_ref().session(session.id()).await.unwrap().view,
            Some(rect)
        );
    }

    #[tokio::test]
    #[serial]
    async fn it_disables_anonymous_access() {
        let ctx = ProInMemoryContext::test_default();

        let req = test::TestRequest::post().uri("/anonymous");
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        config::set_config("session.anonymous_access", false).unwrap();

        let ctx = ProInMemoryContext::test_default();

        let req = test::TestRequest::post().uri("/anonymous");
        let res = send_pro_test_request(req, ctx.clone()).await;

        config::set_config("session.anonymous_access", true).unwrap();

        ErrorResponse::assert(
            res,
            401,
            "AnonymousAccessDisabled",
            "Anonymous access is disabled, please log in",
        )
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn it_disables_user_registration() {
        let ctx = ProInMemoryContext::test_default();

        let user_reg = UserRegistration {
            email: "foo@example.com".to_owned(),
            password: "secret123".to_owned(),
            real_name: "Foo Bar".to_owned(),
        };

        let req = test::TestRequest::post()
            .append_header((header::CONTENT_LENGTH, 0))
            .uri("/user")
            .set_json(&user_reg);
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        config::set_config("user.user_registration", false).unwrap();

        let ctx = ProInMemoryContext::test_default();

        let req = test::TestRequest::post()
            .append_header((header::CONTENT_LENGTH, 0))
            .uri("/user")
            .set_json(&user_reg);
        let res = send_pro_test_request(req, ctx.clone()).await;

        config::set_config("user.user_registration", true).unwrap();

        ErrorResponse::assert(
            res,
            400,
            "UserRegistrationDisabled",
            "User registration is disabled",
        )
        .await;
    }

    const MOCK_CLIENT_ID: &str = "";

    fn single_state_nonce_request_db(issuer: String) -> OidcRequestDb {
        let oidc_config = Oidc {
            enabled: true,
            issuer,
            client_id: MOCK_CLIENT_ID.to_string(),
            client_secret: None,
            redirect_uri: "https://dummy-redirect.com/".into(),
            scopes: vec!["profile".to_string(), "email".to_string()],
        };

        OidcRequestDb::from_oidc_with_static_tokens(oidc_config)
    }

    fn mock_valid_provider_discovery(expected_discoveries: usize) -> Server {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());

        let provider_metadata = mock_provider_metadata(server_url.as_str());
        let jwks = mock_jwks();

        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/.well-known/openid-configuration",
            ))
            .times(expected_discoveries)
            .respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(&provider_metadata).unwrap()),
            ),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/jwk"))
                .times(expected_discoveries)
                .respond_with(
                    status_code(200)
                        .insert_header("content-type", "application/json")
                        .body(serde_json::to_string(&jwks).unwrap()),
                ),
        );
        server
    }

    async fn oidc_init_test_helper(method: Method, ctx: ProInMemoryContext) -> ServiceResponse {
        let req = test::TestRequest::default()
            .method(method)
            .uri("/oidcInit")
            .append_header((header::CONTENT_LENGTH, 0));
        send_pro_test_request(req, ctx).await
    }

    async fn oidc_login_test_helper(
        method: Method,
        ctx: ProInMemoryContext,
        auth_code_response: AuthCodeResponse,
    ) -> ServiceResponse {
        let req = test::TestRequest::default()
            .method(method)
            .uri("/oidcLogin")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&auth_code_response);
        send_pro_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn oidc_init() {
        let server = mock_valid_provider_discovery(1);
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_request_db(server_url);

        let ctx = ProInMemoryContext::new_with_oidc(request_db);
        let res = oidc_init_test_helper(Method::POST, ctx).await;

        assert_eq!(res.status(), 200);
        let _auth_code_url: AuthCodeRequestURL = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn oidc_illegal_provider() {
        let server = Server::run();
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_request_db(server_url);

        let error_message = serde_json::to_string(&json!({
            "error_description": "Dummy bad request",
            "error": "catch_all_error"
        }))
        .expect("Serde Json unsuccessful");

        server.expect(
            Expectation::matching(request::method_path(
                "GET",
                "/.well-known/openid-configuration",
            ))
            .respond_with(
                status_code(404)
                    .insert_header("content-type", "application/json")
                    .body(error_message),
            ),
        );

        let ctx = ProInMemoryContext::new_with_oidc(request_db);
        let res = oidc_init_test_helper(Method::POST, ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "OidcError",
            "OidcError: ProviderDiscoveryError: Server returned invalid response: HTTP status code 404 Not Found",
        ).await;
    }

    #[tokio::test]
    async fn oidc_init_invalid_method() {
        let ctx = ProInMemoryContext::test_default();

        check_allowed_http_methods(
            |method| oidc_init_test_helper(method, ctx.clone()),
            &[Method::POST],
        )
        .await;
    }

    #[tokio::test]
    async fn oidc_login() {
        let server = mock_valid_provider_discovery(2);
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_request_db(server_url.clone());

        let client = request_db.get_client().await.unwrap();
        let request = request_db.generate_request(client).await;

        assert!(request.is_ok());

        let mock_token_config =
            MockTokenConfig::create_from_issuer_and_client(server_url, MOCK_CLIENT_ID.to_string());
        let token_response = mock_token_response(mock_token_config);
        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                status_code(200)
                    .insert_header("content-type", "application/json")
                    .body(serde_json::to_string(&token_response).unwrap()),
            ),
        );

        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: SINGLE_STATE.to_string(),
        };

        let ctx = ProInMemoryContext::new_with_oidc(request_db);
        let res = oidc_login_test_helper(Method::POST, ctx, auth_code_response).await;

        assert_eq!(res.status(), 200);

        let _id: UserSession = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn oidc_login_illegal_request() {
        let server = mock_valid_provider_discovery(1);
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_request_db(server_url.clone());

        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: SINGLE_STATE.to_string(),
        };

        let ctx = ProInMemoryContext::new_with_oidc(request_db);
        let res = oidc_login_test_helper(Method::POST, ctx, auth_code_response).await;

        ErrorResponse::assert(
            res,
            400,
            "OidcError",
            "OidcError: Login failed: Request unknown",
        )
        .await;
    }

    #[tokio::test]
    async fn oidc_login_fail() {
        let server = mock_valid_provider_discovery(2);
        let server_url = format!("http://{}", server.addr());
        let request_db = single_state_nonce_request_db(server_url);

        let client = request_db.get_client().await.unwrap();
        let request = request_db.generate_request(client).await;

        assert!(request.is_ok());

        let error_message = serde_json::to_string(&json!({
            "error_description": "Dummy bad request",
            "error": "catch_all_error"
        }))
        .expect("Serde Json unsuccessful");

        server.expect(
            Expectation::matching(request::method_path("POST", "/token")).respond_with(
                status_code(404)
                    .insert_header("content-type", "application/json")
                    .body(error_message),
            ),
        );

        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: SINGLE_STATE.to_string(),
        };

        let ctx = ProInMemoryContext::new_with_oidc(request_db);
        let res = oidc_login_test_helper(Method::POST, ctx, auth_code_response).await;

        ErrorResponse::assert(
            res,
            400,
            "OidcError",
            "OidcError: Verification failed: Request for code to token exchange failed",
        )
        .await;
    }

    #[tokio::test]
    async fn oidc_login_invalid_method() {
        let ctx = ProInMemoryContext::test_default();

        let auth_code_response = AuthCodeResponse {
            session_state: String::new(),
            code: String::new(),
            state: String::new(),
        };

        check_allowed_http_methods(
            |method| oidc_login_test_helper(method, ctx.clone(), auth_code_response.clone()),
            &[Method::POST],
        )
        .await;
    }

    #[tokio::test]
    async fn oidc_login_invalid_body() {
        let ctx = ProInMemoryContext::test_default();

        let req = test::TestRequest::post()
            .uri("/oidcLogin")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_payload("no json");
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        )
        .await;
    }

    #[tokio::test]
    async fn oidc_login_missing_fields() {
        let ctx = ProInMemoryContext::test_default();

        let auth_code_response = json!({
            "sessionState": "",
            "code": "",
        });

        // register user
        let req = test::TestRequest::post()
            .uri("/oidcLogin")
            .append_header((header::CONTENT_LENGTH, 0))
            .set_json(&auth_code_response);
        let res = send_pro_test_request(req, ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "BodyDeserializeError",
            "missing field `state` at line 1 column 29",
        )
        .await;
    }

    #[tokio::test]
    async fn oidc_login_invalid_type() {
        let ctx = ProInMemoryContext::test_default();

        // register user
        let req = test::TestRequest::post()
            .uri("/oidcLogin")
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

    #[tokio::test]
    async fn it_gets_quota() {
        let ctx = ProInMemoryContext::test_default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            },
        };

        ctx.user_db_ref().register(user).await.unwrap();

        let credentials = UserCredentials {
            email: "foo@example.com".to_string(),
            password: "secret123".to_string(),
        };

        let session = ctx.user_db_ref().login(credentials).await.unwrap();

        ctx.user_db_ref()
            .increment_quota_used(&session.user.id, 111)
            .await
            .unwrap();

        // current user quota
        let req = test::TestRequest::get()
            .uri("/quota")
            .append_header((header::AUTHORIZATION, format!("Bearer {}", session.id)));
        let res = send_pro_test_request(req, ctx.clone()).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.used, 111);

        // specific user quota (self)
        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{}", session.user.id))
            .append_header((header::AUTHORIZATION, format!("Bearer {}", session.id)));
        let res = send_pro_test_request(req, ctx.clone()).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.used, 111);

        // specific user quota (other)
        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{}", uuid::Uuid::new_v4()))
            .append_header((header::AUTHORIZATION, format!("Bearer {}", session.id)));
        let res = send_pro_test_request(req, ctx.clone()).await;
        assert_eq!(res.status(), 401);

        // specific user quota as admin
        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{}", session.user.id))
            .append_header((
                header::AUTHORIZATION,
                format!(
                    "Bearer {}",
                    config::get_config_element::<config::Session>()
                        .unwrap()
                        .admin_session_token
                        .unwrap()
                ),
            ));
        let res = send_pro_test_request(req, ctx).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.used, 111);
    }

    #[tokio::test]
    async fn it_updates_quota() {
        let ctx = ProInMemoryContext::test_default();

        let user = Validated {
            user_input: UserRegistration {
                email: "foo@example.com".to_string(),
                password: "secret123".to_string(),
                real_name: "Foo Bar".to_string(),
            },
        };

        let user_id = ctx.user_db_ref().register(user).await.unwrap();

        let admin_session: UserSession = AdminSession::default().into();

        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{user_id}"))
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id),
            ));
        let res = send_pro_test_request(req, ctx.clone()).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.available, 0);

        let update = UpdateQuota { available: 123 };

        let req = test::TestRequest::post()
            .uri(&format!("/quotas/{user_id}"))
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id),
            ))
            .set_json(update);
        let res = send_pro_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let req = test::TestRequest::get()
            .uri(&format!("/quotas/{user_id}"))
            .append_header((
                header::AUTHORIZATION,
                format!("Bearer {}", admin_session.id),
            ));
        let res = send_pro_test_request(req, ctx.clone()).await;
        let quota: Quota = test::read_body_json(res).await;
        assert_eq!(quota.available, 123);
    }

    #[tokio::test]
    async fn it_checks_quota_before_querying() {
        let ctx = ProInMemoryContext::test_default();

        let session = ctx.user_db_ref().anonymous().await.unwrap();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            DefaultColors::OverUnder {
                over_color: RgbaColor::white(),
                under_color: RgbaColor::black(),
            },
        )
        .unwrap();

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            (
                "bbox",
                "1.95556640625,0.90087890625,1.9775390625,0.9228515625",
            ),
            ("width", "256"),
            ("height", "256"),
            ("crs", "EPSG:4326"),
            (
                "styles",
                &format!("custom:{}", serde_json::to_string(&colorizer).unwrap()),
            ),
            ("format", "image/png"),
            ("time", "2014-04-01T12:00:00.0Z"),
            ("exceptions", "JSON"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_pro_test_request(req, ctx.clone()).await;

        ErrorResponse::assert(res, 200, "Operator", "Operator: QuotaExhausted").await;

        ctx.user_db_ref()
            .update_quota_available_by_user(&session.user.id, 9999)
            .await
            .unwrap();

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id.to_string())));
        let res = send_pro_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get(&CONTENT_TYPE),
            Some(&header::HeaderValue::from_static("image/png"))
        );
    }
}
