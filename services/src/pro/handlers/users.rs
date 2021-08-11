use crate::error;
use crate::error::Result;
use crate::handlers::authenticate;
use crate::pro::contexts::ProContext;
use crate::pro::users::UserCredentials;
use crate::pro::users::UserDb;
use crate::pro::users::UserRegistration;
use crate::pro::users::UserSession;
use crate::projects::ProjectId;
use crate::projects::STRectangle;
use crate::util::user_input::UserInput;
use crate::util::IdResponse;

use snafu::ResultExt;
use uuid::Uuid;
use warp::reply::Reply;
use warp::Filter;

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
pub(crate) fn register_user_handler<C: ProContext>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path("user")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(register_user)
}

// TODO: move into handler once async closures are available?
async fn register_user<C: ProContext>(
    user: UserRegistration,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let user = user.validated()?;
    let id = ctx.user_db_ref_mut().await.register(user).await?;
    Ok(warp::reply::json(&IdResponse::from(id)))
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
pub(crate) fn login_handler<C: ProContext>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path("login")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(login)
}

// TODO: move into handler once async closures are available?
async fn login<C: ProContext>(
    user: UserCredentials,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    let session = ctx
        .user_db_ref_mut()
        .await
        .login(user)
        .await
        .map_err(Box::new)
        .context(error::Authorization)?;
    Ok(warp::reply::json(&session).into_response())
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
pub(crate) fn logout_handler<C: ProContext>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path("logout")
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(logout)
}

// TODO: move into handler once async closures are available?
async fn logout<C: ProContext>(
    session: UserSession,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.user_db_ref_mut().await.logout(session.id).await?;
    Ok(warp::reply().into_response())
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
pub(crate) fn anonymous_handler<C: ProContext>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("anonymous")
        .and(warp::post())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(anonymous)
}

// TODO: move into handler once async closures are available?
async fn anonymous<C: ProContext>(ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    let session = ctx.user_db_ref_mut().await.anonymous().await?;
    Ok(warp::reply::json(&session))
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
pub(crate) fn session_project_handler<C: ProContext>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("session" / "project" / Uuid)
        .map(ProjectId)
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and_then(session_project)
}

// TODO: move into handler once async closures are available?
async fn session_project<C: ProContext>(
    project: ProjectId,
    session: UserSession,
    ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.user_db_ref_mut()
        .await
        .set_session_project(&session, project)
        .await?;

    Ok(warp::reply())
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
pub(crate) fn session_view_handler<C: ProContext>(
    ctx: C,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    warp::path!("session" / "view")
        .and(warp::post())
        .and(authenticate(ctx.clone()))
        .and(warp::any().map(move || ctx.clone()))
        .and(warp::body::json())
        .and_then(session_view)
}

// TODO: move into handler once async closures are available?
async fn session_view<C: ProContext>(
    session: C::Session,
    ctx: C,
    view: STRectangle,
) -> Result<impl warp::Reply, warp::Rejection> {
    ctx.user_db_ref_mut()
        .await
        .set_session_view(&session, view)
        .await?;

    Ok(warp::reply())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::contexts::Session;
    use crate::handlers::handle_rejection;
    use crate::handlers::session::session_handler;
    use crate::handlers::ErrorResponse;
    use crate::pro::contexts::ProInMemoryContext;
    use crate::pro::users::UserId;
    use crate::pro::util::tests::create_project_helper;
    use crate::pro::util::tests::create_session_helper;
    use crate::util::tests::check_allowed_http_methods;
    use crate::util::user_input::Validated;

    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use serde_json::json;
    use warp::http::Response;
    use warp::hyper::body::Bytes;

    async fn register_test_helper<C: ProContext>(
        ctx: C,
        method: &str,
        email: &str,
    ) -> Response<Bytes> {
        let user = UserRegistration {
            email: email.to_string(),
            password: "secret123".to_string(),
            real_name: " Foo Bar".to_string(),
        };

        // register user
        warp::test::request()
            .method(method)
            .path("/user")
            .header("Content-Length", "0")
            .json(&user)
            .reply(&register_user_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn register() {
        let ctx = ProInMemoryContext::default();

        let res = register_test_helper(ctx, "POST", "foo@bar.de").await;

        assert_eq!(res.status(), 200);

        let body = std::str::from_utf8(res.body()).unwrap();
        assert!(serde_json::from_str::<IdResponse<UserId>>(body).is_ok());
    }

    #[tokio::test]
    async fn register_fail() {
        let ctx = ProInMemoryContext::default();

        let res = register_test_helper(ctx, "POST", "notanemail").await;

        ErrorResponse::assert(
            &res,
            400,
            "RegistrationFailed",
            "Registration failed: Invalid e-mail address",
        );
    }

    #[tokio::test]
    async fn register_duplicate_email() {
        let ctx = ProInMemoryContext::default();

        register_test_helper(ctx.clone(), "POST", "foo@bar.de").await;

        // register user
        let res = register_test_helper(ctx, "POST", "foo@bar.de").await;

        ErrorResponse::assert(
            &res,
            409,
            "Duplicate",
            "Tried to create duplicate: E-mail already exists",
        );
    }

    #[tokio::test]
    async fn register_invalid_method() {
        let ctx = ProInMemoryContext::default();

        check_allowed_http_methods(
            |method| register_test_helper(ctx.clone(), method, "foo@bar.de"),
            &["POST"],
        )
        .await;
    }

    #[tokio::test]
    async fn register_invalid_body() {
        let ctx = ProInMemoryContext::default();

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user")
            .header("Content-Length", "0")
            .body("no json")
            .reply(&register_user_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        );
    }

    #[tokio::test]
    async fn register_missing_fields() {
        let ctx = ProInMemoryContext::default();

        let user = json!({
            "password": "secret123",
            "real_name": " Foo Bar",
        });

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user")
            .header("Content-Length", "0")
            .json(&user)
            .reply(&register_user_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            400,
            "BodyDeserializeError",
            "missing field `email` at line 1 column 47",
        );
    }

    #[tokio::test]
    async fn register_invalid_type() {
        let ctx = ProInMemoryContext::default();

        // register user
        let res = warp::test::request()
            .method("POST")
            .path("/user")
            .header("Content-Length", "0")
            .header("Content-Type", "text/html")
            .reply(&register_user_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            415,
            "UnsupportedMediaType",
            "Unsupported content type header.",
        );
    }

    async fn login_test_helper(method: &str, password: &str) -> Response<Bytes> {
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

        warp::test::request()
            .method(method)
            .path("/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn login() {
        let res = login_test_helper("POST", "secret123").await;

        assert_eq!(res.status(), 200);

        let body = std::str::from_utf8(res.body()).unwrap();
        let _id: UserSession = serde_json::from_str(body).unwrap();
    }

    #[tokio::test]
    async fn login_fail() {
        let res = login_test_helper("POST", "wrong").await;

        ErrorResponse::assert(
            &res,
            401,
            "LoginFailed",
            "User does not exist or password is wrong.",
        );
    }

    #[tokio::test]
    async fn login_invalid_method() {
        check_allowed_http_methods(|method| login_test_helper(method, "secret123"), &["POST"])
            .await;
    }

    #[tokio::test]
    async fn login_invalid_body() {
        let ctx = ProInMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/login")
            .header("Content-Length", "0")
            .body("no json")
            .reply(&login_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            400,
            "BodyDeserializeError",
            "expected ident at line 1 column 2",
        );
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

        let res = warp::test::request()
            .method("POST")
            .path("/login")
            .header("Content-Length", "0")
            .json(&credentials)
            .reply(&login_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            400,
            "BodyDeserializeError",
            "missing field `password` at line 1 column 22",
        );
    }

    async fn logout_test_helper(method: &str) -> Response<Bytes> {
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

        warp::test::request()
            .method(method)
            .path("/logout")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .body("no json")
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn logout() {
        let res = logout_test_helper("POST").await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.body(), "");
    }

    #[tokio::test]
    async fn logout_missing_header() {
        let ctx = ProInMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            401,
            "MissingAuthorizationHeader",
            "Header with authorization token not provided.",
        );
    }

    #[tokio::test]
    async fn logout_wrong_token() {
        let ctx = ProInMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header(
                "Authorization",
                format!("Bearer {}", "6ecff667-258e-4108-9dc9-93cb8c64793c"),
            )
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(&res, 401, "InvalidSession", "The session id is invalid.");
    }

    #[tokio::test]
    async fn logout_wrong_scheme() {
        let ctx = ProInMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header("Authorization", "7e855f3c-b0cd-46d1-b5b3-19e6e3f9ea5")
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            401,
            "InvalidAuthorizationScheme",
            "Authentication scheme must be Bearer.",
        );
    }

    #[tokio::test]
    async fn logout_invalid_token() {
        let ctx = ProInMemoryContext::default();

        let res = warp::test::request()
            .method("POST")
            .path("/logout")
            .header("Authorization", format!("Bearer {}", "no uuid"))
            .reply(&logout_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(
            &res,
            401,
            "InvalidUuid",
            "Identifier does not have the right format.",
        );
    }

    #[tokio::test]
    async fn logout_invalid_method() {
        check_allowed_http_methods(logout_test_helper, &["POST"]).await;
    }

    #[tokio::test]
    async fn session() {
        let ctx = ProInMemoryContext::default();

        let session = create_session_helper(&ctx).await;

        let res = warp::test::request()
            .method("GET")
            .path("/session")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&session_handler(ctx.clone()).recover(handle_rejection))
            .await;

        let body = std::str::from_utf8(res.body()).unwrap();
        let session: UserSession = serde_json::from_str(body).unwrap();

        ctx.user_db()
            .write()
            .await
            .logout(session.id)
            .await
            .unwrap();

        let res = warp::test::request()
            .method("GET")
            .path("/session")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&session_handler(ctx).recover(handle_rejection))
            .await;

        ErrorResponse::assert(&res, 401, "InvalidSession", "The session id is invalid.");
    }

    #[tokio::test]
    async fn session_view_project() {
        let ctx = ProInMemoryContext::default();

        let (session, project) = create_project_helper(&ctx).await;

        let res = warp::test::request()
            .method("POST")
            .path(&format!("/session/project/{}", project.to_string()))
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&session_project_handler(ctx.clone()).recover(handle_rejection))
            .await;

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
        let res = warp::test::request()
            .method("POST")
            .header("Content-Length", "0")
            .path("/session/view")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .json(&rect)
            .reply(&session_view_handler(ctx.clone()).recover(handle_rejection))
            .await;

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
