use uuid::Uuid;
use warp::Filter;

use crate::{
    contexts::{Context, Session, SimpleContext},
    handlers::authenticate,
    projects::{ProjectId, STRectangle},
};

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
pub(crate) fn anonymous_handler<C: SimpleContext>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("anonymous")
        .and(warp::post())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(anonymous)
}

// TODO: move into handler once async closures are available?
async fn anonymous<C: SimpleContext>(ctx: C) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(warp::reply::json(&ctx.default_session().await))
}

/// Retrieves details about the [Session].
///
/// # Example
///
/// ```text
/// GET /session
/// Authorization: Bearer fc9b5dc2-a1eb-400f-aeed-a7845d9935c9
/// ```
/// Response:
/// ```text
/// {
///   "id": "29fb1e93-7b6b-466f-952a-fdde87736c62",
///   "user": {
///     "id": "f33429a5-d207-4e59-827d-fc48f9630c9c",
///     "email": "foo@bar.de",
///     "realName": "Foo Bar"
///   },
///   "created": "2021-04-18T17:20:44.190720500Z",
///   "validUntil": "2021-04-18T18:20:44.190726700Z",
///   "project": null,
///   "view": null
/// }
/// ```
///
/// # Errors
///
/// This call fails if the session is invalid.
pub(crate) fn session_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("session")
        .and(warp::get())
        .and(authenticate(ctx))
        .and_then(session)
}

// TODO: move into handler once async closures are available?
async fn session<S: Session>(session: S) -> Result<impl warp::Reply, warp::Rejection> {
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
pub(crate) fn session_project_handler<C: SimpleContext>(
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
async fn session_project<C: SimpleContext>(
    project: ProjectId,
    mut session: C::Session,
    mut ctx: C,
) -> Result<impl warp::Reply, warp::Rejection> {
    session.project = Some(project);

    ctx.set_default_session(session);

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
pub(crate) fn session_view_handler<C: SimpleContext>(
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
async fn session_view<C: SimpleContext>(
    mut session: C::Session,
    mut ctx: C,
    view: STRectangle,
) -> Result<impl warp::Reply, warp::Rejection> {
    session.view = Some(view);

    ctx.set_default_session(session);

    Ok(warp::reply())
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use warp::http::Response;
    use warp::hyper::body::Bytes;

    use crate::{
        contexts::{InMemoryContext, SimpleSession},
        handlers::{handle_rejection, ErrorResponse},
        util::tests::{check_allowed_http_methods, create_project_helper},
    };

    use super::*;

    #[tokio::test]
    async fn session() {
        let ctx = InMemoryContext::default();

        let session = ctx.default_session();

        let res = warp::test::request()
            .method("GET")
            .path("/session")
            .header(
                "Authorization",
                format!("Bearer {}", session.id().to_string()),
            )
            .reply(&session_handler(ctx.clone()).recover(handle_rejection))
            .await;

        let body = std::str::from_utf8(&res.body()).unwrap();
        let session: SimpleSession = serde_json::from_str(body).unwrap();

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
        let ctx = InMemoryContext::default();

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

        assert_eq!(ctx.default_session().project(), Some(project));

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

        assert_eq!(ctx.default_session().view(), Some(rect).as_ref());
    }

    async fn anonymous_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        warp::test::request()
            .method(method)
            .path("/anonymous")
            .reply(&anonymous_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn anonymous() {
        let res = anonymous_test_helper("POST").await;

        assert_eq!(res.status(), 200);

        let body = std::str::from_utf8(&res.body()).unwrap();
        let _session = serde_json::from_str::<SimpleSession>(&body).unwrap();
    }

    #[tokio::test]
    async fn anonymous_invalid_method() {
        check_allowed_http_methods(anonymous_test_helper, &["POST"]).await;
    }
}
