use crate::{
    contexts::{Context, SimpleContext},
    error::{self, Result},
    projects::{ProjectId, STRectangle},
    util::config,
};
use actix_web::{web, HttpResponse, Responder};

pub(crate) fn init_session_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: SimpleContext,
{
    cfg.service(web::resource("/anonymous").route(web::post().to(anonymous_handler::<C>)))
        .service(
            web::scope("/session")
                .service(web::resource("").route(web::get().to(session_handler::<C>)))
                .service(
                    web::resource("/project/{project}")
                        .route(web::post().to(session_project_handler::<C>)),
                )
                .service(web::resource("/view").route(web::post().to(session_view_handler::<C>))),
        );
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
async fn anonymous_handler<C: SimpleContext>(ctx: web::Data<C>) -> Result<impl Responder> {
    if !config::get_config_element::<crate::util::config::Session>()?.anonymous_access {
        return Err(error::Error::Authorization {
            source: Box::new(error::Error::AnonymousAccessDisabled),
        });
    }

    let session = ctx.default_session_ref().await.clone();
    Ok(web::Json(session))
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
#[allow(clippy::unused_async)] // the function signature of request handlers requires it
pub(crate) async fn session_handler<C: Context>(session: C::Session) -> impl Responder {
    web::Json(session)
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
async fn session_project_handler<C: SimpleContext>(
    project: web::Path<ProjectId>,
    _session: C::Session,
    ctx: web::Data<C>,
) -> impl Responder {
    ctx.default_session_ref_mut().await.project = Some(project.into_inner());

    HttpResponse::Ok()
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
async fn session_view_handler<C: SimpleContext>(
    _session: C::Session,
    ctx: web::Data<C>,
    view: web::Json<STRectangle>,
) -> impl Responder {
    ctx.default_session_ref_mut().await.view = Some(view.into_inner());

    HttpResponse::Ok()
}

#[cfg(test)]
mod tests {
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use geoengine_datatypes::util::test::TestDefault;

    use crate::{
        contexts::{InMemoryContext, SimpleSession},
        util::tests::{check_allowed_http_methods, create_project_helper, send_test_request},
    };

    use super::*;
    use crate::contexts::Session;

    #[tokio::test]
    async fn session() {
        let ctx = InMemoryContext::test_default();

        let session = ctx.default_session_ref().await.clone();

        let req = test::TestRequest::get()
            .uri("/session")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, ctx).await;
        let deserialized_session: SimpleSession = test::read_body_json(res).await;

        assert_eq!(session, deserialized_session);
    }

    #[tokio::test]
    async fn session_view_project() {
        let ctx = InMemoryContext::test_default();

        let (session, project) = create_project_helper(&ctx).await;

        let req = test::TestRequest::post()
            .uri(&format!("/session/project/{}", project))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(ctx.default_session_ref().await.project(), Some(project));

        let rect =
            STRectangle::new_unchecked(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1);
        let req = test::TestRequest::post()
            .uri("/session/view")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(&rect);
        let res = send_test_request(req, ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(ctx.default_session_ref().await.view(), Some(rect).as_ref());
    }

    async fn anonymous_test_helper(method: Method) -> ServiceResponse {
        let ctx = InMemoryContext::test_default();

        let req = test::TestRequest::default()
            .method(method)
            .uri("/anonymous");
        send_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn anonymous() {
        let res = anonymous_test_helper(Method::POST).await;

        assert_eq!(res.status(), 200);

        let _session: SimpleSession = test::read_body_json(res).await;
    }

    #[tokio::test]
    async fn anonymous_invalid_method() {
        check_allowed_http_methods(anonymous_test_helper, &[Method::POST]).await;
    }
}
