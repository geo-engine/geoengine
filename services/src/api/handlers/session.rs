use crate::{
    contexts::{ApplicationContext, SimpleApplicationContext},
    error::{self, Result},
    projects::{ProjectId, STRectangle},
    util::config,
};
use actix_web::{web, HttpResponse, Responder};

pub(crate) fn init_session_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: SimpleApplicationContext,
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

/// Creates session for anonymous user. The session's id serves as a Bearer token for requests.
#[utoipa::path(
    tag = "Session",
    post,
    path = "/anonymous",
    responses(
        (status = 200, description = "The created session", body = SimpleSession,
            example = json!({
                "id": "2fee8652-3192-4d3e-8adc-14257064224a",
                "project": null,
                "view": null
            })
        )
    )
)]
async fn anonymous_handler<C: SimpleApplicationContext>(
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    if !config::get_config_element::<crate::util::config::Session>()?.anonymous_access {
        return Err(error::Error::Unauthorized {
            source: Box::new(error::Error::AnonymousAccessDisabled),
        });
    }

    let session = app_ctx.default_session().await?;
    Ok(web::Json(session))
}

/// Retrieves details about the current session.
#[utoipa::path(
    tag = "Session",
    get,
    path = "/session",
    responses(
        (status = 200, description = "The current session", body = SimpleSession,
            example = json!({
                "id": "2fee8652-3192-4d3e-8adc-14257064224a",
                "project": null,
                "view": null
            })
        )
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::unused_async)] // the function signature of request handlers requires it
pub(crate) async fn session_handler<C: ApplicationContext>(session: C::Session) -> impl Responder {
    web::Json(session)
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
#[allow(clippy::no_effect_underscore_binding)] // need `_session` to quire authentication
async fn session_project_handler<C: SimpleApplicationContext>(
    project: web::Path<ProjectId>,
    _session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<impl Responder> {
    app_ctx
        .update_default_session_project(project.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

/// Sets the active view of the session.
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
#[allow(clippy::no_effect_underscore_binding)] // need `_session` to quire authentication
async fn session_view_handler<C: SimpleApplicationContext>(
    _session: C::Session,
    app_ctx: web::Data<C>,
    view: web::Json<STRectangle>,
) -> Result<impl Responder> {
    app_ctx
        .update_default_session_view(view.into_inner())
        .await?;

    Ok(HttpResponse::Ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::responses::ErrorResponse;
    use crate::contexts::{PostgresContext, Session, SessionContext};
    use crate::ge_context;
    use crate::{
        contexts::SimpleSession,
        util::tests::{check_allowed_http_methods, create_project_helper, send_test_request},
    };
    use actix_web::dev::ServiceResponse;
    use actix_web::{http::header, http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn session(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.default_session().await.unwrap();

        let req = test::TestRequest::get()
            .uri("/session")
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx).await;
        let deserialized_session: SimpleSession = test::read_body_json(res).await;

        assert_eq!(session, deserialized_session);
    }

    #[ge_context::test]
    async fn session_view_project(app_ctx: PostgresContext<NoTls>) {
        let ctx = app_ctx.default_session_context().await.unwrap();

        let session = ctx.session().clone();
        let project = create_project_helper(&app_ctx).await;

        let req = test::TestRequest::post()
            .uri(&format!("/session/project/{project}"))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())));
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            app_ctx.default_session().await.unwrap().project(),
            Some(project)
        );

        let rect =
            STRectangle::new_unchecked(SpatialReferenceOption::Unreferenced, 0., 0., 1., 1., 0, 1);
        let req = test::TestRequest::post()
            .uri("/session/view")
            .append_header((header::CONTENT_LENGTH, 0))
            .append_header((header::AUTHORIZATION, Bearer::new(session.id().to_string())))
            .set_json(&rect);
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        assert_eq!(
            app_ctx.default_session().await.unwrap().view(),
            Some(rect).as_ref()
        );
    }

    async fn anonymous_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let req = test::TestRequest::default()
            .method(method)
            .uri("/anonymous");
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test(test_execution = "serial")]
    async fn anonymous(app_ctx: PostgresContext<NoTls>) {
        let res = anonymous_test_helper(app_ctx, Method::POST).await;

        assert_eq!(res.status(), 200);

        let _session: SimpleSession = test::read_body_json(res).await;
    }

    #[ge_context::test]
    async fn anonymous_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| anonymous_test_helper(app_ctx.clone(), method),
            &[Method::POST],
        )
        .await;
    }

    fn it_disables_anonymous_access_before() {
        config::set_config(
            "session.fixed_session_token",
            "18fec623-6600-41af-b82b-24ccf47cb9f9",
        )
        .unwrap();
    }

    #[ge_context::test(
        test_execution = "serial",
        before = "it_disables_anonymous_access_before"
    )]
    async fn it_disables_anonymous_access(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::post().uri("/anonymous");
        let res = send_test_request(req, app_ctx.clone()).await;

        assert_eq!(res.status(), 200);

        let session: SimpleSession = actix_web::test::read_body_json(res).await;

        assert_eq!(
            session.id().to_string(),
            "18fec623-6600-41af-b82b-24ccf47cb9f9"
        );

        config::set_config("session.anonymous_access", false).unwrap();

        let req = test::TestRequest::post().uri("/anonymous");
        let res = send_test_request(req, app_ctx.clone()).await;

        config::set_config("session.anonymous_access", true).unwrap();

        ErrorResponse::assert(
            res,
            401,
            "Unauthorized",
            "Authorization error *\n\nCaused by this error:\n  1: Anonymous access is disabled, please log in\n\nNOTE: Some redundant information has been removed from the lines marked with *. Set SNAFU_RAW_ERROR_MESSAGES=1 to disable this behavior.",
        )
        .await;
    }
}
