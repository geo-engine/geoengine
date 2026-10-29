use crate::{
    api::model::responses::UnauthorizedAdminResponse, contexts::ApplicationContext, error::Result,
};
use actix_web::{FromRequest, HttpResponse, web};

pub(crate) fn init_cache_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/cache/clear").route(web::post().to(clear_handler::<C>)));
}

/// Clear operator results, STAC query metadata and GDAL worker caches.
///
/// Requires an administrator. Caching remains enabled, with unchanged capacities and lifetimes.
/// Waits for GDAL reads to finish and replacement workers to start before returning.
/// Run without concurrent queries for comparable fresh-access measurements: concurrent work
/// can repopulate caches during clearance. Does not clear Wildlive's persistent database cache,
/// operating-system caches, or caches in clients, proxies and upstream services.
#[utoipa::path(
    tag = "Cache",
    post,
    path = "/cache/clear",
    responses(
        (status = 204, description = "Caches cleared; GDAL workers are ready."),
        (status = 401, response = UnauthorizedAdminResponse),
        (status = 500, description = "Cache clearance failed; some caches may already have been cleared.")
    ),
    security(("session_token" = []))
)]
async fn clear_handler<C: ApplicationContext>(
    session: C::Session,
    app_ctx: web::Data<C>,
) -> Result<HttpResponse> {
    app_ctx.clear_caches(session).await?;
    Ok(HttpResponse::NoContent().finish())
}

#[cfg(test)]
mod tests {
    use crate::{
        contexts::{PostgresContext, Session},
        ge_context,
        users::UserAuth,
        util::tests::{admin_login, send_test_request},
    };
    use actix_web::{http::header, test::TestRequest};
    use actix_web_httpauth::headers::authorization::Bearer;
    use tokio_postgres::NoTls;

    #[ge_context::test]
    async fn it_requires_admin_and_completes_clearance(app_ctx: PostgresContext<NoTls>) {
        let response =
            send_test_request(TestRequest::post().uri("/cache/clear"), app_ctx.clone()).await;
        assert_eq!(response.status(), 401);

        let anonymous = app_ctx.create_anonymous_session().await.unwrap();
        let response = send_test_request(
            TestRequest::post().uri("/cache/clear").append_header((
                header::AUTHORIZATION,
                Bearer::new(anonymous.id().to_string()),
            )),
            app_ctx.clone(),
        )
        .await;
        assert_eq!(response.status(), 401);

        let admin = admin_login(&app_ctx).await;
        // Repeated clearing is supported; caching is not disabled by the endpoint.
        for _ in 0..2 {
            let response = send_test_request(
                TestRequest::post()
                    .uri("/cache/clear")
                    .append_header((header::AUTHORIZATION, Bearer::new(admin.id().to_string()))),
                app_ctx.clone(),
            )
            .await;
            assert_eq!(response.status(), 204);
        }
    }
}
