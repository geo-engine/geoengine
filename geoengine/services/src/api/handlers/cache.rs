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
/// Waits for every GDAL worker to close its dataset and clear its remote file caches.
/// Run without concurrent queries for comparable fresh-access measurements: concurrent work
/// can repopulate caches during clearance. Does not clear Wildlive's persistent database cache,
/// operating-system caches, or caches in clients, proxies and upstream services.
#[utoipa::path(
    tag = "Cache",
    post,
    path = "/cache/clear",
    responses(
        (status = 204, description = "Caches cleared; all GDAL workers have acknowledged clearance."),
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
        util::tests::{admin_login, register_ndvi_processing_graph_helper, send_test_request},
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

    /// With `[cache] enable_new_raster_cache`, the second map reads its blocks and header again.
    #[ge_context::test]
    async fn it_renders_the_same_map_after_clearing(app_ctx: PostgresContext<NoTls>) {
        let admin = admin_login(&app_ctx).await;
        let (_, id) = register_ndvi_processing_graph_helper(&app_ctx).await;
        let auth = (header::AUTHORIZATION, Bearer::new(admin.id().to_string()));
        let get_map = async || {
            let response = send_test_request(
                TestRequest::get()
                    .uri(&format!("/wms/{id}?request=GetMap&service=WMS&version=1.3.0&layers={id}&bbox=20,-10,80,50&width=600&height=600&crs=EPSG:4326&styles=ssss&format=image/png&time=2014-01-01T00:00:00.0Z"))
                    .append_header(auth.clone()),
                app_ctx.clone(),
            )
            .await;
            assert_eq!(response.status(), 200);
            actix_web::test::read_body(response).await
        };

        let before = get_map().await;
        let response = send_test_request(
            TestRequest::post()
                .uri("/cache/clear")
                .append_header(auth.clone()),
            app_ctx.clone(),
        )
        .await;
        assert_eq!(response.status(), 204);

        assert_eq!(get_map().await, before);
    }
}
