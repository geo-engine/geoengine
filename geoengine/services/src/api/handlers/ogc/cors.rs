use crate::config;
use actix_web::{
    Error, HttpResponse,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::{
        Method,
        header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue, VARY,
        },
    },
};

fn ogc_config() -> Result<config::Ogc, Error> {
    config::get_config_element::<config::Ogc>().map_err(Into::into)
}

/// Applies CORS headers to the given response based on the OGC configuration.
///
/// # Errors
///
/// Returns an `Error` if the OGC configuration cannot be retrieved or if any of the header values are invalid.
///
fn apply_cors_headers<B>(response: &mut ServiceResponse<B>) -> Result<(), Error>
where
    B: MessageBody + 'static,
{
    let Some(config) = ogc_config()?.cors else {
        return Ok(());
    };

    let headers = response.headers_mut();
    if let Some(allowed_origin) = &config.allow_origin {
        headers.insert(
            ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_str(allowed_origin.trim())?,
        );
    }
    if !config.allow_headers.is_empty() {
        headers.insert(
            ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_str(config.allow_headers.join(", ").trim())?,
        );
    }
    if !config.allow_methods.is_empty() {
        headers.insert(
            ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_str(config.allow_methods.join(", ").trim())?,
        );
    }
    // This response depends on the request's Origin, so caches must not reuse the
    // same value for a different origin. Keeping `Vary: Origin` here prevents
    // cross-origin responses from being incorrectly served from cache.
    headers.insert(VARY, HeaderValue::from_static("Origin"));

    Ok(())
}

/// CORS middleware for handling preflight requests and applying CORS headers to OGC responses.
///
/// # Errors
///
/// Returns an `Error` if the OGC configuration cannot be retrieved or if any of the header values are invalid.
///
pub async fn cors_middleware(
    req: ServiceRequest,
    next: actix_web::middleware::Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<actix_web::body::BoxBody>, Error> {
    if req.method() == Method::OPTIONS {
        let (request, _) = req.into_parts();
        let mut res = ServiceResponse::new(request, HttpResponse::NoContent().finish());
        apply_cors_headers(&mut res)?;
        return Ok(res.map_into_boxed_body());
    }

    let mut res = next.call(req).await?;
    apply_cors_headers(&mut res)?;

    Ok(res.map_into_boxed_body())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{
        App,
        http::header,
        middleware,
        test::{self, TestRequest},
        web,
    };

    #[actix_web::test]
    async fn it_sets_cors_headers_for_ogc_requests() {
        let app = test::init_service(App::new().wrap(middleware::from_fn(cors_middleware)).route(
            "/ogc/{dataConnectorId}/{layerId}/collections/{collectionId}/map/tiles",
            web::get().to(|| async { HttpResponse::Ok().json(serde_json::json!({"ok": true})) }),
        ))
        .await;

        let req = TestRequest::get()
            .uri("/ogc/123/456/collections/789/map/tiles")
            .insert_header((header::ORIGIN, "https://example.com"))
            .to_request();

        let res = test::call_service(&app, req).await;

        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("*"),
        );
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
                .and_then(|value| value.to_str().ok()),
            Some("Authorization, Content-Type"),
        );
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_METHODS)
                .and_then(|value| value.to_str().ok()),
            Some("GET, OPTIONS, HEAD"),
        );
        assert_eq!(
            res.headers()
                .get(header::VARY)
                .and_then(|value| value.to_str().ok()),
            Some("Origin"),
        );
    }

    #[actix_web::test]
    async fn it_sets_cors_headers_for_options_preflight_requests() {
        let app = test::init_service(
            App::new().wrap(middleware::from_fn(cors_middleware)).route(
                "/ogc/{dataConnectorId}/{layerId}/collections/{collectionId}/map/tiles",
                web::route()
                    .method(Method::OPTIONS)
                    .to(|| async { HttpResponse::NoContent().finish() }),
            ),
        )
        .await;

        let req = TestRequest::default()
            .method(Method::OPTIONS)
            .uri("/ogc/123/456/collections/789/map/tiles")
            .insert_header((header::ORIGIN, "https://example.com"))
            .insert_header((header::ACCESS_CONTROL_REQUEST_METHOD, "GET"))
            .insert_header((header::ACCESS_CONTROL_REQUEST_HEADERS, "Authorization"))
            .to_request();

        let res = test::call_service(&app, req).await;

        assert_eq!(res.status(), actix_web::http::StatusCode::NO_CONTENT);
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("*"),
        );
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
                .and_then(|value| value.to_str().ok()),
            Some("Authorization, Content-Type"),
        );
        assert_eq!(
            res.headers()
                .get(header::ACCESS_CONTROL_ALLOW_METHODS)
                .and_then(|value| value.to_str().ok()),
            Some("GET, OPTIONS, HEAD"),
        );
        assert_eq!(
            res.headers()
                .get(header::VARY)
                .and_then(|value| value.to_str().ok()),
            Some("Origin"),
        );
    }
}
