use std::future::{ready, Ready};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    http::header::{HeaderName, HeaderValue},
    Error, HttpMessage,
};
use futures_util::future::LocalBoxFuture;
use tracing::{event_enabled, Level};
use tracing_actix_web::RequestId;

const REQUEST_ID_HEADER: &str = "x-request-header";

// There are two steps in middleware processing.
// 1. Middleware initialization, middleware factory gets called with
//    next service in chain as parameter.
// 2. Middleware's call method gets called with normal request.
pub struct OutputRequestId;

// Middleware factory is `Transform` trait
// `S` - type of the next service
// `B` - type of response's body
impl<S, B> Transform<S, ServiceRequest> for OutputRequestId
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = OutputRequestIdMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(OutputRequestIdMiddleware {
            service,
            // Only send header if request_id gets logged, because
            // otherwise correlation is impossible anyway.
            enabled: event_enabled!(target: "tracing_actix_web::middleware", Level::WARN, request_id)
                || cfg!(test),
        }))
    }
}

pub struct OutputRequestIdMiddleware<S> {
    service: S,
    enabled: bool,
}

impl<S, B> Service<ServiceRequest> for OutputRequestIdMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // TODO: rethink this error handling
        let request_id = req
            .extensions()
            .get::<RequestId>()
            .copied()
            .expect("it should not run without the `RequestId` extension");

        let fut = self.service.call(req);

        if !self.enabled {
            return Box::pin(fut);
        }

        Box::pin(async move {
            let mut res = fut.await?;

            if !res.status().is_success() {
                res.headers_mut().insert(
                    HeaderName::from_static(REQUEST_ID_HEADER),
                    HeaderValue::from_str(request_id.to_string().as_str())
                        .expect("uuid is always valid"),
                );
            }

            Ok(res)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use actix_web::test;
    use tokio_postgres::NoTls;

    use crate::{contexts::PostgresContext, ge_context, util::tests::send_test_request};

    #[ge_context::test]
    async fn it_sends_request_id_on_error(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::get().uri("/asdf1234_notfound");

        let res = send_test_request(req, app_ctx.clone()).await;

        assert!(!res.status().is_success());
        assert!(res.headers().get(REQUEST_ID_HEADER).is_some());
    }

    #[ge_context::test]
    async fn it_hides_request_id_on_success(app_ctx: PostgresContext<NoTls>) {
        let req = test::TestRequest::get().uri("/dummy");

        let res = send_test_request(req, app_ctx.clone()).await;

        assert!(res.status().is_success());
        assert!(res.headers().get(REQUEST_ID_HEADER).is_none());
    }
}
