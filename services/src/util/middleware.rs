use std::future::{Ready, ready};

use actix_web::{
    Error, HttpMessage,
    dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready},
    http::header::{HeaderName, HeaderValue},
};
use futures_util::future::LocalBoxFuture;
use tracing::{Level, event_enabled};
use tracing_actix_web::RequestId;

const REQUEST_ID_HEADER: &str = "x-request-id";

/// Middleware that sends the request id in the `x-request-id` header on failed requests.
/// The header is skipped if actix tracing is deactivated.
pub struct OutputRequestId;

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
        let request_id = req
            .extensions()
            .get::<RequestId>()
            .copied()
            .expect("`RequestId` was attached by `TracingLogger`");

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
    use crate::{contexts::PostgresContext, ge_context, util::tests::send_test_request};
    use actix_web::test;
    use tokio_postgres::NoTls;

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
