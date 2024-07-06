use std::future::{ready, Ready};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform}, http::header::{HeaderName, HeaderValue}, Error, HttpMessage
};
use futures_util::future::LocalBoxFuture;
use tracing::{event_enabled, Level};
use tracing_actix_web::RequestId;

// There are two steps in middleware processing.
// 1. Middleware initialization, middleware factory gets called with
//    next service in chain as parameter.
// 2. Middleware's call method gets called with normal request.
#[derive(Default)]
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
            enabled: event_enabled!(target: "tracing_actix_web::middleware", Level::WARN, request_id),
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
                    HeaderName::from_static("x-request-id"),
                    HeaderValue::from_str(request_id.to_string().as_str())
                        .expect("uuid is always valid"),
                );
            }

            Ok(res)
        })
    }
}
