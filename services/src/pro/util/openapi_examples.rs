use crate::pro::contexts::ProPostgresContext;
use crate::pro::users::UserAuth;
use crate::util::openapi_examples::RunnableExample;
use actix_web::dev::ServiceResponse;
use actix_web::test::TestRequest;
use std::future::Future;
use tokio_postgres::NoTls;
use utoipa::openapi::{OpenApi, RefOr};

/// Runs all example requests against the provided test server to check for bad documentation,
/// for example due to incompatible schema changes between the time of writing the request body
/// and now. It can also detect if the query parameters are not documented correctly or the
/// request path changed.
///
/// # Panics
///
/// panics if the creation of an anonymous session fails.
#[allow(clippy::unimplemented)]
pub async fn can_run_pro_examples<F, Fut>(
    app_ctx: ProPostgresContext<NoTls>,
    api: OpenApi,
    send_test_request: F,
) where
    F: Fn(TestRequest, ProPostgresContext<NoTls>) -> Fut
        + Send
        + std::panic::UnwindSafe
        + 'static
        + Clone,
    Fut: Future<Output = ServiceResponse>,
{
    let components = api.components.expect("api has at least one component");

    for (uri, path_item) in api.paths.paths {
        for (http_method, operation) in path_item.operations {
            if let Some(request_body) = operation.request_body {
                let with_auth = operation.security.is_some();

                for content in request_body.content.into_values() {
                    if let Some(example) = content.example {
                        RunnableExample {
                            components: &components,
                            http_method: &http_method,
                            uri: uri.as_str(),
                            parameters: &operation.parameters,
                            body: example,
                            with_auth,
                            session_id: app_ctx.create_anonymous_session().await.unwrap().id,
                            ctx: app_ctx.clone(),
                            send_test_request: &send_test_request,
                        }
                        .check_for_bad_documentation()
                        .await;
                    } else {
                        for example in content.examples.into_values() {
                            match example {
                                RefOr::Ref(_reference) => {
                                    // This never happened during testing.
                                    // It is undocumented how the references would look like.
                                    unimplemented!()
                                }
                                RefOr::T(concrete) => {
                                    if let Some(body) = concrete.value {
                                        RunnableExample {
                                            components: &components,
                                            http_method: &http_method,
                                            uri: uri.as_str(),
                                            parameters: &operation.parameters,
                                            body,
                                            with_auth,
                                            session_id: app_ctx
                                                .create_anonymous_session()
                                                .await
                                                .unwrap()
                                                .id,
                                            ctx: app_ctx.clone(),
                                            send_test_request: &send_test_request,
                                        }
                                        .check_for_bad_documentation()
                                        .await;
                                    } else {
                                        //skip external examples
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
