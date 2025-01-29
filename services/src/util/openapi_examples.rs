use crate::api::model::responses::ErrorResponse;
use crate::contexts::PostgresContext;
use crate::contexts::SessionId;
use crate::users::UserAuth;
use actix_web::dev::ServiceResponse;
use actix_web::http::{header, Method};
use actix_web::test::TestRequest;
use actix_web_httpauth::headers::authorization::Bearer;
use std::collections::HashMap;
use std::future::Future;
use tokio_postgres::NoTls;
use utoipa::openapi::path::{Parameter, ParameterIn};
use utoipa::openapi::{
    Components, KnownFormat, OpenApi, PathItemType, RefOr, Schema, SchemaFormat, SchemaType,
};
use uuid::Uuid;

pub struct RunnableExample<'a, C, F, Fut>
where
    F: Fn(TestRequest, C) -> Fut,
    Fut: Future<Output = ServiceResponse>,
{
    pub(crate) components: &'a Components,
    pub(crate) http_method: &'a PathItemType,
    pub(crate) uri: &'a str,
    pub(crate) parameters: &'a Option<Vec<Parameter>>,
    pub(crate) body: serde_json::Value,
    pub(crate) with_auth: bool,
    pub(crate) ctx: C,
    pub(crate) session_id: SessionId,
    pub(crate) send_test_request: &'a F,
}

impl<'a, C, F, Fut> RunnableExample<'a, C, F, Fut>
where
    F: Fn(TestRequest, C) -> Fut,
    Fut: Future<Output = ServiceResponse>,
{
    fn get_actix_http_method(&self) -> Method {
        match self.http_method {
            PathItemType::Get => Method::GET,
            PathItemType::Post => Method::POST,
            PathItemType::Put => Method::PUT,
            PathItemType::Delete => Method::DELETE,
            PathItemType::Options => Method::OPTIONS,
            PathItemType::Head => Method::HEAD,
            PathItemType::Patch => Method::PATCH,
            PathItemType::Trace => Method::TRACE,
            PathItemType::Connect => Method::CONNECT,
        }
    }

    #[allow(clippy::unimplemented)]
    fn get_default_parameter_value(schema: &Schema) -> String {
        match schema {
            Schema::Object(obj) => match obj.schema_type {
                SchemaType::Type(Type::String) => match &obj.format {
                    Some(SchemaFormat::KnownFormat(format)) => match format {
                        KnownFormat::Uuid => Uuid::new_v4().to_string(),
                        _ => unimplemented!(),
                    },
                    None => "asdf".to_string(),
                    _ => unimplemented!(),
                },
                SchemaType::Integer | SchemaType::Number => "42".to_string(),
                SchemaType::Boolean => "false".to_string(),
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        }
    }

    fn resolve_schema(&'a self, ref_or: &'a RefOr<Schema>) -> &'a Schema {
        match ref_or {
            RefOr::Ref(reference) => {
                const SCHEMA_REF_PREFIX_LEN: usize = "#/components/schemas/".len();
                //can_resolve_reference(reference, self.components); checked in can_resolve_api
                let schema_name = &reference.ref_location[SCHEMA_REF_PREFIX_LEN..];
                self.resolve_schema(
                    self.components
                        .schemas
                        .get(schema_name)
                        .expect("checked before"),
                )
            }
            RefOr::T(concrete) => concrete,
        }
    }

    fn insert_parameters(&self, parameters: &Vec<Parameter>) -> TestRequest {
        let mut req = TestRequest::default();
        let mut uri = self.uri.to_string();
        let mut query_params = HashMap::new();
        let mut cookies = HashMap::new();

        for parameter in parameters {
            let schema = self.resolve_schema(
                parameter
                    .schema
                    .as_ref()
                    .expect("utoipa adds schema everytime"),
            );
            let value = Self::get_default_parameter_value(schema);

            match parameter.parameter_in {
                ParameterIn::Query => {
                    query_params.insert(parameter.name.as_str(), value);
                }
                ParameterIn::Path => {
                    uri = uri.replace(&format!("{{{}}}", parameter.name), value.as_str());
                }
                ParameterIn::Header => {
                    req = req.append_header((parameter.name.as_str(), value));
                }
                ParameterIn::Cookie => {
                    cookies.insert(parameter.name.as_str(), value);
                }
            }
        }
        if let Ok(cookie_str) = serde_urlencoded::to_string(cookies) {
            if !cookie_str.is_empty() {
                req = req.append_header((header::COOKIE, cookie_str.replace('&', "; ")));
            }
        }
        if let Ok(query_params_str) = serde_urlencoded::to_string(query_params) {
            if !query_params_str.is_empty() {
                uri = format!("{uri}?{query_params_str}");
            }
        }
        req.uri(uri.as_str())
    }

    fn build_request(&self) -> TestRequest {
        let http_method = self.get_actix_http_method();
        let mut req;

        if let Some(parameters) = self.parameters {
            req = self.insert_parameters(parameters);
        } else {
            req = TestRequest::default().uri(self.uri);
        }
        req = req.method(http_method);

        if self.with_auth {
            req = req.append_header((
                header::AUTHORIZATION,
                Bearer::new(self.session_id.to_string()),
            ));
        }
        req.append_header((header::CONTENT_TYPE, "application/json"))
            .set_json(&self.body)
    }

    async fn run(self) -> ServiceResponse {
        let req = self.build_request();
        (self.send_test_request)(req, self.ctx).await
    }

    /// # Panics
    /// Will panic if an example cannot be run due to incomplete or
    /// outdated OpenAPI documentation.
    pub(crate) async fn check_for_bad_documentation(self) {
        let res = self.run().await;

        if res.status() != 200 {
            let method = res.request().head().method.to_string();
            let path = res.request().path().to_string();
            let body: ErrorResponse = actix_web::test::read_body_json(res).await;

            match body.error.as_str() {
                "NotFound" | "MethodNotAllowed" => panic!(
                    "The handler of the example at {method} {path} wasn't reachable. \
                    Check if the http method and path parameters are correctly set in the documentation."
                ),
                "UnableToParseQueryString" => panic!(
                    "The example at {method} {path} threw an UnableToParseQueryString error. \
                    Check if the query parameters are correctly set in the documentation."
                ),
                "BodyDeserializeError" => panic!(
                    "The example at {method} {path} threw an BodyDeserializeError. \
                    Check if there were schema changes and update the request body accordingly."
                ),
                _ => {}
            }
        }
    }
}

/// Runs all example requests against the provided test server to check for bad documentation,
/// for example due to incompatible schema changes between the time of writing the request body
/// and now. It can also detect if the query parameters are not documented correctly or the
/// request path changed.
///
/// # Panics
///
/// panics if the creation of an anonymous session fails or the example contains a Ref which is not yet supported.
pub async fn can_run_examples<F, Fut>(
    app_ctx: PostgresContext<NoTls>,
    api: OpenApi,
    send_test_request: F,
) where
    F: Fn(TestRequest, PostgresContext<NoTls>) -> Fut
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
                            session_id: app_ctx
                                .create_anonymous_session()
                                .await
                                .expect("creating an anonymous session should always work")
                                .id,
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
                                    panic!("checking pro examples with references is not yet implemented")
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
                                                .expect("creating an anonymous session should always work")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::model::services::Volume;
    use crate::ge_context;
    use crate::util::server::{configure_extractors, render_404, render_405};
    use actix_web::{http, middleware, post, web, App, HttpResponse, Responder};
    use serde::Deserialize;
    use serde_json::json;
    use utoipa::openapi::path::{OperationBuilder, ParameterBuilder, PathItemBuilder};
    use utoipa::openapi::request_body::RequestBodyBuilder;
    use utoipa::openapi::{
        ComponentsBuilder, ContentBuilder, Object, ObjectBuilder, OpenApiBuilder, PathItemType,
        PathsBuilder,
    };
    use utoipa::ToSchema;

    #[derive(Deserialize)]
    struct DummyQueryParams {
        #[serde(rename = "x")]
        _x: String,
    }

    #[post("/test/{id}")]
    #[allow(
        clippy::unused_async, // the function signature of request handlers requires it
        clippy::no_effect_underscore_binding // adding path and query parameter to ensure parameter insertion works
    )]
    async fn dummy_handler(
        _id: web::Path<u32>,
        _params: web::Query<DummyQueryParams>,
        _body: web::Json<Volume>,
    ) -> impl Responder {
        HttpResponse::Ok()
    }

    async fn dummy_send_test_request(
        req: TestRequest,
        ctx: PostgresContext<NoTls>,
    ) -> ServiceResponse {
        let app = actix_web::test::init_service(
            App::new()
                .app_data(web::Data::new(ctx))
                .wrap(
                    middleware::ErrorHandlers::default()
                        .handler(http::StatusCode::NOT_FOUND, render_404)
                        .handler(http::StatusCode::METHOD_NOT_ALLOWED, render_405),
                )
                .configure(configure_extractors)
                .service(dummy_handler),
        )
        .await;
        actix_web::test::call_service(&app, req.to_request())
            .await
            .map_into_boxed_body()
    }

    async fn run_dummy_example(app_ctx: PostgresContext<NoTls>, example: serde_json::Value) {
        can_run_examples(
            app_ctx,
            OpenApiBuilder::new()
                .paths(
                    PathsBuilder::new().path(
                        "/test/{id}",
                        PathItemBuilder::new()
                            .operation(
                                PathItemType::Post,
                                OperationBuilder::new()
                                    .parameter(
                                        ParameterBuilder::new()
                                            .name("id")
                                            .parameter_in(ParameterIn::Path)
                                            .schema(Some(RefOr::T(
                                                ObjectBuilder::new()
                                                    .schema_type(SchemaType::Integer)
                                                    .format(Some(SchemaFormat::KnownFormat(
                                                        KnownFormat::Int32,
                                                    )))
                                                    .into(),
                                            ))),
                                    )
                                    .parameter(
                                        ParameterBuilder::new()
                                            .name("x")
                                            .parameter_in(ParameterIn::Query)
                                            .schema(Some(RefOr::T(
                                                Object::with_type(SchemaType::Type(Type::String))
                                                    .into(),
                                            ))),
                                    )
                                    .request_body(Some(
                                        RequestBodyBuilder::new()
                                            .content(
                                                "application/json",
                                                ContentBuilder::new()
                                                    .schema(Volume::schema().1)
                                                    .example(Some(example))
                                                    .into(),
                                            )
                                            .into(),
                                    )),
                            )
                            .into(),
                    ),
                )
                .components(Some(
                    ComponentsBuilder::new()
                        .schemas_from_iter([
                            ("Schema1", Schema::default()),
                            ("Schema2", Schema::default()),
                            ("Schema3", Schema::default()),
                        ])
                        .into(),
                ))
                .into(),
            dummy_send_test_request,
        )
        .await;
    }

    #[ge_context::test(expect_panic = "BodyDeserializeError")]
    async fn detects_bodydeserializeerror(app_ctx: PostgresContext<NoTls>) {
        run_dummy_example(app_ctx, json!({"path": "note-name_field_missing"})).await;
    }

    #[ge_context::test]
    async fn successfull_example_run(app_ctx: PostgresContext<NoTls>) {
        run_dummy_example(app_ctx, json!({"name": "Files", "path": "/path/to/files"})).await;
    }
}
