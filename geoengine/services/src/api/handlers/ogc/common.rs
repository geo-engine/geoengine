use crate::{
    api::handlers::ogc::{OgcApiResult, internal_server_error, ogc_base_url},
    contexts::ApplicationContext,
    workflows::workflow::WorkflowId,
};
use actix_web::web;
use ogcapi_types::common::{
    Conformance, LandingPage, Link,
    link_rel::{CONFORMANCE, DATA, SELF, TILING_SCHEMES},
    media_type::JSON,
};

/// OGC API Landing Page
///
/// Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/ogc/{workflow}/",
    responses(
        (status = 200, description = "OK", body = LandingPage,
            example = json!({
                "title": "Geo Engine OGC API",
                "description": "Processing System für Geospatial Data",
                "links": [
                    {
                        "href": "http://geoengine.example.com/",
                        "rel": SELF,
                        "type": JSON
                    },
                    {
                        "href": "http://geoengine.example.com/conformance",
                        "rel": CONFORMANCE,
                        "type": JSON
                    },
                    {
                        "href": "http://geoengine.example.com/collections",
                        "rel": DATA,
                        "type": JSON
                    },
                    {
                        "href": "http://geoengine.example.com/tileMatrixSets",
                        "rel": TILING_SCHEMES,
                        "type": JSON
                    }
                ]
            })
        )
    ),
    params(
        ("processingGraphId" = WorkflowId, description = "Processing Graph ID"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn landing_page<C: ApplicationContext>(
    _session: C::Session,
    _app_ctx: web::Data<C>,
    processing_graph_id: web::Path<WorkflowId>,
) -> OgcApiResult<web::Json<LandingPage>> {
    let processing_graph_id = processing_graph_id.into_inner();
    let create_link = link_creator(processing_graph_id);

    Ok(web::Json(LandingPage {
        title: Some(format!("Geo Engine OGC API [{processing_graph_id}]")),
        description: Some(format!(
            "OGC API Landing Page for Geo Engine and processing graph {processing_graph_id}"
        )),
        links: vec![
            create_link("", SELF, JSON)?,
            create_link("conformance", CONFORMANCE, JSON)?,
            create_link("collections", DATA, JSON)?,
            create_link("tileMatrixSets", TILING_SCHEMES, JSON)?,
        ],
        ..LandingPage::default()
    }))
}

/// OGC API Conformance Classes
///
/// Cf. [OGC API - Common - Part 1: Core](https://docs.ogc.org/is/19-072/19-072.html).
#[utoipa::path(
    tag = "OGC API",
    get,
    path = "/ogc/{workflow}/conformance",
    responses(
        (status = 200, description = "OK", body = Conformance,
            example = json!({
                "conformsTo": [
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
                    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
                    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
                    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
                ]
            })
        )
    ),
    params(
        ("processingGraphId" = WorkflowId, description = "Processing Graph ID"),
    ),
    security(
        ("session_token" = [])
    )
)]
pub async fn conformance<C: ApplicationContext>(
    _session: C::Session,
    _app_ctx: web::Data<C>,
    _processing_graph_id: web::Path<WorkflowId>,
) -> web::Json<Conformance> {
    web::Json(Conformance::new(&[
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
        // "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
        // "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
        // "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
        // "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
    ]))
}

fn link_creator(
    processing_graph_id: WorkflowId,
) -> impl Fn(&str, &'static str, &'static str) -> OgcApiResult<Link> {
    move |path: &str, rel: &'static str, mediatype: &'static str| -> OgcApiResult<Link> {
        let base_url = match ogc_base_url(processing_graph_id) {
            Ok(base_url) => base_url,
            Err(error) => {
                tracing::error!(
                    "failed to generate OGC base url for workflow {processing_graph_id}: {error}"
                );
                return Err(internal_server_error());
            }
        };

        let href = match base_url.join(path) {
            Ok(href) => href,
            Err(error) => {
                tracing::error!(
                    "failed to build OGC landing page link '{path}' for workflow {processing_graph_id}: {error}"
                );
                return Err(internal_server_error());
            }
        };

        Ok(Link::new(href.to_string(), rel).mediatype(mediatype))
    }
}

#[cfg(test)]
mod tests {
    use crate::contexts::{
        ApplicationContext, PostgresContext, Session, SessionContext, SessionId,
    };
    use crate::ge_context;
    use crate::users::UserAuth;
    use crate::util::tests::{read_body_json, register_ndvi_workflow_helper, send_test_request};
    use crate::workflows::workflow::WorkflowId;
    use actix_web::{http::header, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use pretty_assertions::assert_eq;
    use tokio_postgres::NoTls;

    async fn session_and_processing_graph_id(
        app_ctx: &PostgresContext<NoTls>,
    ) -> (SessionId, WorkflowId) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();

        let (_, processing_graph_id) = register_ndvi_workflow_helper(app_ctx).await;

        (session_id, processing_graph_id)
    }

    #[ge_context::test]
    async fn it_returns_ogc_landing_page(app_ctx: PostgresContext<NoTls>) {
        let server_url = "http://127.0.0.1:3030";
        let (session_id, processing_graph_id) = session_and_processing_graph_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!("/ogc/{processing_graph_id}/"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
              "title": format!("Geo Engine OGC API [{processing_graph_id}]"),
              "description": format!("OGC API Landing Page for Geo Engine and processing graph {processing_graph_id}"),
              "links": [
                {
                  "href": format!("{server_url}/api/ogc/{processing_graph_id}/"),
                  "rel": "self",
                  "type": "application/json"
                },
                {
                  "href": format!("{server_url}/api/ogc/{processing_graph_id}/conformance"),
                  "rel": "conformance",
                  "type": "application/json"
                },
                {
                  "href": format!("{server_url}/api/ogc/{processing_graph_id}/collections"),
                  "rel": "data",
                  "type": "application/json"
                },
                {
                  "href": format!("{server_url}/api/ogc/{processing_graph_id}/tileMatrixSets"),
                  "rel": "http://www.opengis.net/def/rel/ogc/1.0/tiling-schemes",
                  "type": "application/json"
                }
              ]
            })
        );
    }

    #[ge_context::test]
    async fn it_returns_ogc_conformance_classes(app_ctx: PostgresContext<NoTls>) {
        let (session_id, processing_graph_id) = session_and_processing_graph_id(&app_ctx).await;

        let req = test::TestRequest::get()
            .uri(&format!("/ogc/{processing_graph_id}/conformance"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;
        let status = res.status();
        let body = read_body_json(res).await;

        assert_eq!(status, 200, "{body}");

        assert_eq!(
            body,
            serde_json::json!({
              "conformsTo": [
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
                "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
                // "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
                // "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
                // "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
                // "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
              ]
            })
        );
    }
}
