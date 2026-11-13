use crate::api::model::processing_graphs::ProcessingGraphId;
use crate::{
    api::ogc::{
        util::{OgcProtocol, OgcQueryExtractor, ogc_endpoint_url},
        wfs::request::{GetCapabilities, GetFeature},
    },
    config::{self},
    contexts::{ApplicationContext, SessionContext},
    error::{self, Result},
    processing_graphs::{
        processing_graph::ProcessingGraphId as ServicesProcessingGraphId,
        registry::ProcessingGraphRegistry,
    },
    queries::{
        run_query,
        vector::{VectorQuery, features_geojson},
    },
    util::server::{CacheControlHeader, connection_closed},
};
use actix_web::{FromRequest, HttpRequest, HttpResponse, web};
use geoengine_datatypes::{
    collections::{MultiPointCollection, ToGeoJson},
    primitives::{CacheHint, FeatureData, MultiPoint},
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::ProcessingGraphOperatorPath;
use reqwest::Url;
use serde::Deserialize;
use snafu::ensure;
use std::{str::FromStr, time::Duration};
use utoipa::{
    IntoParams, ToSchema,
    openapi::{Ref, Required},
};

pub(crate) fn init_wfs_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/wfs/{processingGraph}").route(web::get().to(wfs_handler::<C>)));
}

#[derive(Debug, Deserialize)]
#[serde(tag = "request", rename_all = "PascalCase")]
#[allow(clippy::large_enum_variant)] // variants may have many fields
pub enum WfsQueryParams {
    GetCapabilities(GetCapabilities),
    GetFeature(GetFeature),
}

/// manual implementation because derive macro does not support enums
/// Note, that WFS is not really OpenAPI compatible, so this is just an approximation to enable client generation
impl IntoParams for WfsQueryParams {
    fn into_params(
        _parameter_in_provider: impl Fn() -> Option<utoipa::openapi::path::ParameterIn>,
    ) -> Vec<utoipa::openapi::path::Parameter> {
        let pip = || Some(utoipa::openapi::path::ParameterIn::Query);

        let mut params = Vec::new();

        params.push(
            utoipa::openapi::path::ParameterBuilder::new()
                .name("request")
                .required(utoipa::openapi::Required::True)
                .parameter_in(pip().unwrap_or_default())
                .description(Some("type of WFS request"))
                .schema(Some(Ref::from_schema_name("WfsRequest")))
                .build(),
        );

        for mut p in GetCapabilities::into_params(pip) {
            p.required = Required::False;
            params.push(p);
        }
        for mut p in GetFeature::into_params(pip) {
            p.required = Required::False;
            params.push(p);
        }

        // remove duplicate parameters
        params.sort_by(|a, b| a.name.cmp(&b.name));
        params.dedup_by(|a, b| a.name == b.name);

        params
    }
}

/// OGC WFS endpoint
#[utoipa::path(
    tag = "OGC WFS",
    get,
    path = "/wfs/{processingGraph}",
    responses(
        (status = 200, description = "OK", content_type = "text/xml", body = String,
            // TODO: add example when utoipa supports more than just json examples
            // example = r#"
            // <?xml version="1.0" encoding="UTF-8"?>
            // <wfs:WFS_Capabilities version="2.0.0"
            // xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            // xmlns="http://www.opengis.net/wfs/2.0"
            // xmlns:wfs="http://www.opengis.net/wfs/2.0"
            // xmlns:ows="http://www.opengis.net/ows/1.1"
            // xmlns:xlink="http://www.w3.org/1999/xlink"
            // xmlns:xs="http://www.w3.org/2001/XMLSchema" xsi:schemaLocation="http://www.opengis.net/wfs/2.0 http://schemas.opengis.net/wfs/2.0/wfs.xsd"
            // xmlns:xml="http://www.w3.org/XML/1998/namespace">
            // <ows:ServiceIdentification>
            //   <ows:Title>Geo Engine</ows:Title>
            //   <ows:ServiceType>WFS</ows:ServiceType>
            //   <ows:ServiceTypeVersion>2.0.0</ows:ServiceTypeVersion>
            //   <ows:Fees>NONE</ows:Fees>
            //   <ows:AccessConstraints>NONE</ows:AccessConstraints>
            // </ows:ServiceIdentification>
            // <ows:ServiceProvider>
            //   <ows:ProviderName>Geo Engine</ows:ProviderName>
            //   <ows:ServiceContact>
            //     <ows:ContactInfo>
            //       <ows:Address>
            //         <ows:ElectronicMailAddress>info@geoengine.de</ows:ElectronicMailAddress>
            //       </ows:Address>
            //     </ows:ContactInfo>
            //   </ows:ServiceContact>
            // </ows:ServiceProvider>
            // <ows:OperationsMetadata>
            //   <ows:Operation name="GetCapabilities">
            //     <ows:DCP>
            //       <ows:HTTP>
            //         <ows:Get xlink:href="http://127.0.0.1:3030/api/wfs/93d6785e-5eea-4e0e-8074-e7f78733d988"/>
            //         <ows:Post xlink:href="http://127.0.0.1:3030/api/wfs/93d6785e-5eea-4e0e-8074-e7f78733d988"/>
            //       </ows:HTTP>
            //     </ows:DCP>
            //     <ows:Parameter name="AcceptVersions">
            //       <ows:AllowedValues>
            //         <ows:Value>2.0.0</ows:Value>
            //       </ows:AllowedValues>
            //     </ows:Parameter>
            //     <ows:Parameter name="AcceptFormats">
            //       <ows:AllowedValues>
            //         <ows:Value>text/xml</ows:Value>
            //       </ows:AllowedValues>
            //     </ows:Parameter>
            //   </ows:Operation>
            //   <ows:Operation name="GetFeature">
            //     <ows:DCP>
            //       <ows:HTTP>
            //         <ows:Get xlink:href="http://127.0.0.1:3030/api/wfs/93d6785e-5eea-4e0e-8074-e7f78733d988"/>
            //         <ows:Post xlink:href="http://127.0.0.1:3030/api/wfs/93d6785e-5eea-4e0e-8074-e7f78733d988"/>
            //       </ows:HTTP>
            //     </ows:DCP>
            //     <ows:Parameter name="resultType">
            //       <ows:AllowedValues>
            //         <ows:Value>results</ows:Value>
            //         <ows:Value>hits</ows:Value>
            //       </ows:AllowedValues>
            //     </ows:Parameter>
            //     <ows:Parameter name="outputFormat">
            //       <ows:AllowedValues>
            //         <ows:Value>application/json</ows:Value>
            //         <ows:Value>json</ows:Value>
            //       </ows:AllowedValues>
            //     </ows:Parameter>
            //     <ows:Constraint name="PagingIsTransactionSafe">
            //       <ows:NoValues/>
            //       <ows:DefaultValue>FALSE</ows:DefaultValue>
            //     </ows:Constraint>
            //   </ows:Operation>
            //   <ows:Constraint name="ImplementsBasicWFS">
            //     <ows:NoValues/>
            //     <ows:DefaultValue>TRUE</ows:DefaultValue>
            //   </ows:Constraint>
            // </ows:OperationsMetadata>
            // <FeatureTypeList>
            //   <FeatureType>
            //     <Name>93d6785e-5eea-4e0e-8074-e7f78733d988</Name>
            //     <Title>Wofklow 93d6785e-5eea-4e0e-8074-e7f78733d988</Title>
            //     <DefaultCRS>urn:ogc:def:crs:EPSG::4326</DefaultCRS>
            //     <ows:WGS84BoundingBox>
            //       <ows:LowerCorner>-90 -180</ows:LowerCorner>
            //       <ows:UpperCorner>90 180</ows:UpperCorner>
            //     </ows:WGS84BoundingBox>
            //   </FeatureType>
            // </FeatureTypeList>
            // </wfs:WFS_Capabilities>"#
        ),
        (status = 200, description = "OK", body = GeoJson,
        example = json!(
        {
            "type": "FeatureCollection",
            "features": [
                {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                    0.0,
                    0.1
                    ]
                },
                "properties": {
                    "foo": 0
                },
                "when": {
                    "start": "1970-01-01T00:00:00+00:00",
                    "end": "1970-01-01T00:00:00.001+00:00",
                    "type": "Interval"
                }
                },
                {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                    1.0,
                    1.1
                    ]
                },
                "properties": {
                    "foo": null
                },
                "when": {
                    "start": "1970-01-01T00:00:00+00:00",
                    "end": "1970-01-01T00:00:00.001+00:00",
                    "type": "Interval"
                }
                },
                {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                    2.0,
                    3.1
                    ]
                },
                "properties": {
                    "foo": 2
                },
                "when": {
                    "start": "1970-01-01T00:00:00+00:00",
                    "end": "1970-01-01T00:00:00.001+00:00",
                    "type": "Interval"
                }
                },
                {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                    3.0,
                    3.1
                    ]
                },
                "properties": {
                    "foo": 3
                },
                "when": {
                    "start": "1970-01-01T00:00:00+00:00",
                    "end": "1970-01-01T00:00:00.001+00:00",
                    "type": "Interval"
                }
                },
                {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                    4.0,
                    4.1
                    ]
                },
                "properties": {
                    "foo": 4
                },
                "when": {
                    "start": "1970-01-01T00:00:00+00:00",
                    "end": "1970-01-01T00:00:00.001+00:00",
                    "type": "Interval"
                }
                }
            ]
        }
        )),
    ),
    params(
        ("processingGraph" = ProcessingGraphId, description = "Processing graph id"),
        WfsQueryParams
    ),
    security(
        ("session_token" = [])
    )
)]

async fn wfs_handler<C>(
    req: HttpRequest,
    processing_graph: web::Path<ProcessingGraphId>,
    request: OgcQueryExtractor<WfsQueryParams>, // case insensitive query params
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse>
where
    C: ApplicationContext,
{
    let request = request.into_inner();

    match request {
        WfsQueryParams::GetCapabilities(r) => {
            wfs_get_capabilities(processing_graph.into_inner().into(), r, app_ctx, session).await
        }
        WfsQueryParams::GetFeature(r) => {
            wfs_get_feature(
                req,
                processing_graph.into_inner().into(),
                r,
                app_ctx,
                session,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_lines)] // long xml body
async fn wfs_get_capabilities<C>(
    processing_graph_id: ServicesProcessingGraphId,
    // TODO: react on capabilities
    _request: GetCapabilities,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse>
where
    C: ApplicationContext,
{
    let wfs_url = wfs_url(processing_graph_id)?;

    let ctx = app_ctx.session_context(session);

    let processing_graph = ctx.db().load_processing_graph(&processing_graph_id).await?;

    let exe_ctx = ctx.execution_context()?;
    let processing_graph_operator_path_root = ProcessingGraphOperatorPath::initialize_root();

    let operator = processing_graph
        .operator
        .get_vector()?
        .initialize(processing_graph_operator_path_root, &exe_ctx)
        .await?;

    let result_descriptor = operator.result_descriptor();

    let spatial_reference: Option<SpatialReference> = result_descriptor.spatial_reference.into();
    let spatial_reference = spatial_reference.ok_or(error::Error::MissingSpatialReference)?;

    let response = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<wfs:WFS_Capabilities version="2.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns="http://www.opengis.net/wfs/2.0"
    xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:ows="http://www.opengis.net/ows/1.1"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xs="http://www.w3.org/2001/XMLSchema" xsi:schemaLocation="http://www.opengis.net/wfs/2.0 http://schemas.opengis.net/wfs/2.0/wfs.xsd"
    xmlns:xml="http://www.w3.org/XML/1998/namespace">
    <ows:ServiceIdentification>
        <ows:Title>Geo Engine</ows:Title>     
        <ows:ServiceType>WFS</ows:ServiceType>
        <ows:ServiceTypeVersion>2.0.0</ows:ServiceTypeVersion>
        <ows:Fees>NONE</ows:Fees>
        <ows:AccessConstraints>NONE</ows:AccessConstraints>
    </ows:ServiceIdentification>
    <ows:ServiceProvider>
        <ows:ProviderName>Geo Engine</ows:ProviderName>    
		<ows:ServiceContact>
            <ows:ContactInfo>
                <ows:Address>
                    <ows:ElectronicMailAddress>info@geoengine.de</ows:ElectronicMailAddress>
                </ows:Address>
            </ows:ContactInfo>
        </ows:ServiceContact>		
    </ows:ServiceProvider>
    <ows:OperationsMetadata>
        <ows:Operation name="GetCapabilities">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="{wfs_url}"/>
                    <ows:Post xlink:href="{wfs_url}"/>
                </ows:HTTP>
            </ows:DCP>
            <ows:Parameter name="AcceptVersions">
                <ows:AllowedValues>
                    <ows:Value>2.0.0</ows:Value>
                </ows:AllowedValues>
            </ows:Parameter>
            <ows:Parameter name="AcceptFormats">
                <ows:AllowedValues>
                    <ows:Value>text/xml</ows:Value>
                </ows:AllowedValues>
            </ows:Parameter>
        </ows:Operation>  
        <ows:Operation name="GetFeature">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="{wfs_url}"/>
                    <ows:Post xlink:href="{wfs_url}"/>
                </ows:HTTP>
            </ows:DCP>
            <ows:Parameter name="resultType">
                <ows:AllowedValues>
                    <ows:Value>results</ows:Value>
                    <ows:Value>hits</ows:Value>
                </ows:AllowedValues>
            </ows:Parameter>
            <ows:Parameter name="outputFormat">
                <ows:AllowedValues>
                    <ows:Value>application/json</ows:Value>
                    <ows:Value>json</ows:Value>
                </ows:AllowedValues>
            </ows:Parameter>
            <ows:Constraint name="PagingIsTransactionSafe">
                <ows:NoValues/>
                <ows:DefaultValue>FALSE</ows:DefaultValue>
            </ows:Constraint>
        </ows:Operation>     
        <ows:Constraint name="ImplementsBasicWFS">
            <ows:NoValues/>
            <ows:DefaultValue>TRUE</ows:DefaultValue>
        </ows:Constraint>     
    </ows:OperationsMetadata>
    <FeatureTypeList>
        <FeatureType>
            <Name>{processing_graph}</Name>
            <Title>Processing Graph {processing_graph}</Title>
            <DefaultCRS>urn:ogc:def:crs:{srs_authority}::{srs_code}</DefaultCRS>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-90 -180</ows:LowerCorner>
                <ows:UpperCorner>90 180</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </FeatureType>       
    </FeatureTypeList>
</wfs:WFS_Capabilities>"#,
        wfs_url = wfs_url,
        processing_graph = processing_graph_id,
        srs_authority = spatial_reference.authority(),
        srs_code = spatial_reference.code(),
    );

    Ok(HttpResponse::Ok()
        .content_type(mime::TEXT_HTML_UTF_8)
        .body(response))
}

fn wfs_url(processing_graph: ServicesProcessingGraphId) -> Result<Url> {
    let web_config = crate::config::get_config_element::<crate::config::Web>()?;
    let base = web_config.api_url()?;

    ogc_endpoint_url(&base, OgcProtocol::Wfs, processing_graph)
}

async fn wfs_get_feature<C: ApplicationContext>(
    req: HttpRequest,
    endpoint: ServicesProcessingGraphId,
    request: GetFeature,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    let type_names = match request.type_names.namespace.as_deref() {
        None => ServicesProcessingGraphId::from_str(&request.type_names.feature_type)?,
        Some(_) => {
            return Err(error::Error::InvalidNamespace);
        }
    };

    ensure!(
        endpoint == type_names,
        error::WFSEndpointTypeNamesMissmatch {
            endpoint,
            type_names
        }
    );

    // TODO: validate request further

    if request.type_names.feature_type == "93d6785e-5eea-4e0e-8074-e7f78733d988" {
        return get_feature_mock(&request);
    }

    let conn_closed = connection_closed(
        &req,
        config::get_config_element::<config::Wfs>()?
            .request_timeout_seconds
            .map(Duration::from_secs),
    );

    let query = VectorQuery {
        processing_graph: type_names,
        // TODO: use a default spatial reference if it is not set?
        crs: request
            .srs_name
            .ok_or(error::Error::InvalidSpatialReference)?,
        bounds: request.bbox.bounds_naive()?,
        time: request
            .time
            .map_or_else(config::default_time_from_config::<config::Wfs>, Into::into),
    };

    let ctx = app_ctx.session_context(session);
    let (json, cache_hint, computation_id) =
        run_query(&ctx, conn_closed, features_geojson(ctx.clone(), query)).await?;

    Ok(HttpResponse::Ok()
        .append_header(cache_hint.cache_control_header())
        .append_header(computation_id)
        .json(json))
}

// Define GeoJson types purely for modelling the output of the WFS handler for OpenAPI
#[derive(Debug, Deserialize, ToSchema)]
pub struct GeoJson {
    #[serde(rename = "type")]
    pub collection_type: CollectionType,
    pub features: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub enum CollectionType {
    FeatureCollection,
}

fn get_feature_mock(_request: &GetFeature) -> Result<HttpResponse> {
    let collection = MultiPointCollection::from_data(
        MultiPoint::many(vec![
            (0.0, 0.1),
            (1.0, 1.1),
            (2.0, 3.1),
            (3.0, 3.1),
            (4.0, 4.1),
        ])?,
        vec![geoengine_datatypes::primitives::TimeInterval::new_unchecked(0, 1); 5],
        [(
            "foo".to_string(),
            FeatureData::NullableInt(vec![Some(0), None, Some(2), Some(3), Some(4)]),
        )]
        .iter()
        .cloned()
        .collect(),
        CacheHint::no_cache(),
    )?;

    Ok(HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .body(collection.to_geo_json()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::model::{
            processing_graphs::{
                MockPointSource, MockPointSourceParameters, SpatialBoundsDerive, TypedOperator,
                VectorOperator,
            },
            responses::ErrorResponse,
        },
        contexts::{PostgresContext, Session},
        datasets::DatasetIdAndName,
        ge_context,
        processing_graphs::processing_graph::ProcessingGraph as ServicesProcessingGraph,
        users::UserAuth,
        util::tests::{
            add_pro_file_definition_to_datasets_as_admin, check_allowed_http_methods,
            read_body_string, send_test_request,
        },
    };
    use actix_web::{
        dev::ServiceResponse,
        http::{Method, header},
        test,
    };
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::raster::{TileSize, TilingSpecification};
    use geoengine_datatypes::test_data;
    use geoengine_operators::engine::{
        TypedOperator as OperatorsTypedOperator, VectorOperator as _,
    };
    use geoengine_operators::source::CsvSourceParameters;
    use geoengine_operators::source::{CsvGeometrySpecification, CsvSource, CsvTimeSpecification};
    use serde_json::json;
    use std::io::{Seek, SeekFrom, Write};
    use tokio_postgres::NoTls;
    use xml::ParserConfig;

    #[ge_context::test]
    async fn mock_test(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let req = test::TestRequest::get()
            .uri("/wfs/93d6785e-5eea-4e0e-8074-e7f78733d988?request=GetFeature&service=WFS&version=2.0.0&typeNames=93d6785e-5eea-4e0e-8074-e7f78733d988&bbox=1,2,3,4")
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            read_body_string(res).await,
            json!({
                "type": "FeatureCollection",
                "features": [{
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [0.0, 0.1]
                        },
                        "properties": {
                            "foo": 0
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [1.0, 1.1]
                        },
                        "properties": {
                            "foo": null
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    }, {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [2.0, 3.1]
                        },
                        "properties": {
                            "foo": 2
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    }, {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [3.0, 3.1]
                        },
                        "properties": {
                            "foo": 3
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    }, {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [4.0, 4.1]
                        },
                        "properties": {
                            "foo": 4
                        },
                        "when": {
                            "start": "1970-01-01T00:00:00+00:00",
                            "end": "1970-01-01T00:00:00.001+00:00",
                            "type": "Interval"
                        }
                    }
                ]
            })
            .to_string()
        );
    }

    async fn get_capabilities_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let processing_graph = ServicesProcessingGraph {
            operator: TypedOperator::Vector(VectorOperator::MockPointSource(MockPointSource {
                r#type: Default::default(),
                params: MockPointSourceParameters {
                    points: vec![(0.0, 0.1).into(), (1.0, 1.1).into()],
                    spatial_bounds: SpatialBoundsDerive::None(Default::default()),
                },
            }))
            .try_into()
            .unwrap(),
        };

        let processing_graph_id = ctx
            .db()
            .register_processing_graph(processing_graph)
            .await
            .unwrap();

        let req = test::TestRequest::with_uri(&format!(
            "/wfs/{processing_graph_id}?request=GetCapabilities&service=WFS"
        ))
        .method(method)
        .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn get_capabilities(app_ctx: PostgresContext<NoTls>) {
        let res = get_capabilities_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);

        // TODO: validate against schema
        let body = test::read_body(res).await;
        let reader = ParserConfig::default().create_reader(body.as_ref());

        for event in reader {
            assert!(event.is_ok());
        }
    }

    #[ge_context::test]
    async fn get_capabilities_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| get_capabilities_test_helper(app_ctx.clone(), method),
            &[Method::GET],
        )
        .await;
    }

    async fn get_feature_registry_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        write!(
            temp_file,
            "
x;y
0;1
2;3
4;5
"
        )
        .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let processing_graph = ServicesProcessingGraph {
            operator: OperatorsTypedOperator::Vector(
                CsvSource {
                    params: CsvSourceParameters {
                        file_path: temp_file.path().into(),
                        field_separator: ';',
                        geometry: CsvGeometrySpecification::XY {
                            x: "x".to_string(),
                            y: "y".to_string(),
                        },
                        time: CsvTimeSpecification::None,
                    },
                }
                .typed(),
            ),
        };

        let id = ctx
            .db()
            .register_processing_graph(processing_graph.clone())
            .await
            .unwrap();

        let req = test::TestRequest::with_uri(&format!("/wfs/{id}?request=GetFeature&service=WFS&version=2.0.0&typeNames={id}&bbox=-90,-180,90,180&srsName=EPSG:4326", id = id.to_string())).method(method).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn get_feature_registry(app_ctx: PostgresContext<NoTls>) {
        let res = get_feature_registry_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            read_body_string(res).await,
            json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 1.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262143-01-01T00:00:00+00:00",
                        "end": "+262142-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [2.0, 3.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262143-01-01T00:00:00+00:00",
                        "end": "+262142-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [4.0, 5.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262143-01-01T00:00:00+00:00",
                        "end": "+262142-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
    }

    #[ge_context::test]
    async fn get_feature_registry_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| get_feature_registry_test_helper(app_ctx.clone(), method),
            &[Method::GET],
        )
        .await;
    }

    #[ge_context::test]
    async fn get_feature_registry_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let req = test::TestRequest::get().uri(
            "/wfs/93d6785e-5eea-4e0e-8074-e7f78733d988?request=GetFeature&service=WFS&version=2.0.0&bbox=-90,-180,90,180&crs=EPSG:4326",
        ).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: missing field `typeNames`",
        )
        .await;
    }

    async fn get_feature_json_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();
        write!(
            temp_file,
            "
x;y
0;1
2;3
4;5
"
        )
        .unwrap();
        temp_file.seek(SeekFrom::Start(0)).unwrap();

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let processing_graph = ServicesProcessingGraph {
            operator: OperatorsTypedOperator::Vector(
                CsvSource {
                    params: CsvSourceParameters {
                        file_path: temp_file.path().into(),
                        field_separator: ';',
                        geometry: CsvGeometrySpecification::XY {
                            x: "x".to_string(),
                            y: "y".to_string(),
                        },
                        time: CsvTimeSpecification::None,
                    },
                }
                .typed(),
            ),
        };

        let processing_graph_id = ctx
            .db()
            .register_processing_graph(processing_graph)
            .await
            .unwrap();

        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("typeNames", &processing_graph_id.to_string()),
            ("bbox", "-90,-180,90,180"),
            ("srsName", "EPSG:4326"),
        ];
        let req = test::TestRequest::with_uri(&format!(
            "/wfs/{}?{}",
            processing_graph_id,
            serde_urlencoded::to_string(params).unwrap()
        ))
        .method(method)
        .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn get_feature_json(app_ctx: PostgresContext<NoTls>) {
        let res = get_feature_json_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            read_body_string(res).await,
            json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [0.0, 1.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262143-01-01T00:00:00+00:00",
                        "end": "+262142-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [2.0, 3.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262143-01-01T00:00:00+00:00",
                        "end": "+262142-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }, {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [4.0, 5.0]
                    },
                    "properties": {},
                    "when": {
                        "start": "-262143-01-01T00:00:00+00:00",
                        "end": "+262142-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
    }

    #[ge_context::test]
    async fn get_feature_json_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| get_feature_json_test_helper(app_ctx.clone(), method),
            &[Method::GET],
        )
        .await;
    }

    #[ge_context::test]
    async fn get_feature_json_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("bbox", "-90,-180,90,180"),
            ("crs", "EPSG:4326"),
        ];
        let req = test::TestRequest::get()
            .uri(&format!(
                "/wfs/93d6785e-5eea-4e0e-8074-e7f78733d988?{}",
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(
            res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: missing field `typeNames`",
        )
        .await;
    }

    /// override the pixel size since this test was designed for 600 x 600 pixel tiles
    fn raster_vector_join_tiling_spec() -> TilingSpecification {
        TilingSpecification {
            tile_size: TileSize::new_unchecked(600, 600),
        }
    }

    #[ge_context::test(tiling_spec = "raster_vector_join_tiling_spec")]
    #[allow(clippy::too_many_lines)]
    async fn raster_vector_join(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let DatasetIdAndName {
            id: _,
            name: ndvi_name,
        } = add_pro_file_definition_to_datasets_as_admin(
            &app_ctx,
            test_data!("dataset_defs/ndvi.json"),
        )
        .await;
        let DatasetIdAndName {
            id: _,
            name: points_with_time_name,
        } = add_pro_file_definition_to_datasets_as_admin(
            &app_ctx,
            test_data!("dataset_defs/points_with_time.json"),
        )
        .await;

        let processing_graph = serde_json::json!({
            "type": "Vector",
            "operator": {
                "type": "RasterVectorJoin",
                "params": {
                    "names": {
                        "type": "names",
                        "values": ["NDVI"]
                    },
                    "featureAggregation": "first",
                    "temporalAggregation": "first"
                },
                "sources": {
                    "vector": {
                        "type": "OgrSource",
                        "params": {
                            "data": points_with_time_name,
                            "attributeProjection": null
                        }
                    },
                    "rasters": [{
                        "type": "GdalSource",
                        "params": {

                            "data": ndvi_name,
                            "overviewLevel:": null

                        }
                    }],
                }
            }
        });

        let json = serde_json::to_string(&processing_graph).unwrap();

        let processing_graph = serde_json::from_str(&json).unwrap();

        let processing_graph_id = ctx
            .db()
            .register_processing_graph(processing_graph)
            .await
            .unwrap();

        let params = &[
            ("request", "GetFeature"),
            ("service", "WFS"),
            ("version", "2.0.0"),
            ("typeNames", &processing_graph_id.to_string()),
            ("bbox", "-90,-180,90,180"),
            ("srsName", "EPSG:4326"),
            ("time", "2014-04-01T12:00:00.000Z/2014-04-01T12:00:00.000Z"),
        ];
        let req = test::TestRequest::get()
            .uri(&format!(
                "/wfs/{}?{}",
                processing_graph_id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        let body: serde_json::Value = test::read_body_json(res).await;

        assert_eq!(
            body,
            serde_json::json!({
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [12.843_159, 47.825_724]
                    },
                    "properties": {
                        "NDVI": 228
                    },
                    "when": {
                        "start": "2014-04-01T00:00:00+00:00",
                        "end": "2014-07-01T00:00:00+00:00",
                        "type": "Interval"
                    }
                }]
            })
        );
    }

    #[ge_context::test]
    async fn it_sets_cache_control_header(app_ctx: PostgresContext<NoTls>) {
        let res = get_feature_json_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache"
        );
    }

    #[ge_context::test]
    async fn it_sets_computation_id_header(app_ctx: PostgresContext<NoTls>) {
        let res = get_feature_json_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);
        assert!(res.headers().get("x-computation-id").is_some());
    }
}
