use actix_web::{web, FromRequest, HttpRequest, HttpResponse};
use futures::future::BoxFuture;
use geoengine_datatypes::primitives::CacheHint;
use geoengine_datatypes::primitives::VectorQueryRectangle;
use geoengine_operators::util::abortable_query_execution;
use geoengine_operators::util::input::RasterOrVectorOperator;
use reqwest::Url;
use serde::Deserialize;
use snafu::{ensure, ResultExt};
use utoipa::openapi::ArrayBuilder;
use utoipa::ToSchema;

use crate::api::model::datatypes::TimeInterval;
use crate::contexts::ApplicationContext;
use crate::error;
use crate::error::Result;
use crate::handlers::SessionContext;
use crate::ogc::util::{ogc_endpoint_url, OgcProtocol, OgcRequestGuard};
use crate::ogc::wfs::request::{GetCapabilities, GetFeature};
use crate::util::config;
use crate::util::config::get_config_element;
use crate::util::server::{connection_closed, not_implemented_handler, CacheControlHeader};
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::{Workflow, WorkflowId};
use futures::StreamExt;
use geoengine_datatypes::collections::ToGeoJson;
use geoengine_datatypes::{
    collections::{FeatureCollection, MultiPointCollection},
    primitives::SpatialResolution,
};
use geoengine_datatypes::{
    primitives::{FeatureData, Geometry, MultiPoint},
    spatial_reference::SpatialReference,
};
use geoengine_operators::engine::{
    CanonicOperatorName, QueryContext, ResultDescriptor, SingleRasterOrVectorSource,
    TypedVectorQueryProcessor, VectorQueryProcessor,
};
use geoengine_operators::engine::{QueryProcessor, WorkflowOperatorPath};
use geoengine_operators::processing::{
    InitializedVectorReprojection, Reprojection, ReprojectionParams,
};
use serde_json::json;
use std::str::FromStr;
use std::time::Duration;

pub(crate) fn init_wfs_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::resource("/wfs/{workflow}")
            .route(
                web::get()
                    .guard(OgcRequestGuard::new("GetCapabilities"))
                    .to(wfs_capabilities_handler::<C>),
            )
            .route(
                web::get()
                    .guard(OgcRequestGuard::new("GetFeature"))
                    .to(wfs_feature_handler::<C>),
            )
            .route(web::get().to(not_implemented_handler)),
    );
}

/// Get WFS Capabilities
#[utoipa::path(
    tag = "OGC WFS",
    get,
    path = "/wfs/{workflow}?request=GetCapabilities",
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
        )
    ),
    params(
        ("workflow" = WorkflowId, description = "Workflow id"),
        GetCapabilities
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::too_many_lines)]
async fn wfs_capabilities_handler<C>(
    workflow_id: web::Path<WorkflowId>,
    _request: web::Query<GetCapabilities>,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse>
where
    C: ApplicationContext,
{
    let workflow_id = workflow_id.into_inner();
    let wfs_url = wfs_url(workflow_id)?;

    let ctx = app_ctx.session_context(session);

    let workflow = ctx.db().load_workflow(&workflow_id).await?;

    let exe_ctx = ctx.execution_context()?;
    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let operator = workflow
        .operator
        .get_vector()
        .context(error::Operator)?
        .initialize(workflow_operator_path_root, &exe_ctx)
        .await
        .context(error::Operator)?;

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
            <Name>{workflow}</Name>
            <Title>Workflow {workflow}</Title>
            <DefaultCRS>urn:ogc:def:crs:{srs_authority}::{srs_code}</DefaultCRS>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-90 -180</ows:LowerCorner>
                <ows:UpperCorner>90 180</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </FeatureType>       
    </FeatureTypeList>
</wfs:WFS_Capabilities>"#,
        wfs_url = wfs_url,
        workflow = workflow_id,
        srs_authority = spatial_reference.authority(),
        srs_code = spatial_reference.code(),
    );

    Ok(HttpResponse::Ok()
        .content_type(mime::TEXT_HTML_UTF_8)
        .body(response))
}

fn wfs_url(workflow: WorkflowId) -> Result<Url> {
    let web_config = crate::util::config::get_config_element::<crate::util::config::Web>()?;
    let base = web_config.api_url()?;

    ogc_endpoint_url(&base, OgcProtocol::Wfs, workflow)
}

/// Get WCS Features
#[utoipa::path(
    tag = "OGC WFS",
    get,
    path = "/wfs/{workflow}?request=GetFeature",
    responses(
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
        ("workflow" = WorkflowId, description = "Workflow id"),
        GetFeature
    ),
    security(
        ("session_token" = [])
    )
)]
async fn wfs_feature_handler<C: ApplicationContext>(
    req: HttpRequest,
    endpoint: web::Path<WorkflowId>,
    request: web::Query<GetFeature>,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    let endpoint = endpoint.into_inner();
    let request = request.into_inner();

    let type_names = match request.type_names.namespace.as_deref() {
        None => WorkflowId::from_str(&request.type_names.feature_type)?,
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

    let ctx = app_ctx.session_context(session);

    let workflow: Workflow = ctx.db().load_workflow(&type_names).await?;

    let operator = workflow.operator.get_vector().context(error::Operator)?;

    let execution_context = ctx.execution_context()?;
    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let initialized = operator
        .clone()
        .initialize(workflow_operator_path_root, &execution_context)
        .await
        .context(error::Operator)?;

    // handle request and workflow crs matching
    let workflow_spatial_ref: Option<SpatialReference> =
        initialized.result_descriptor().spatial_reference().into();
    let workflow_spatial_ref = workflow_spatial_ref.ok_or(error::Error::InvalidSpatialReference)?;

    // TODO: use a default spatial reference if it is not set?
    let request_spatial_ref: SpatialReference = request
        .srs_name
        .ok_or(error::Error::InvalidSpatialReference)?;

    // perform reprojection if necessary
    let initialized = if request_spatial_ref == workflow_spatial_ref {
        initialized
    } else {
        log::debug!(
            "WFS query srs: {}, workflow srs: {} --> injecting reprojection",
            request_spatial_ref,
            workflow_spatial_ref
        );

        let reprojection_params = ReprojectionParams {
            target_spatial_reference: request_spatial_ref,
        };

        // create the reprojection operator in order to get the canonic operator name
        let reprojected_workflow = Reprojection {
            params: reprojection_params,
            sources: SingleRasterOrVectorSource {
                source: RasterOrVectorOperator::Vector(operator),
            },
        };

        // create the inititalized operator directly, to avoid re-initializing everything
        let ivp = InitializedVectorReprojection::try_new_with_input(
            CanonicOperatorName::from(&reprojected_workflow),
            reprojection_params,
            initialized,
        )
        .context(error::Operator)?;

        Box::new(ivp)
    };

    let processor = initialized.query_processor().context(error::Operator)?;

    let query_rect = VectorQueryRectangle::with_bounds_and_resolution(
        request.bbox.bounds_naive()?,
        request.time.unwrap_or_else(default_time_from_config).into(),
        // TODO: find reasonable default
        request
            .query_resolution
            .map_or_else(SpatialResolution::zero_point_one, |r| r.0),
    );
    let query_ctx = ctx.query_context()?;

    let (json, cache_hint) = match processor {
        TypedVectorQueryProcessor::Data(p) => {
            vector_stream_to_geojson(p, query_rect, query_ctx, conn_closed).await
        }
        TypedVectorQueryProcessor::MultiPoint(p) => {
            vector_stream_to_geojson(p, query_rect, query_ctx, conn_closed).await
        }
        TypedVectorQueryProcessor::MultiLineString(p) => {
            vector_stream_to_geojson(p, query_rect, query_ctx, conn_closed).await
        }
        TypedVectorQueryProcessor::MultiPolygon(p) => {
            vector_stream_to_geojson(p, query_rect, query_ctx, conn_closed).await
        }
    }?;

    Ok(HttpResponse::Ok()
        .append_header(cache_hint.cache_control_header())
        .json(json))
}

// Define GeoJson types purely for modelling the output of the WFS handler for OpenAPI
#[derive(Debug, Deserialize, ToSchema)]
pub struct GeoJson {
    #[serde(rename = "type")]
    pub collection_type: CollectionType,
    pub features: Vec<Feature>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub enum CollectionType {
    FeatureCollection,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct Feature {
    #[serde(rename = "type")]
    pub feature_type: FeatureType,
    pub coordinates: Coordinates,
}

#[derive(Debug, Deserialize)]
pub struct Coordinates;

impl<'a> ToSchema<'a> for Coordinates {
    fn schema() -> (&'a str, utoipa::openapi::RefOr<utoipa::openapi::Schema>) {
        ("Coordinates", ArrayBuilder::new().into())
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub enum FeatureType {
    Point,
    MulitPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
}

async fn vector_stream_to_geojson<G, C: QueryContext + 'static>(
    processor: Box<dyn VectorQueryProcessor<VectorType = FeatureCollection<G>>>,
    query_rect: VectorQueryRectangle,
    mut query_ctx: C,
    conn_closed: BoxFuture<'_, ()>,
) -> Result<(serde_json::Value, CacheHint)>
where
    G: Geometry + 'static,
    for<'c> FeatureCollection<G>: ToGeoJson<'c>,
{
    let query_abort_trigger = query_ctx.abort_trigger()?;

    let features: Vec<serde_json::Value> = Vec::new();
    // TODO: more efficient merging of the partial feature collections
    let stream = processor.query(query_rect, &query_ctx).await?;

    let future: BoxFuture<geoengine_operators::util::Result<(Vec<serde_json::Value>, CacheHint)>> =
        Box::pin(stream.fold(
            geoengine_operators::util::Result::<(Vec<serde_json::Value>, CacheHint)>::Ok((
                features,
                CacheHint::max_duration(),
            )),
            |output, collection| async move {
                match (output, collection) {
                    (Ok((mut output, mut cache_hint)), Ok(collection)) => {
                        // TODO: avoid parsing the generated json
                        let mut json: serde_json::Value =
                            serde_json::from_str(&collection.to_geo_json())
                                .expect("to_geojson is correct");
                        let more_features = json
                            .get_mut("features")
                            .expect("to_geojson is correct")
                            .as_array_mut()
                            .expect("to geojson is correct");

                        output.append(more_features);

                        cache_hint.merge_with(&collection.cache_hint);

                        Ok((output, cache_hint))
                    }
                    (Err(error), _) | (_, Err(error)) => Err(error),
                }
            },
        ));

    let (features, cache_hint) =
        abortable_query_execution(future, conn_closed, query_abort_trigger).await?;

    let mut output = json!({
        "type": "FeatureCollection"
    });

    output
        .as_object_mut()
        .expect("as defined")
        .insert("features".into(), serde_json::Value::Array(features));

    Ok((output, cache_hint))
}

#[allow(clippy::unnecessary_wraps)] // TODO: remove line once implemented fully
fn get_feature_mock(_request: &GetFeature) -> Result<HttpResponse> {
    let collection = MultiPointCollection::from_data(
        MultiPoint::many(vec![
            (0.0, 0.1),
            (1.0, 1.1),
            (2.0, 3.1),
            (3.0, 3.1),
            (4.0, 4.1),
        ])
        .unwrap(),
        vec![geoengine_datatypes::primitives::TimeInterval::new_unchecked(0, 1); 5],
        [(
            "foo".to_string(),
            FeatureData::NullableInt(vec![Some(0), None, Some(2), Some(3), Some(4)]),
        )]
        .iter()
        .cloned()
        .collect(),
        CacheHint::default(),
    )
    .unwrap();

    Ok(HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .body(collection.to_geo_json()))
}

fn default_time_from_config() -> TimeInterval {
    get_config_element::<config::Wfs>()
        .ok()
        .and_then(|wfs| wfs.default_time)
        .map_or_else(
            || {
                get_config_element::<config::Ogc>()
                    .ok()
                    .and_then(|ogc| ogc.default_time)
                    .map_or_else(
                        || {
                            geoengine_datatypes::primitives::TimeInterval::new_instant(
                                geoengine_datatypes::primitives::TimeInstance::now(),
                            )
                            .expect("is a valid time interval")
                            .into()
                        },
                        |time| time.time_interval(),
                    )
            },
            |time| time.time_interval(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::model::datatypes::{DataId, DatasetId};
    use crate::contexts::{PostgresContext, Session, SimpleApplicationContext};
    use crate::datasets::storage::{DatasetDefinition, DatasetStore};
    use crate::handlers::ErrorResponse;
    use crate::util::tests::with_temp_context;
    use crate::util::tests::with_temp_context_from_spec;
    use crate::util::tests::{check_allowed_http_methods, read_body_string, send_test_request};
    use crate::workflows::workflow::Workflow;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::header;
    use actix_web::{http::Method, test};
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::raster::{GridShape2D, TilingSpecification};
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::TypedOperator;
    use geoengine_operators::source::CsvSourceParameters;
    use geoengine_operators::source::{CsvGeometrySpecification, CsvSource, CsvTimeSpecification};
    use serde_json::json;
    use std::fs;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::Path;
    use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
    use tokio_postgres::Socket;
    use xml::ParserConfig;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn mock_test() {
        with_temp_context(|app_ctx, _| async move {
        let ctx = app_ctx.default_session_context().await.unwrap();
        let session_id = ctx.session().id();

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

        })
        .await;
    }

    async fn get_capabilities_test_helper(method: Method) -> ServiceResponse {
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

        with_temp_context(|app_ctx, _| async move {
            let session_id = app_ctx.default_session_id().await;

            let workflow = Workflow {
                operator: TypedOperator::Vector(Box::new(CsvSource {
                    params: CsvSourceParameters {
                        file_path: temp_file.path().into(),
                        field_separator: ';',
                        geometry: CsvGeometrySpecification::XY {
                            x: "x".into(),
                            y: "y".into(),
                        },
                        time: CsvTimeSpecification::None,
                    },
                })),
            };

            let workflow_id = app_ctx
                .default_session_context()
                .await
                .unwrap()
                .db()
                .register_workflow(workflow)
                .await
                .unwrap();

            let req = test::TestRequest::with_uri(&format!(
                "/wfs/{workflow_id}?request=GetCapabilities&service=WFS"
            ))
            .method(method)
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
            send_test_request(req, app_ctx).await
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_capabilities() {
        let res = get_capabilities_test_helper(Method::GET).await;

        assert_eq!(res.status(), 200);

        // TODO: validate against schema
        let body = test::read_body(res).await;
        let reader = ParserConfig::default().create_reader(body.as_ref());

        for event in reader {
            assert!(event.is_ok());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_capabilities_invalid_method() {
        check_allowed_http_methods(get_capabilities_test_helper, &[Method::GET]).await;
    }

    async fn get_feature_registry_test_helper(method: Method) -> ServiceResponse {
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

        with_temp_context(|app_ctx, _| async move {
        let ctx = app_ctx.default_session_context().await.unwrap();
        let session_id = app_ctx.default_session_id().await;

        let workflow = Workflow {
            operator: TypedOperator::Vector(Box::new(CsvSource {
                params: CsvSourceParameters {
                    file_path: temp_file.path().into(),
                    field_separator: ';',
                    geometry: CsvGeometrySpecification::XY {
                        x: "x".into(),
                        y: "y".into(),
                    },
                    time: CsvTimeSpecification::None,
                },
            })),
        };

        let id = ctx.db().register_workflow(workflow.clone()).await.unwrap();

        let req = test::TestRequest::with_uri(&format!("/wfs/{id}?request=GetFeature&service=WFS&version=2.0.0&typeNames={id}&bbox=-90,-180,90,180&srsName=EPSG:4326", id = id.to_string())).method(method).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, app_ctx).await

        })
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_feature_registry() {
        let res = get_feature_registry_test_helper(Method::GET).await;

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
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
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
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
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
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_feature_registry_invalid_method() {
        check_allowed_http_methods(get_feature_registry_test_helper, &[Method::GET]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_feature_registry_missing_fields() {
        with_temp_context(|app_ctx, _| async move {
        let ctx = app_ctx.default_session_context().await.unwrap();
        let session_id = ctx.session().id();

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

        })
        .await;
    }

    async fn get_feature_json_test_helper(method: Method) -> ServiceResponse {
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

        with_temp_context(|app_ctx, _| async move {
            let session_id = app_ctx.default_session_id().await;

            let workflow = Workflow {
                operator: TypedOperator::Vector(Box::new(CsvSource {
                    params: CsvSourceParameters {
                        file_path: temp_file.path().into(),
                        field_separator: ';',
                        geometry: CsvGeometrySpecification::XY {
                            x: "x".into(),
                            y: "y".into(),
                        },
                        time: CsvTimeSpecification::None,
                    },
                })),
            };

            let workflow_id = app_ctx
                .default_session_context()
                .await
                .unwrap()
                .db()
                .register_workflow(workflow)
                .await
                .unwrap();

            let params = &[
                ("request", "GetFeature"),
                ("service", "WFS"),
                ("version", "2.0.0"),
                ("typeNames", &workflow_id.to_string()),
                ("bbox", "-90,-180,90,180"),
                ("srsName", "EPSG:4326"),
            ];
            let req = test::TestRequest::with_uri(&format!(
                "/wfs/{}?{}",
                workflow_id,
                &serde_urlencoded::to_string(params).unwrap()
            ))
            .method(method)
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
            send_test_request(req, app_ctx).await
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_feature_json() {
        let res = get_feature_json_test_helper(Method::GET).await;

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
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
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
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
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
                        "start": "-262144-01-01T00:00:00+00:00",
                        "end": "+262143-12-31T23:59:59.999+00:00",
                        "type": "Interval"
                    }
                }]
            })
            .to_string()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_feature_json_invalid_method() {
        check_allowed_http_methods(get_feature_json_test_helper, &[Method::GET]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn get_feature_json_missing_fields() {
        with_temp_context(|app_ctx, _| async move {
            let ctx = app_ctx.default_session_context().await.unwrap();
            let session_id = ctx.session().id();

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
                    &serde_urlencoded::to_string(params).unwrap()
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
        })
        .await;
    }

    async fn add_dataset_definition_to_datasets<Tls>(
        app_ctx: &PostgresContext<Tls>,
        dataset_definition_path: &Path,
    ) -> DatasetId
    where
        Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
        <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
        <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
        <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let data = fs::read_to_string(dataset_definition_path).unwrap();
        let data = data.replace("test_data/", test_data!("./").to_str().unwrap());
        let def: DatasetDefinition = serde_json::from_str(&data).unwrap();

        let db = app_ctx.default_session_context().await.unwrap().db();

        db.add_dataset(def.properties, Box::new(def.meta_data))
            .await
            .unwrap()
            .id
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[allow(clippy::too_many_lines)]
    async fn raster_vector_join() {
        let exe_ctx_tiling_spec = TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        };

        // override the pixel size since this test was designed for 600 x 600 pixel tiles
        with_temp_context_from_spec(
            exe_ctx_tiling_spec,
            TestDefault::test_default(),
            |app_ctx, _| async move {
                let session_id = app_ctx.default_session_id().await;

                let _ndvi_id: DataId = add_dataset_definition_to_datasets(
                    &app_ctx,
                    test_data!("dataset_defs/ndvi.json"),
                )
                .await
                .into();
                let _points_with_time_id: DataId = add_dataset_definition_to_datasets(
                    &app_ctx,
                    test_data!("dataset_defs/points_with_time.json"),
                )
                .await
                .into();

                let workflow = serde_json::json!({
                    "type": "Vector",
                    "operator": {
                        "type": "RasterVectorJoin",
                        "params": {
                            "names": [
                                "NDVI"
                            ],
                            "featureAggregation": "first",
                            "temporalAggregation": "first"
                        },
                        "sources": {
                            "vector": {
                                "type": "OgrSource",
                                "params": {
                                    "data": "points_with_time",
                                    "attributeProjection": null
                                }
                            },
                            "rasters": [{
                                "type": "GdalSource",
                                "params": {
                                    "data": "ndvi",
                                }
                            }],
                        }
                    }
                });

                let json = serde_json::to_string(&workflow).unwrap();

                let workflow = serde_json::from_str(&json).unwrap();

                let workflow_id = app_ctx
                    .default_session_context()
                    .await
                    .unwrap()
                    .db()
                    .register_workflow(workflow)
                    .await
                    .unwrap();

                let params = &[
                    ("request", "GetFeature"),
                    ("service", "WFS"),
                    ("version", "2.0.0"),
                    ("typeNames", &workflow_id.to_string()),
                    ("bbox", "-90,-180,90,180"),
                    ("srsName", "EPSG:4326"),
                    ("time", "2014-04-01T12:00:00.000Z/2014-04-01T12:00:00.000Z"),
                ];
                let req = test::TestRequest::get()
                    .uri(&format!(
                        "/wfs/{}?{}",
                        workflow_id,
                        &serde_urlencoded::to_string(params).unwrap()
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
            },
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn it_sets_cache_control_header() {
        let res = get_feature_json_test_helper(Method::GET).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache"
        );
    }
}
