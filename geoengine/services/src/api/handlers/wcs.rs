use crate::api::model::processing_graphs::ProcessingGraphId;
use crate::{
    api::{
        handlers::spatial_references::spatial_reference_specification,
        ogc::{
            util::{OgcProtocol, OgcQueryExtractor, ogc_endpoint_url},
            wcs::request::{DescribeCoverage, GetCapabilities, GetCoverage, WcsVersion},
        },
    },
    config::{self},
    contexts::{ApplicationContext, SessionContext},
    error::{self, Error, Result},
    queries::{
        raster_grid::{CoverageQuery, coverage_geotiff},
        run_query,
    },
    util::server::{CacheControlHeader, connection_closed},
};
use actix_web::{FromRequest, HttpRequest, HttpResponse, web};
use geoengine_datatypes::{
    primitives::AxisAlignedRectangle, raster::GridShape2D, spatial_reference::SpatialReference,
};
use geoengine_operators::engine::{InitializedRasterOperator, WorkflowOperatorPath};
use serde::Deserialize;
use snafu::ensure;
use std::{str::FromStr, time::Duration};
use tracing::info;
use url::Url;
use utoipa::{
    IntoParams,
    openapi::{Ref, Required},
};

pub(crate) fn init_wcs_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/wcs/{processingGraph}").route(web::get().to(wcs_handler::<C>)));
}

#[derive(Debug, Deserialize)]
#[serde(tag = "request", rename_all = "PascalCase")]
pub enum WcsQueryParams {
    GetCapabilities(GetCapabilities),
    DescribeCoverage(DescribeCoverage),
    GetCoverage(GetCoverage),
}

/// manual implementation because derive macro does not support enums
/// Note, that WCS is not really OpenAPI compatible, so this is just an approximation to enable client generation
impl IntoParams for WcsQueryParams {
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
                .description(Some("type of WCS request"))
                .schema(Some(Ref::from_schema_name("WcsRequest")))
                .build(),
        );

        for mut p in GetCapabilities::into_params(pip) {
            p.required = Required::False;
            params.push(p);
        }
        for mut p in DescribeCoverage::into_params(pip) {
            p.required = Required::False;
            params.push(p);
        }
        for mut p in GetCoverage::into_params(pip) {
            p.required = Required::False;
            params.push(p);
        }

        // remove duplicate parameters
        params.sort_by(|a, b| a.name.cmp(&b.name));
        params.dedup_by(|a, b| a.name == b.name);

        params
    }
}

fn wcs_url(processing_graph: ServicesProcessingGraphId) -> Result<Url> {
    let web_config = crate::config::get_config_element::<crate::config::Web>()?;
    let base = web_config.api_url()?;

    ogc_endpoint_url(&base, OgcProtocol::Wcs, processing_graph)
}

/// OGC WCS endpoint
#[utoipa::path(
    tag = "OGC WCS",
    get,
    path = "/wcs/{processingGraph}",
    responses(
        (status = 200, description = "OK", content_type = "text/xml", body = String,
            // TODO: add example when utoipa supports more than just json examples
        )
    ),
    params(
        ("processingGraph" = ProcessingGraphId, description = "Processing graph id"),
        WcsQueryParams
    ),
    security(
        ("session_token" = [])
    )
)]

async fn wcs_handler<C: ApplicationContext>(
    req: HttpRequest,
    processing_graph: web::Path<ProcessingGraphId>,
    request: OgcQueryExtractor<WcsQueryParams>,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    let processing_graph: ServicesProcessingGraphId = processing_graph.into_inner().into();

    match request.into_inner() {
        WcsQueryParams::GetCapabilities(r) => {
            wcs_get_capabilities::<C>(processing_graph, r, session).await
        }
        WcsQueryParams::DescribeCoverage(r) => {
            wcs_describe_coverage::<C>(processing_graph, r, app_ctx, session).await
        }
        WcsQueryParams::GetCoverage(r) => {
            wcs_get_coverage::<C>(req, processing_graph, r, app_ctx, session).await
        }
    }
}

#[allow(
    clippy::unused_async, // the function signature of request handlers requires it
    clippy::no_effect_underscore_binding // need `_session` to quire authentication
)]
async fn wcs_get_capabilities<C: ApplicationContext>(
    processing_graph: ServicesProcessingGraphId,
    request: GetCapabilities,
    _session: C::Session,
) -> Result<HttpResponse> {
    info!("{request:?}");

    // TODO: processing graph bounding box
    // TODO: host schema file(?)
    // TODO: load ServiceIdentification and ServiceProvider from config

    let wcs_url = wcs_url(processing_graph)?;
    let mock = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
    <wcs:Capabilities version="1.1.1"
            xmlns:wcs="http://www.opengis.net/wcs/1.1.1"
            xmlns:xlink="http://www.w3.org/1999/xlink"
            xmlns:ogc="http://www.opengis.net/ogc"
            xmlns:ows="http://www.opengis.net/ows/1.1"
            xmlns:gml="http://www.opengis.net/gml"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 {wcs_url}/schemas/wcs/1.1.1/wcsGetCapabilities.xsd" updateSequence="152">
            <ows:ServiceIdentification>
                <ows:Title>Web Coverage Service</ows:Title>
                <ows:ServiceType>WCS</ows:ServiceType>
                <ows:ServiceTypeVersion>1.1.1</ows:ServiceTypeVersion>
                <ows:Fees>NONE</ows:Fees>
                <ows:AccessConstraints>NONE</ows:AccessConstraints>
            </ows:ServiceIdentification>
            <ows:ServiceProvider>
                <ows:ProviderName>Provider Name</ows:ProviderName>
            </ows:ServiceProvider>
            <ows:OperationsMetadata>
                <ows:Operation name="GetCapabilities">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="{wcs_url}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="DescribeCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="{wcs_url}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="GetCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="{wcs_url}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
            </ows:OperationsMetadata>
            <wcs:Contents>
                <wcs:CoverageSummary>
                    <ows:Title>Processing Graph {processing_graph}</ows:Title>
                    <ows:WGS84BoundingBox>
                        <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                        <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
                    </ows:WGS84BoundingBox>
                    <wcs:Identifier>{processing_graph}</wcs:Identifier>
                </wcs:CoverageSummary>
            </wcs:Contents>
    </wcs:Capabilities>"#
    );

    Ok(HttpResponse::Ok().content_type(mime::TEXT_XML).body(mock))
}

#[allow(clippy::too_many_lines)]
async fn wcs_describe_coverage<C: ApplicationContext>(
    processing_graph: ServicesProcessingGraphId,
    request: DescribeCoverage,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    let endpoint = processing_graph;

    info!("{request:?}");

    let identifiers = ServicesProcessingGraphId::from_str(&request.identifiers)?;

    ensure!(
        endpoint == identifiers,
        error::WCSEndpointIdentifiersMissmatch {
            endpoint,
            identifiers
        }
    );

    // TODO: validate request (version)?

    let wcs_url = wcs_url(identifiers)?;

    let ctx = app_ctx.session_context(session);

    let processing_graph = ctx.db().load_processing_graph(&identifiers).await?;

    let exe_ctx = ctx.execution_context()?;

    let processing_graph_operator_path_root = ProcessingGraphOperatorPath::initialize_root();

    let operator = processing_graph
        .operator
        .get_raster()?
        .initialize(processing_graph_operator_path_root, &exe_ctx)
        .await?;

    let result_descriptor = operator.result_descriptor();

    let spatial_grid_descriptor = result_descriptor.spatial_grid_descriptor();

    let spatial_reference: Option<SpatialReference> = result_descriptor.spatial_reference.into();
    let spatial_reference = spatial_reference.ok_or(error::Error::MissingSpatialReference)?;
    let spatial_reference_spec = spatial_reference_specification(spatial_reference.into())?;
    let spatial_ref_axis_order =
        spatial_reference_spec
            .axis_order
            .ok_or(Error::AxisOrderingNotKnownForSrs {
                srs_string: spatial_reference.srs_string(),
            })?;
    let bounds = spatial_grid_descriptor.spatial_partition();

    let [bbox_ll_0, bbox_ll_1] =
        spatial_ref_axis_order.xy_to_native_order([bounds.lower_left().x, bounds.lower_left().y]);
    let [bbox_ur_0, bbox_ur_1] =
        spatial_ref_axis_order.xy_to_native_order([bounds.upper_right().x, bounds.upper_right().y]);

    let GridShape2D { shape_array } = spatial_grid_descriptor.grid_shape();

    let [raster_size_0, raster_size_1] = spatial_ref_axis_order.xy_to_native_order(shape_array);

    let spatial_resolution = spatial_grid_descriptor.spatial_resolution();
    let pixel_size_x = spatial_resolution.x();
    let pixel_size_y = spatial_resolution.y();

    let band_0 = &result_descriptor.bands[0];

    let mock = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
    <wcs:CoverageDescriptions xmlns:wcs="http://www.opengis.net/wcs/1.1.1"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        xmlns:ogc="http://www.opengis.net/ogc"
        xmlns:ows="http://www.opengis.net/ows/1.1"
        xmlns:gml="http://www.opengis.net/gml"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 {wcs_url}/schemas/wcs/1.1.1/wcsDescribeCoverage.xsd">
        <wcs:CoverageDescription>
            <ows:Title>Processing Graph {processing_graph_id}</ows:Title>
            <wcs:Identifier>{processing_graph_id}</wcs:Identifier>
            <wcs:Domain>
                <wcs:SpatialDomain>
                    <ows:BoundingBox crs="urn:ogc:def:crs:{srs_authority}::{srs_code}" dimensions="2">
                        <ows:LowerCorner>{bbox_ll_0} {bbox_ll_1}</ows:LowerCorner>
                        <ows:UpperCorner>{bbox_ur_0} {bbox_ur_1}</ows:UpperCorner>
                    </ows:BoundingBox>
                    <ows:BoundingBox crs="urn:ogc:def:crs:OGC:1.3:CRS:imageCRS" dimensions="2">
                        <ows:LowerCorner>0 0</ows:LowerCorner>
                        <ows:UpperCorner>{raster_size_0} {raster_size_1}</ows:UpperCorner>
                    </ows:BoundingBox>
                    <wcs:GridCRS>
                        <wcs:GridBaseCRS>urn:ogc:def:crs:{srs_authority}::{srs_code}</wcs:GridBaseCRS>
                        <wcs:GridType>urn:ogc:def:method:WCS:1.1:2dGridIn2dCrs</wcs:GridType>
                        <wcs:GridOrigin>{origin_x} {origin_y}</wcs:GridOrigin>
                        <wcs:GridOffsets>{pixel_size_x} 0.0 0.0 {pixel_size_y}</wcs:GridOffsets>
                        <wcs:GridCS>urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS</wcs:GridCS>
                    </wcs:GridCRS>
                </wcs:SpatialDomain>
            </wcs:Domain>
            <wcs:Range>
                <wcs:Field>
                    <wcs:Identifier>contents</wcs:Identifier>
                    <wcs:Axis identifier="Bands">
                        <wcs:AvailableKeys>
                            <wcs:Key>{band_name}</wcs:Key>
                        </wcs:AvailableKeys>
                    </wcs:Axis>
                </wcs:Field>
            </wcs:Range>
            <wcs:SupportedCRS>{srs_authority}:{srs_code}</wcs:SupportedCRS>
            <wcs:SupportedFormat>image/tiff</wcs:SupportedFormat>
        </wcs:CoverageDescription>
    </wcs:CoverageDescriptions>"#,
        wcs_url = wcs_url,
        processing_graph_id = identifiers,
        srs_authority = spatial_reference.authority(),
        srs_code = spatial_reference.code(),
        origin_x = bounds.upper_left().x,
        origin_y = bounds.upper_left().y,
        bbox_ll_0 = bbox_ll_0,
        bbox_ll_1 = bbox_ll_1,
        bbox_ur_0 = bbox_ur_0,
        bbox_ur_1 = bbox_ur_1,
        band_name = band_0.name,
        pixel_size_y = -pixel_size_y, // TODO: use the "real" sign in the resolution?
        pixel_size_x = pixel_size_x
    );

    Ok(HttpResponse::Ok().content_type(mime::TEXT_XML).body(mock))
}

#[allow(clippy::too_many_lines)]
async fn wcs_get_coverage<C: ApplicationContext>(
    req: HttpRequest,
    processing_graph: ServicesProcessingGraphId,
    request: GetCoverage,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    let endpoint = processing_graph;

    info!("{request:?}");

    let identifier = ServicesProcessingGraphId::from_str(&request.identifier)?;

    ensure!(
        endpoint == identifier,
        error::WCSEndpointIdentifierMissmatch {
            endpoint,
            identifier
        }
    );

    ensure!(
        request.version == WcsVersion::V1_1_0 || request.version == WcsVersion::V1_1_1,
        error::WcsVersionNotSupported
    );

    let conn_closed = connection_closed(
        &req,
        config::get_config_element::<config::Wcs>()?
            .request_timeout_seconds
            .map(Duration::from_secs),
    );

    let query = CoverageQuery {
        workflow: identifier,
        partition: request.spatial_partition()?,
        crs: request.spatial_ref().map(Into::into)?,
        resolution: request.spatial_resolution().transpose()?,
        time: request
            .time
            .map_or_else(config::default_time_from_config::<config::Wcs>, Into::into),
        no_data_value: request.nodatavalue,
    };

    let ctx = app_ctx.session_context(session);
    let (bytes, cache_hint, computation_id) =
        run_query(&ctx, conn_closed, coverage_geotiff(ctx.clone(), query)).await?;

    Ok(HttpResponse::Ok()
        .append_header(cache_hint.cache_control_header())
        .append_header(computation_id)
        .content_type("image/tiff")
        .body(bytes))
}

#[cfg(test)]
mod tests {
    use crate::contexts::PostgresContext;
    use crate::contexts::Session;
    use crate::ge_context;
    use crate::users::UserAuth;
    use crate::util::tests::register_ndvi_processing_graph_helper;
    use crate::util::tests::{read_body_string, send_test_request};
    use actix_web::http::header;
    use actix_web::test;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::raster::GridShape2D;
    use geoengine_datatypes::raster::TilingSpecification;
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::ImageFormat;
    use geoengine_datatypes::util::assert_image_equals_with_format;
    use tokio_postgres::NoTls;

    fn tiling_spec() -> TilingSpecification {
        TilingSpecification::new(GridShape2D::new([600, 600]))
    }

    #[ge_context::test]
    async fn get_capabilities(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, processing_graph_id) = register_ndvi_processing_graph_helper(&app_ctx).await;

        let params = &[
            ("service", "WCS"),
            ("request", "GetCapabilities"),
            ("version", "1.1.1"),
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                processing_graph_id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());
        let body = read_body_string(res).await;
        assert_eq!(
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
    <wcs:Capabilities version="1.1.1"
            xmlns:wcs="http://www.opengis.net/wcs/1.1.1"
            xmlns:xlink="http://www.w3.org/1999/xlink"
            xmlns:ogc="http://www.opengis.net/ogc"
            xmlns:ows="http://www.opengis.net/ows/1.1"
            xmlns:gml="http://www.opengis.net/gml"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 http://127.0.0.1:3030/api/wcs/{processing_graph_id}/schemas/wcs/1.1.1/wcsGetCapabilities.xsd" updateSequence="152">
            <ows:ServiceIdentification>
                <ows:Title>Web Coverage Service</ows:Title>
                <ows:ServiceType>WCS</ows:ServiceType>
                <ows:ServiceTypeVersion>1.1.1</ows:ServiceTypeVersion>
                <ows:Fees>NONE</ows:Fees>
                <ows:AccessConstraints>NONE</ows:AccessConstraints>
            </ows:ServiceIdentification>
            <ows:ServiceProvider>
                <ows:ProviderName>Provider Name</ows:ProviderName>
            </ows:ServiceProvider>
            <ows:OperationsMetadata>
                <ows:Operation name="GetCapabilities">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://127.0.0.1:3030/api/wcs/{processing_graph_id}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="DescribeCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://127.0.0.1:3030/api/wcs/{processing_graph_id}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="GetCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://127.0.0.1:3030/api/wcs/{processing_graph_id}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
            </ows:OperationsMetadata>
            <wcs:Contents>
                <wcs:CoverageSummary>
                    <ows:Title>Processing Graph {processing_graph_id}</ows:Title>
                    <ows:WGS84BoundingBox>
                        <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                        <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
                    </ows:WGS84BoundingBox>
                    <wcs:Identifier>{processing_graph_id}</wcs:Identifier>
                </wcs:CoverageSummary>
            </wcs:Contents>
    </wcs:Capabilities>"#
            ),
            body
        );
    }

    #[ge_context::test]
    async fn describe_coverage(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, processing_graph_id) = register_ndvi_processing_graph_helper(&app_ctx).await;

        let params = &[
            ("service", "WCS"),
            ("request", "DescribeCoverage"),
            ("version", "1.1.1"),
            ("identifiers", &processing_graph_id.to_string()),
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                processing_graph_id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());
        let body = read_body_string(res).await;
        assert_eq!(
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
    <wcs:CoverageDescriptions xmlns:wcs="http://www.opengis.net/wcs/1.1.1"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        xmlns:ogc="http://www.opengis.net/ogc"
        xmlns:ows="http://www.opengis.net/ows/1.1"
        xmlns:gml="http://www.opengis.net/gml"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 http://127.0.0.1:3030/api/wcs/{processing_graph_id}/schemas/wcs/1.1.1/wcsDescribeCoverage.xsd">
        <wcs:CoverageDescription>
            <ows:Title>Processing Graph {processing_graph_id}</ows:Title>
            <wcs:Identifier>{processing_graph_id}</wcs:Identifier>
            <wcs:Domain>
                <wcs:SpatialDomain>
                    <ows:BoundingBox crs="urn:ogc:def:crs:EPSG::4326" dimensions="2">
                        <ows:LowerCorner>-90 -180</ows:LowerCorner>
                        <ows:UpperCorner>90 180</ows:UpperCorner>
                    </ows:BoundingBox>
                    <ows:BoundingBox crs="urn:ogc:def:crs:OGC:1.3:CRS:imageCRS" dimensions="2">
                        <ows:LowerCorner>0 0</ows:LowerCorner>
                        <ows:UpperCorner>3600 1800</ows:UpperCorner>
                    </ows:BoundingBox>
                    <wcs:GridCRS>
                        <wcs:GridBaseCRS>urn:ogc:def:crs:EPSG::4326</wcs:GridBaseCRS>
                        <wcs:GridType>urn:ogc:def:method:WCS:1.1:2dGridIn2dCrs</wcs:GridType>
                        <wcs:GridOrigin>-180 90</wcs:GridOrigin>
                        <wcs:GridOffsets>0.1 0.0 0.0 -0.1</wcs:GridOffsets>
                        <wcs:GridCS>urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS</wcs:GridCS>
                    </wcs:GridCRS>
                </wcs:SpatialDomain>
            </wcs:Domain>
            <wcs:Range>
                <wcs:Field>
                    <wcs:Identifier>contents</wcs:Identifier>
                    <wcs:Axis identifier="Bands">
                        <wcs:AvailableKeys>
                            <wcs:Key>ndvi</wcs:Key>
                        </wcs:AvailableKeys>
                    </wcs:Axis>
                </wcs:Field>
            </wcs:Range>
            <wcs:SupportedCRS>EPSG:4326</wcs:SupportedCRS>
            <wcs:SupportedFormat>image/tiff</wcs:SupportedFormat>
        </wcs:CoverageDescription>
    </wcs:CoverageDescriptions>"#
            ),
            body
        );
    }

    // TODO: add get_coverage with masked band

    #[ge_context::test(tiling_spec = "tiling_spec")]
    async fn get_coverage_with_nodatavalue(app_ctx: PostgresContext<NoTls>) {
        // override the pixel size since this test was designed for 600 x 600 pixel tiles

        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, id) = register_ndvi_processing_graph_helper(&app_ctx).await;

        let params = &[
            ("service", "WCS"),
            ("request", "GetCoverage"),
            ("version", "1.1.1"),
            ("identifier", &id.to_string()),
            ("boundingbox", "20,-10,80,50,urn:ogc:def:crs:EPSG::4326"),
            ("format", "image/tiff"),
            ("gridbasecrs", "urn:ogc:def:crs:EPSG::4326"),
            ("gridcs", "urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS"),
            ("gridtype", "urn:ogc:def:method:WCS:1.1:2dSimpleGrid"),
            ("gridorigin", "80,-10"),
            ("gridoffsets", "0.1,0.1"),
            ("time", "2014-01-01T00:00:00.0Z"),
            ("nodatavalue", "0.0"),
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());
        assert_image_equals_with_format(
            test_data!("raster/geotiff_from_stream_compressed.tiff"),
            test::read_body(res).await.as_ref(),
            ImageFormat::Tiff,
        );
    }

    #[ge_context::test(tiling_spec = "tiling_spec")]
    async fn it_sets_cache_control_header(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, id) = register_ndvi_processing_graph_helper(&app_ctx).await;

        let params = &[
            ("service", "WCS"),
            ("request", "GetCoverage"),
            ("version", "1.1.1"),
            ("identifier", &id.to_string()),
            ("boundingbox", "20,-10,80,50,urn:ogc:def:crs:EPSG::4326"),
            ("format", "image/tiff"),
            ("gridbasecrs", "urn:ogc:def:crs:EPSG::4326"),
            ("gridcs", "urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS"),
            ("gridtype", "urn:ogc:def:method:WCS:1.1:2dSimpleGrid"),
            ("gridorigin", "80,-10"),
            ("gridoffsets", "0.1,0.1"),
            ("time", "2014-01-01T00:00:00.0Z"),
            ("nodatavalue", "0.0"),
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));

        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200, "{:?}", res.response());
        assert_eq!(
            res.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache"
        );
    }
}
