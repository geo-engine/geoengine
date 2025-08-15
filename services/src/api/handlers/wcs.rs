use crate::api::handlers::spatial_references::spatial_reference_specification;
use crate::api::ogc::util::{OgcProtocol, OgcRequestGuard, ogc_endpoint_url};
use crate::api::ogc::wcs::request::{DescribeCoverage, GetCapabilities, GetCoverage, WcsVersion};
use crate::config;
use crate::config::get_config_element;
use crate::contexts::{ApplicationContext, SessionContext};
use crate::error::Result;
use crate::error::{self, Error};
use crate::util::server::{CacheControlHeader, connection_closed, not_implemented_handler};
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use actix_web::{FromRequest, HttpRequest, HttpResponse, web};
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, BandSelection, RasterQueryRectangle, SpatialResolution, TimeInterval,
};
use geoengine_datatypes::raster::GridShape2D;
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::call_on_generic_raster_processor_gdal_types;
use geoengine_operators::engine::{
    ExecutionContext, InitializedRasterOperator, WorkflowOperatorPath,
};
use geoengine_operators::util::raster_stream_to_geotiff::{
    GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions, raster_stream_to_multiband_geotiff_bytes,
};
use snafu::ensure;
use std::str::FromStr;
use std::time::Duration;
use tracing::info;
use url::Url;
use uuid::Uuid;

pub(crate) fn init_wcs_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
    C::Session: FromRequest,
{
    cfg.service(
        web::resource("/wcs/{workflow}")
            .route(
                web::get()
                    .guard(OgcRequestGuard::new("GetCapabilities"))
                    .to(wcs_capabilities_handler::<C>),
            )
            .route(
                web::get()
                    .guard(OgcRequestGuard::new("DescribeCoverage"))
                    .to(wcs_describe_coverage_handler::<C>),
            )
            .route(
                web::get()
                    .guard(OgcRequestGuard::new("GetCoverage"))
                    .to(wcs_get_coverage_handler::<C>),
            )
            .route(web::get().to(not_implemented_handler)),
    );
}

fn wcs_url(workflow: WorkflowId) -> Result<Url> {
    let web_config = crate::config::get_config_element::<crate::config::Web>()?;
    let base = web_config.api_url()?;

    ogc_endpoint_url(&base, OgcProtocol::Wcs, workflow)
}

/// Get WCS Capabilities
#[utoipa::path(
    tag = "OGC WCS",
    get,
    path = "/wcs/{workflow}?request=GetCapabilities",
    responses(
        (status = 200, description = "OK", content_type = "text/xml", body = String,
            // TODO: add example when utoipa supports more than just json examples
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
#[allow(
    clippy::unused_async, // the function signature of request handlers requires it
    clippy::no_effect_underscore_binding // need `_session` to quire authentication
)]
async fn wcs_capabilities_handler<C: ApplicationContext>(
    workflow: web::Path<WorkflowId>,
    request: web::Query<GetCapabilities>,
    _session: C::Session,
) -> Result<HttpResponse> {
    let workflow = workflow.into_inner();

    info!("{request:?}");

    // TODO: workflow bounding box
    // TODO: host schema file(?)
    // TODO: load ServiceIdentification and ServiceProvider from config

    let wcs_url = wcs_url(workflow)?;
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
                    <ows:Title>Workflow {workflow}</ows:Title>
                    <ows:WGS84BoundingBox>
                        <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                        <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
                    </ows:WGS84BoundingBox>
                    <wcs:Identifier>{workflow}</wcs:Identifier>
                </wcs:CoverageSummary>
            </wcs:Contents>
    </wcs:Capabilities>"#
    );

    Ok(HttpResponse::Ok().content_type(mime::TEXT_XML).body(mock))
}

/// Get WCS Coverage Description
#[utoipa::path(
    tag = "OGC WCS",
    get,
    path = "/wcs/{workflow}?request=DescribeCoverage",
    responses(
        (status = 200, description = "OK", content_type = "text/xml", body = String,
            // TODO: add example when utoipa supports more than just json examples
        )
    ),
    params(
        ("workflow" = WorkflowId, description = "Workflow id"),
        DescribeCoverage
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::too_many_lines)]
async fn wcs_describe_coverage_handler<C: ApplicationContext>(
    workflow: web::Path<WorkflowId>,
    request: web::Query<DescribeCoverage>,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    let endpoint = workflow.into_inner();

    info!("{request:?}");

    let identifiers = WorkflowId::from_str(&request.identifiers)?;

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

    let workflow = ctx.db().load_workflow(&identifiers).await?;

    let exe_ctx = ctx.execution_context()?;

    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let operator = workflow
        .operator
        .get_raster()?
        .initialize(workflow_operator_path_root, &exe_ctx)
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

    let SpatialResolution {
        x: pixel_size_x,
        y: pixel_size_y,
    } = spatial_grid_descriptor.spatial_resolution();

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
            <ows:Title>Workflow {workflow_id}</ows:Title>
            <wcs:Identifier>{workflow_id}</wcs:Identifier>
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
        workflow_id = identifiers,
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

/// Get WCS Coverage
#[utoipa::path(
    tag = "OGC WCS",
    get,
    path = "/wcs/{workflow}?request=GetCoverage",
    responses(
        (status = 200, response = crate::api::model::responses::PngResponse),
    ),
    params(
        ("workflow" = WorkflowId, description = "Workflow id"),
        GetCoverage
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::too_many_lines)]
async fn wcs_get_coverage_handler<C: ApplicationContext>(
    req: HttpRequest,
    workflow: web::Path<WorkflowId>,
    request: web::Query<GetCoverage>,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    let endpoint = workflow.into_inner();

    info!("{request:?}");

    let identifier = WorkflowId::from_str(&request.identifier)?;

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

    let request_spatial_ref: SpatialReference = request.spatial_ref().map(Into::into)?;
    let request_resolution = request.spatial_resolution().transpose()?;
    let request_partition = request.spatial_partition()?;
    let request_time: TimeInterval = request
        .time
        .map_or_else(default_time_from_config, Into::into);
    let request_no_data_value = request.nodatavalue;

    let ctx = app_ctx.session_context(session);

    let workflow = ctx.db().load_workflow(&identifier).await?;

    let operator = workflow.operator.get_raster()?;

    let execution_context = ctx.execution_context()?;

    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let initialized = operator
        .clone()
        .initialize(workflow_operator_path_root, &execution_context)
        .await?;

    let tiling_spec = execution_context.tiling_specification();

    let wrapped =
        geoengine_operators::util::WrapWithProjectionAndResample::new_create_result_descriptor(
            operator,
            initialized,
        )
        .wrap_with_projection_and_resample(
            Some(request_partition.upper_left()), // TODO: set none if not changed? But how to handle mapping to grid?
            request_resolution,
            request_spatial_ref,
            tiling_spec,
            &execution_context,
        )
        .await?;

    let query_tiling_pixel_grid = wrapped
        .result_descriptor
        .spatial_grid_descriptor()
        .tiling_grid_definition(tiling_spec)
        .tiling_spatial_grid_definition()
        .spatial_bounds_to_compatible_spatial_grid(request_partition);

    let query_rect = RasterQueryRectangle::new(
        query_tiling_pixel_grid.grid_bounds(),
        request_time,
        BandSelection::first(), // TODO: support multi bands in API and set the selection here
    );

    let processor = wrapped.initialized_operator.query_processor()?;

    let query_ctx = ctx.query_context(identifier.0, Uuid::new_v4())?;

    let (bytes, cache_hint) = call_on_generic_raster_processor_gdal_types!(processor, p =>
        raster_stream_to_multiband_geotiff_bytes(
            p,
            query_rect,
            query_ctx,
            GdalGeoTiffDatasetMetadata {
                no_data_value: request_no_data_value,
                spatial_reference: request_spatial_ref,
            },
            GdalGeoTiffOptions {
                compression_num_threads: get_config_element::<crate::config::Gdal>()?.compression_num_threads,
                as_cog: false,
                force_big_tiff: false,
            },
            Some(get_config_element::<crate::config::Wcs>()?.tile_limit),
            conn_closed,
            execution_context.tiling_specification(),
        )
        .await)?
    .map_err(error::Error::from)?;

    Ok(HttpResponse::Ok()
        .append_header(cache_hint.cache_control_header())
        .content_type("image/tiff")
        .body(bytes))
}

fn default_time_from_config() -> TimeInterval {
    get_config_element::<config::Wcs>()
        .ok()
        .and_then(|wcs| wcs.default_time)
        .map_or_else(
            || {
                get_config_element::<config::Ogc>()
                    .ok()
                    .and_then(|ogc| ogc.default_time)
                    .map_or_else(
                        || {
                            TimeInterval::new_instant(
                                geoengine_datatypes::primitives::TimeInstance::now(),
                            )
                            .expect("is a valid time interval")
                        },
                        |time| time.time_interval(),
                    )
            },
            |time| time.time_interval(),
        )
}

#[cfg(test)]
mod tests {
    use crate::contexts::PostgresContext;
    use crate::contexts::Session;
    use crate::ge_context;
    use crate::users::UserAuth;
    use crate::util::tests::register_ndvi_workflow_helper;
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

        let (_, workflow_id) = register_ndvi_workflow_helper(&app_ctx).await;

        let params = &[
            ("service", "WCS"),
            ("request", "GetCapabilities"),
            ("version", "1.1.1"),
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                &workflow_id.to_string(),
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
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 http://127.0.0.1:3030/api/wcs/{workflow_id}/schemas/wcs/1.1.1/wcsGetCapabilities.xsd" updateSequence="152">
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
                                <ows:Get xlink:href="http://127.0.0.1:3030/api/wcs/{workflow_id}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="DescribeCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://127.0.0.1:3030/api/wcs/{workflow_id}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="GetCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://127.0.0.1:3030/api/wcs/{workflow_id}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
            </ows:OperationsMetadata>
            <wcs:Contents>
                <wcs:CoverageSummary>
                    <ows:Title>Workflow {workflow_id}</ows:Title>
                    <ows:WGS84BoundingBox>
                        <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                        <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
                    </ows:WGS84BoundingBox>
                    <wcs:Identifier>{workflow_id}</wcs:Identifier>
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

        let (_, workflow_id) = register_ndvi_workflow_helper(&app_ctx).await;

        let params = &[
            ("service", "WCS"),
            ("request", "DescribeCoverage"),
            ("version", "1.1.1"),
            ("identifiers", &workflow_id.to_string()),
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                &workflow_id.to_string(),
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
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 http://127.0.0.1:3030/api/wcs/{workflow_id}/schemas/wcs/1.1.1/wcsDescribeCoverage.xsd">
        <wcs:CoverageDescription>
            <ows:Title>Workflow {workflow_id}</ows:Title>
            <wcs:Identifier>{workflow_id}</wcs:Identifier>
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

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

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
                &id.to_string(),
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

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

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
                &id.to_string(),
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
