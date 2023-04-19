use std::str::FromStr;
use std::time::Duration;

use actix_web::{web, FromRequest, HttpRequest, HttpResponse};
use geoengine_operators::call_on_generic_raster_processor_gdal_types;
use geoengine_operators::util::raster_stream_to_geotiff::{
    raster_stream_to_multiband_geotiff_bytes, GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
};
use log::info;
use snafu::{ensure, ResultExt};
use url::Url;

use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, RasterQueryRectangle, SpatialPartition2D,
};
use geoengine_datatypes::{primitives::SpatialResolution, spatial_reference::SpatialReference};
use utoipa::openapi::{KnownFormat, ObjectBuilder, SchemaFormat, SchemaType};
use utoipa::ToSchema;

use crate::api::model::datatypes::TimeInterval;
use crate::contexts::ApplicationContext;
use crate::error::Result;
use crate::error::{self, Error};
use crate::handlers::spatial_references::{spatial_reference_specification, AxisOrder};
use crate::handlers::SessionContext;
use crate::ogc::util::{ogc_endpoint_url, OgcProtocol, OgcRequestGuard};
use crate::ogc::wcs::request::{DescribeCoverage, GetCapabilities, GetCoverage, WcsVersion};
use crate::util::config;
use crate::util::config::get_config_element;
use crate::util::server::{connection_closed, not_implemented_handler};
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;

use geoengine_operators::engine::ResultDescriptor;
use geoengine_operators::engine::{ExecutionContext, WorkflowOperatorPath};
use geoengine_operators::processing::{InitializedRasterReprojection, ReprojectionParams};

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
    let web_config = crate::util::config::get_config_element::<crate::util::config::Web>()?;
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
#[allow(clippy::unused_async)]
async fn wcs_capabilities_handler<C: ApplicationContext>(
    workflow: web::Path<WorkflowId>,
    request: web::Query<GetCapabilities>,
    _app_ctx: web::Data<C>,
    _session: C::Session,
) -> Result<HttpResponse> {
    let workflow = workflow.into_inner();

    info!("{:?}", request);

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
async fn wcs_describe_coverage_handler<C: ApplicationContext>(
    workflow: web::Path<WorkflowId>,
    request: web::Query<DescribeCoverage>,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    let endpoint = workflow.into_inner();

    info!("{:?}", request);

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
        .get_raster()
        .context(error::Operator)?
        .initialize(workflow_operator_path_root, &exe_ctx)
        .await
        .context(error::Operator)?;

    let result_descriptor = operator.result_descriptor();

    let spatial_reference: Option<SpatialReference> = result_descriptor.spatial_reference.into();
    let spatial_reference = spatial_reference.ok_or(error::Error::MissingSpatialReference)?;

    // TODO: give tighter bounds if possible
    let area_of_use: SpatialPartition2D = spatial_reference
        .area_of_use_projected()
        .context(error::DataType)?;

    let (bbox_ll_0, bbox_ll_1, bbox_ur_0, bbox_ur_1) =
        match spatial_reference_specification(&spatial_reference.proj_string()?)?
            .axis_order
            .ok_or(Error::AxisOrderingNotKnownForSrs {
                srs_string: spatial_reference.srs_string(),
            })? {
            AxisOrder::EastNorth => (
                area_of_use.lower_left().x,
                area_of_use.lower_left().y,
                area_of_use.upper_right().x,
                area_of_use.upper_right().y,
            ),
            AxisOrder::NorthEast => (
                area_of_use.lower_left().y,
                area_of_use.lower_left().x,
                area_of_use.upper_right().y,
                area_of_use.upper_right().x,
            ),
        };

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
                    <wcs:GridCRS>
                        <wcs:GridBaseCRS>urn:ogc:def:crs:{srs_authority}::{srs_code}</wcs:GridBaseCRS>
                        <wcs:GridType>urn:ogc:def:method:WCS:1.1:2dGridIn2dCrs</wcs:GridType>
                        <wcs:GridOrigin>{origin_x} {origin_y}</wcs:GridOrigin>
                        <wcs:GridOffsets>0 0.0 0.0 -0</wcs:GridOffsets>
                        <wcs:GridCS>urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS</wcs:GridCS>
                    </wcs:GridCRS>
                </wcs:SpatialDomain>
            </wcs:Domain>
            <wcs:SupportedCRS>{srs_authority}:{srs_code}</wcs:SupportedCRS>
            <wcs:SupportedFormat>image/tiff</wcs:SupportedFormat>
        </wcs:CoverageDescription>
    </wcs:CoverageDescriptions>"#,
        wcs_url = wcs_url,
        workflow_id = identifiers,
        srs_authority = spatial_reference.authority(),
        srs_code = spatial_reference.code(),
        origin_x = area_of_use.upper_left().x,
        origin_y = area_of_use.upper_left().y,
        bbox_ll_0 = bbox_ll_0,
        bbox_ll_1 = bbox_ll_1,
        bbox_ur_0 = bbox_ur_0,
        bbox_ur_1 = bbox_ur_1,
    );

    Ok(HttpResponse::Ok().content_type(mime::TEXT_XML).body(mock))
}

/// Get WCS Coverage
#[utoipa::path(
    tag = "OGC WCS",
    get,
    path = "/wcs/{workflow}?request=GetCoverage",
    responses(
        (status = 200, description = "OK", content_type= "image/png", body = CoverageResponse, example = json!("image bytes")),
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

    info!("{:?}", request);

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

    let request_partition = request.spatial_partition()?;

    if let Some(gridorigin) = request.gridorigin {
        ensure!(
            gridorigin.coordinate(request.gridbasecrs)? == request_partition.upper_left(),
            error::WcsGridOriginMustEqualBoundingboxUpperLeft
        );
    }

    if let Some(bbox_spatial_reference) = request.boundingbox.spatial_reference {
        ensure!(
            request.gridbasecrs == bbox_spatial_reference,
            error::WcsBoundingboxCrsMustEqualGridBaseCrs
        );
    }

    let ctx = app_ctx.session_context(session);

    let workflow = ctx.db().load_workflow(&identifier).await?;

    let operator = workflow.operator.get_raster().context(error::Operator)?;

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

    let request_spatial_ref: SpatialReference = request.gridbasecrs.into();
    let request_no_data_value = request.nodatavalue;

    // perform reprojection if necessary
    let initialized = if request_spatial_ref == workflow_spatial_ref {
        initialized
    } else {
        log::debug!(
            "WCS query srs: {}, workflow srs: {} --> injecting reprojection",
            request_spatial_ref,
            workflow_spatial_ref
        );
        let irp = InitializedRasterReprojection::try_new_with_input(
            ReprojectionParams {
                target_spatial_reference: request_spatial_ref,
            },
            initialized,
            execution_context.tiling_specification(),
        )
        .context(error::Operator)?;

        Box::new(irp)
    };

    let processor = initialized.query_processor().context(error::Operator)?;

    let spatial_resolution: SpatialResolution =
        if let Some(spatial_resolution) = request.spatial_resolution() {
            spatial_resolution?
        } else {
            // TODO: proper default resolution
            SpatialResolution {
                x: request_partition.size_x() / 256.,
                y: request_partition.size_y() / 256.,
            }
        };

    let query_rect = RasterQueryRectangle {
        spatial_bounds: request_partition,
        time_interval: request.time.unwrap_or_else(default_time_from_config).into(),
        spatial_resolution,
    };

    let query_ctx = ctx.query_context()?;

    let bytes = call_on_generic_raster_processor_gdal_types!(processor, p =>
        raster_stream_to_multiband_geotiff_bytes(
            p,
            query_rect,
            query_ctx,
            GdalGeoTiffDatasetMetadata {
                no_data_value: request_no_data_value,
                spatial_reference: request_spatial_ref,
            },
            GdalGeoTiffOptions {
                compression_num_threads: get_config_element::<crate::util::config::Gdal>()?.compression_num_threads,
                as_cog: false,
                force_big_tiff: false,
            },
            Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
            conn_closed,
            execution_context.tiling_specification(),
        )
        .await)?
    .map_err(error::Error::from)?;

    Ok(HttpResponse::Ok().content_type("image/tiff").body(bytes))
}

pub struct CoverageResponse {}

impl<'a> ToSchema<'a> for CoverageResponse {
    fn schema() -> (&'a str, utoipa::openapi::RefOr<utoipa::openapi::Schema>) {
        (
            "CoverageResponse",
            ObjectBuilder::new()
                .schema_type(SchemaType::String)
                .format(Some(SchemaFormat::KnownFormat(KnownFormat::Binary)))
                .into(),
        )
    }
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
    use crate::contexts::{InMemoryContext, Session, SessionContext, SimpleApplicationContext};
    use crate::util::tests::{read_body_string, register_ndvi_workflow_helper, send_test_request};
    use actix_web::http::header;
    use actix_web::test;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::raster::{GridShape2D, TilingSpecification};
    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn get_capabilities() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session_id = ctx.session().id();

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

        assert_eq!(res.status(), 200);
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

    #[tokio::test]
    async fn describe_coverage() {
        let app_ctx = InMemoryContext::test_default();

        let ctx = app_ctx.default_session_context().await;
        let session_id = ctx.session().id();

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

        assert_eq!(res.status(), 200);
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
                    <wcs:GridCRS>
                        <wcs:GridBaseCRS>urn:ogc:def:crs:EPSG::4326</wcs:GridBaseCRS>
                        <wcs:GridType>urn:ogc:def:method:WCS:1.1:2dGridIn2dCrs</wcs:GridType>
                        <wcs:GridOrigin>-180 90</wcs:GridOrigin>
                        <wcs:GridOffsets>0 0.0 0.0 -0</wcs:GridOffsets>
                        <wcs:GridCS>urn:ogc:def:cs:OGC:0.0:Grid2dSquareCS</wcs:GridCS>
                    </wcs:GridCRS>
                </wcs:SpatialDomain>
            </wcs:Domain>
            <wcs:SupportedCRS>EPSG:4326</wcs:SupportedCRS>
            <wcs:SupportedFormat>image/tiff</wcs:SupportedFormat>
        </wcs:CoverageDescription>
    </wcs:CoverageDescriptions>"#
            ),
            body
        );
    }

    // TODO: add get_coverage with masked band

    #[tokio::test]
    async fn get_coverage_with_nodatavalue() {
        let exe_ctx_tiling_spec = TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        };

        // override the pixel size since this test was designed for 600 x 600 pixel tiles
        let app_ctx = InMemoryContext::new_with_context_spec(
            exe_ctx_tiling_spec,
            TestDefault::test_default(),
        );
        let ctx = app_ctx.default_session_context().await;
        let session_id = ctx.session().id();

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

        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../test_data/raster/geotiff_from_stream_compressed.tiff")
                as &[u8],
            test::read_body(res).await.as_ref()
        );
    }
}
