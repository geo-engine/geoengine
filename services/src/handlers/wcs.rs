use std::str::FromStr;

use actix_web::{web, FromRequest, HttpResponse, Responder};
use geoengine_operators::call_on_generic_raster_processor_gdal_types;
use geoengine_operators::util::raster_stream_to_geotiff::{
    raster_stream_to_geotiff_bytes, GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
};
use log::info;
use snafu::{ensure, ResultExt};
use url::Url;

use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartition2D};
use geoengine_datatypes::{primitives::SpatialResolution, spatial_reference::SpatialReference};

use crate::error::Result;
use crate::error::{self, Error};
use crate::handlers::spatial_references::{spatial_reference_specification, AxisOrder};
use crate::handlers::Context;
use crate::ogc::wcs::request::{DescribeCoverage, GetCapabilities, GetCoverage, WcsRequest};
use crate::util::config;
use crate::util::config::get_config_element;
use crate::util::user_input::QueryEx;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;

use geoengine_datatypes::primitives::{TimeInstance, TimeInterval};
use geoengine_operators::engine::ResultDescriptor;
use geoengine_operators::engine::{RasterOperator, RasterQueryRectangle};
use geoengine_operators::processing::{Reprojection, ReprojectionParams};

pub(crate) fn init_wcs_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(web::resource("/wcs/{workflow}").route(web::get().to(wcs_handler::<C>)));
}

async fn wcs_handler<C: Context>(
    workflow: web::Path<WorkflowId>,
    request: QueryEx<WcsRequest>,
    ctx: web::Data<C>,
    session: C::Session,
) -> Result<impl Responder> {
    match request.into_inner() {
        WcsRequest::GetCapabilities(request) => {
            get_capabilities(&request, ctx.get_ref(), workflow.into_inner()).await
        }
        WcsRequest::DescribeCoverage(request) => {
            describe_coverage(&request, ctx.get_ref(), session, workflow.into_inner()).await
        }
        WcsRequest::GetCoverage(request) => {
            get_coverage(&request, ctx.get_ref(), session, workflow.into_inner()).await
        }
    }
}

fn wcs_url(workflow: WorkflowId) -> Result<Url> {
    let base = crate::util::config::get_config_element::<crate::util::config::Web>()?
        .external_address
        .ok_or(Error::ExternalAddressNotConfigured)?;

    base.join("/wcs/")?
        .join(&workflow.to_string())
        .map_err(Into::into)
}

#[allow(clippy::unused_async)]
async fn get_capabilities<C: Context>(
    request: &GetCapabilities,
    _ctx: &C,
    workflow: WorkflowId,
) -> Result<HttpResponse> {
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
    </wcs:Capabilities>"#,
        wcs_url = wcs_url,
        workflow = workflow
    );

    Ok(HttpResponse::Ok().content_type(mime::TEXT_XML).body(mock))
}

async fn describe_coverage<C: Context>(
    request: &DescribeCoverage,
    ctx: &C,
    session: C::Session,
    endpoint: WorkflowId,
) -> Result<HttpResponse> {
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

    let workflow = ctx.workflow_registry_ref().await.load(&identifiers).await?;

    let exe_ctx = ctx.execution_context(session)?;
    let operator = workflow
        .operator
        .get_raster()
        .context(error::Operator)?
        .initialize(&exe_ctx)
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

#[allow(clippy::too_many_lines)]
async fn get_coverage<C: Context>(
    request: &GetCoverage,
    ctx: &C,
    session: C::Session,
    endpoint: WorkflowId,
) -> Result<HttpResponse> {
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
        request.version == "1.1.1" || request.version == "1.1.0",
        error::WcsVersionNotSupported
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

    let workflow = ctx.workflow_registry_ref().await.load(&identifier).await?;

    let operator = workflow.operator.get_raster().context(error::Operator)?;

    let execution_context = ctx.execution_context(session)?;

    let initialized = operator
        .clone()
        .initialize(&execution_context)
        .await
        .context(error::Operator)?;

    // handle request and workflow crs matching
    let workflow_spatial_ref: Option<SpatialReference> =
        initialized.result_descriptor().spatial_reference().into();
    let workflow_spatial_ref = workflow_spatial_ref.ok_or(error::Error::InvalidSpatialReference)?;

    let request_spatial_ref: SpatialReference = request.gridbasecrs;

    // perform reprojection if necessary
    let initialized = if request_spatial_ref == workflow_spatial_ref {
        initialized
    } else {
        let proj = Reprojection {
            params: ReprojectionParams {
                target_spatial_reference: request_spatial_ref,
            },
            sources: operator.into(),
        };

        // TODO: avoid re-initialization of the whole operator graph
        Box::new(proj)
            .initialize(&execution_context)
            .await
            .context(error::Operator)?
    };

    let no_data_value: Option<f64> = initialized.result_descriptor().no_data_value;

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

    let query_rect: RasterQueryRectangle = RasterQueryRectangle {
        spatial_bounds: request_partition,
        time_interval: request.time.unwrap_or_else(default_time_from_config),
        spatial_resolution,
    };

    let query_ctx = ctx.query_context()?;

    let bytes = call_on_generic_raster_processor_gdal_types!(processor, p =>
        raster_stream_to_geotiff_bytes(
            p,
            query_rect,
            query_ctx,
            GdalGeoTiffDatasetMetadata {
                no_data_value,
                spatial_reference: request_spatial_ref,
            },
            GdalGeoTiffOptions {
                compression_num_threads: get_config_element::<crate::util::config::Gdal>()?.compression_num_threads,
                as_cog: false,
            },
            Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
        )
        .await)?
    .map_err(error::Error::from)?;

    Ok(HttpResponse::Ok().content_type("image/tiff").body(bytes))
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
                            TimeInterval::new_instant(TimeInstance::now())
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
    use crate::contexts::{InMemoryContext, Session, SimpleContext};
    use crate::util::tests::{read_body_string, register_ndvi_workflow_helper, send_test_request};
    use actix_web::http::header;
    use actix_web::test;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::raster::{GridShape2D, TilingSpecification};
    use geoengine_datatypes::util::test::TestDefault;

    #[tokio::test]
    async fn get_capabilities() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let params = &[
            ("service", "WCS"),
            ("request", "GetCapabilities"),
            ("version", "1.1.1"),
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                &id.to_string(),
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

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
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 http://localhost:3030/wcs/{workflow_id}/schemas/wcs/1.1.1/wcsGetCapabilities.xsd" updateSequence="152">
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
                                <ows:Get xlink:href="http://localhost:3030/wcs/{workflow_id}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="DescribeCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/{workflow_id}?"/>
                        </ows:HTTP>
                    </ows:DCP>
                </ows:Operation>
                <ows:Operation name="GetCoverage">
                    <ows:DCP>
                        <ows:HTTP>
                                <ows:Get xlink:href="http://localhost:3030/wcs/{workflow_id}?"/>
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
    </wcs:Capabilities>"#,
                workflow_id = id
            ),
            body
        );
    }

    #[tokio::test]
    async fn describe_coverage() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let params = &[
            ("service", "WCS"),
            ("request", "DescribeCoverage"),
            ("version", "1.1.1"),
            ("identifiers", &id.to_string()),
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                &id.to_string(),
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

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
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/wcs/1.1.1 http://localhost:3030/wcs/{workflow_id}/schemas/wcs/1.1.1/wcsDescribeCoverage.xsd">
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
    </wcs:CoverageDescriptions>"#,
                workflow_id = id
            ),
            body
        );
    }

    #[tokio::test]
    async fn get_coverage() {
        let exe_ctx_tiling_spec = TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        };

        // override the pixel size since this test was designed for 600 x 600 pixel tiles
        let ctx = InMemoryContext::new_with_context_spec(
            exe_ctx_tiling_spec,
            TestDefault::test_default(),
        );
        let session_id = ctx.default_session_ref().await.id();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

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
        ];

        let req = test::TestRequest::get()
            .uri(&format!(
                "/wcs/{}?{}",
                &id.to_string(),
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../test_data/raster/geotiff_from_stream_compressed.tiff")
                as &[u8],
            test::read_body(res).await.as_ref()
        );
    }
}
