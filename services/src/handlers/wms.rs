use actix_web::{web, FromRequest, HttpRequest, HttpResponse};
use reqwest::Url;
use serde_json::json;
use snafu::{ensure, ResultExt};

use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, RasterQueryRectangle, SpatialPartition2D,
};
use geoengine_datatypes::{operations::image::Colorizer, primitives::SpatialResolution};
use utoipa::openapi::{KnownFormat, ObjectBuilder, SchemaFormat, SchemaType};
use utoipa::ToSchema;

use crate::api::model::datatypes::{SpatialReference, SpatialReferenceOption, TimeInterval};
use crate::error::Result;
use crate::error::{self, Error};
use crate::handlers::Context;
use crate::ogc::util::{ogc_endpoint_url, OgcProtocol, OgcRequestGuard};
use crate::ogc::wms::request::{GetCapabilities, GetLegendGraphic, GetMap, GetMapExceptionFormat};
use crate::util::config;
use crate::util::config::get_config_element;
use crate::util::server::{connection_closed, not_implemented_handler};
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;

use geoengine_operators::engine::{ExecutionContext, ResultDescriptor};
use geoengine_operators::processing::{InitializedRasterReprojection, ReprojectionParams};
use geoengine_operators::{
    call_on_generic_raster_processor, util::raster_stream_to_png::raster_stream_to_png_bytes,
};
use std::str::FromStr;
use std::time::Duration;

pub(crate) fn init_wms_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: Context,
    C::Session: FromRequest,
{
    cfg.service(
        web::resource("/wms/{workflow}")
            .route(
                web::get()
                    .guard(OgcRequestGuard::new("GetCapabilities"))
                    .to(wms_capabilities_handler::<C>),
            )
            .route(
                web::get()
                    .guard(OgcRequestGuard::new("GetMap"))
                    .to(wms_map_handler::<C>),
            )
            .route(
                web::get()
                    .guard(OgcRequestGuard::new("GetLegendGraphic"))
                    .to(wms_legend_graphic_handler::<C>),
            )
            .route(web::get().to(not_implemented_handler)),
    );
}

/// Get WMS Capabilities
#[utoipa::path(
    tag = "OGC WMS",
    get,
    path = "/wms/{workflow}?request=GetCapabilities",
    responses(
        (status = 200, description = "OK", content_type = "text/xml", body = String,
            // TODO: add example when utoipa supports more than just json examples
            // example = r#"<WMS_Capabilities 
            // xmlns="http://www.opengis.net/wms" 
            // xmlns:sld="http://www.opengis.net/sld" 
            // xmlns:xlink="http://www.w3.org/1999/xlink" 
            // xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.3.0" xsi:schemaLocation="http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/sld_capabilities.xsd">
            // <Service>
            //   <Name>WMS</Name>
            //   <Title>Geo Engine WMS</Title>
            //   <OnlineResource 
            //     xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="http://127.0.0.1:3030/wms/b709b27b-dea5-5a27-a074-ae3366c49498"/>
            //   </Service>
            //   <Capability>
            //     <Request>
            //       <GetCapabilities>
            //         <Format>text/xml</Format>
            //         <DCPType>
            //           <HTTP>
            //             <Get>
            //               <OnlineResource xlink:href="http://127.0.0.1:3030/wms/b709b27b-dea5-5a27-a074-ae3366c49498"/>
            //             </Get>
            //           </HTTP>
            //         </DCPType>
            //       </GetCapabilities>
            //       <GetMap>
            //         <Format>image/png</Format>
            //         <DCPType>
            //           <HTTP>
            //             <Get>
            //               <OnlineResource xlink:href="http://127.0.0.1:3030/wms/b709b27b-dea5-5a27-a074-ae3366c49498"/>
            //             </Get>
            //           </HTTP>
            //         </DCPType>
            //       </GetMap>
            //     </Request>
            //     <Exception>
            //       <Format>XML</Format>
            //     </Exception>
            //     <Layer queryable="1">
            //       <Name>b709b27b-dea5-5a27-a074-ae3366c49498</Name>
            //       <Title>Workflow b709b27b-dea5-5a27-a074-ae3366c49498</Title>
            //       <CRS>EPSG:3857</CRS>
            //       <EX_GeographicBoundingBox>
            //         <westBoundLongitude>-180</westBoundLongitude>
            //         <eastBoundLongitude>180</eastBoundLongitude>
            //         <southBoundLatitude>-90</southBoundLatitude>
            //         <northBoundLatitude>90</northBoundLatitude>
            //       </EX_GeographicBoundingBox>
            //       <BoundingBox CRS="EPSG:4326" minx="-90.0" miny="-180.0" maxx="90.0" maxy="180.0"/>
            //     </Layer>
            //   </Capability>
            // </WMS_Capabilities>"#
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
async fn wms_capabilities_handler<C>(
    workflow: web::Path<WorkflowId>,
    _request: web::Query<GetCapabilities>,
    ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse>
where
    C: Context,
{
    let workflow_id = workflow.into_inner();
    let wms_url = wms_url(workflow_id)?;

    let workflow = ctx.workflow_registry_ref().load(&workflow_id).await?;

    let exe_ctx = ctx.execution_context(session)?;
    let operator = workflow
        .operator
        .get_raster()
        .context(error::Operator)?
        .initialize(&exe_ctx)
        .await
        .context(error::Operator)?;

    let result_descriptor = operator.result_descriptor();

    let spatial_reference: SpatialReferenceOption = result_descriptor.spatial_reference.into();
    let spatial_reference: Option<SpatialReference> = spatial_reference.into();
    let spatial_reference = spatial_reference.ok_or(error::Error::MissingSpatialReference)?;

    let response = format!(
        r#"<WMS_Capabilities xmlns="http://www.opengis.net/wms" xmlns:sld="http://www.opengis.net/sld" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.3.0" xsi:schemaLocation="http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/sld_capabilities.xsd">
    <Service>
        <Name>WMS</Name>
        <Title>Geo Engine WMS</Title>
        <OnlineResource xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="{wms_url}"/>
    </Service>
    <Capability>
        <Request>
            <GetCapabilities>
                <Format>text/xml</Format>
                <DCPType>
                    <HTTP>
                        <Get>
                            <OnlineResource xlink:href="{wms_url}"/>
                        </Get>
                    </HTTP>
                </DCPType>
            </GetCapabilities>
            <GetMap>
                <Format>image/png</Format>
                <DCPType>
                    <HTTP>
                        <Get>
                            <OnlineResource xlink:href="{wms_url}"/>
                        </Get>
                    </HTTP>
                </DCPType>
            </GetMap>
        </Request>
        <Exception>
            <Format>XML</Format>
            <Format>JSON</Format>
        </Exception>
        <Layer queryable="1">
            <Name>{workflow}</Name>
            <Title>Workflow {workflow}</Title>
            <CRS>{srs_authority}:{srs_code}</CRS>
            <EX_GeographicBoundingBox>
                <westBoundLongitude>-180</westBoundLongitude>
                <eastBoundLongitude>180</eastBoundLongitude>
                <southBoundLatitude>-90</southBoundLatitude>
                <northBoundLatitude>90</northBoundLatitude>
            </EX_GeographicBoundingBox>
            <BoundingBox CRS="EPSG:4326" minx="-90.0" miny="-180.0" maxx="90.0" maxy="180.0"/>
        </Layer>
    </Capability>
</WMS_Capabilities>"#,
        wms_url = wms_url,
        workflow = workflow_id,
        srs_authority = spatial_reference.authority(),
        srs_code = spatial_reference.code()
    );

    Ok(HttpResponse::Ok()
        .content_type(mime::TEXT_XML)
        .body(response))
}

fn wms_url(workflow: WorkflowId) -> Result<Url> {
    let web_config = crate::util::config::get_config_element::<crate::util::config::Web>()?;
    let base = web_config
        .external_address
        .unwrap_or(Url::parse(&format!("http://{}/", web_config.bind_address))?);

    ogc_endpoint_url(&base, OgcProtocol::Wms, workflow)
}

/// Get WMS Map
#[utoipa::path(
    tag = "OGC WMS",
    get,
    path = "/wms/{workflow}?request=GetMap",
    responses(
        (status = 200, description = "OK", content_type= "image/png", body = MapResponse, example = json!("image bytes")),
    ),
    params(
        ("workflow" = WorkflowId, description = "Workflow id"),
        GetMap
    ),
    security(
        ("session_token" = [])
    )
)]
async fn wms_map_handler<C: Context>(
    req: HttpRequest,
    workflow: web::Path<WorkflowId>,
    request: web::Query<GetMap>,
    ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    async fn compute_result<C: Context>(
        req: HttpRequest,
        workflow: web::Path<WorkflowId>,
        request: &web::Query<GetMap>,
        ctx: web::Data<C>,
        session: C::Session,
    ) -> Result<Vec<u8>> {
        let endpoint = workflow.into_inner();
        let layer = WorkflowId::from_str(&request.layers)?;

        ensure!(
            endpoint == layer,
            error::WMSEndpointLayerMissmatch { endpoint, layer }
        );

        // TODO: validate request further

        let conn_closed = connection_closed(
            &req,
            config::get_config_element::<config::Wms>()?
                .request_timeout_seconds
                .map(Duration::from_secs),
        );

        let workflow = ctx
            .workflow_registry_ref()
            .load(&WorkflowId::from_str(&request.layers)?)
            .await?;

        let operator = workflow.operator.get_raster().context(error::Operator)?;

        let execution_context = ctx.execution_context(session.clone())?;

        let initialized = operator
            .clone()
            .initialize(&execution_context)
            .await
            .context(error::Operator)?;

        // handle request and workflow crs matching
        let workflow_spatial_ref: SpatialReferenceOption =
            initialized.result_descriptor().spatial_reference().into();
        let workflow_spatial_ref: Option<SpatialReference> = workflow_spatial_ref.into();
        let workflow_spatial_ref =
            workflow_spatial_ref.ok_or(error::Error::InvalidSpatialReference)?;

        // TODO: use a default spatial reference if it is not set?
        let request_spatial_ref: SpatialReference =
            request.crs.ok_or(error::Error::MissingSpatialReference)?;

        // perform reprojection if necessary
        let initialized = if request_spatial_ref == workflow_spatial_ref {
            initialized
        } else {
            log::debug!(
                "WMS query srs: {}, workflow srs: {} --> injecting reprojection",
                request_spatial_ref,
                workflow_spatial_ref
            );
            let irp = InitializedRasterReprojection::try_new_with_input(
                ReprojectionParams {
                    target_spatial_reference: request_spatial_ref.into(),
                },
                initialized,
                execution_context.tiling_specification(),
            )
            .context(error::Operator)?;

            Box::new(irp)
        };

        let processor = initialized.query_processor().context(error::Operator)?;

        let query_bbox: SpatialPartition2D = request.bbox.bounds(request_spatial_ref)?;
        let x_query_resolution = query_bbox.size_x() / f64::from(request.width);
        let y_query_resolution = query_bbox.size_y() / f64::from(request.height);

        let query_rect = RasterQueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: request.time.unwrap_or_else(default_time_from_config).into(),
            spatial_resolution: SpatialResolution::new_unchecked(
                x_query_resolution,
                y_query_resolution,
            ),
        };

        let query_ctx = ctx.query_context(session)?;

        let colorizer = colorizer_from_style(&request.styles)?;

        call_on_generic_raster_processor!(
            processor,
            p =>
                raster_stream_to_png_bytes(p, query_rect, query_ctx, request.width, request.height, request.time.map(Into::into), colorizer, conn_closed).await
        ).map_err(error::Error::from)
    }

    match compute_result(req, workflow, &request, ctx, session).await {
        Ok(image_bytes) => Ok(HttpResponse::Ok()
            .content_type(mime::IMAGE_PNG)
            .body(image_bytes)),
        Err(error) => Ok(handle_wms_error(request.exceptions, &error)),
    }
}

fn handle_wms_error(
    exception_format: Option<GetMapExceptionFormat>,
    error: &Error,
) -> HttpResponse {
    let exception_format = exception_format.unwrap_or(GetMapExceptionFormat::Xml);

    match exception_format {
        GetMapExceptionFormat::Xml => {
            let body = format!(
                r#"
<?xml version="1.0" encoding="UTF-8"?>
    <ServiceExceptionReport version="1.3.0" xmlns="http://www.opengis.net/ogc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/ogc https://cdc.dwd.de/geoserver/schemas/wms/1.3.0/exceptions_1_3_0.xsd">
    <ServiceException>
        {error}
    </ServiceException>
</ServiceExceptionReport>"#
            );

            HttpResponse::Ok().content_type(mime::TEXT_XML).body(body)
        }
        GetMapExceptionFormat::Json => HttpResponse::Ok().json(
            json!({"error": Into::<&str>::into(error).to_string(), "message": error.to_string() }),
        ),
    }
}

pub struct MapResponse {}

impl ToSchema for MapResponse {
    fn schema() -> utoipa::openapi::schema::Schema {
        ObjectBuilder::new()
            .schema_type(SchemaType::String)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Binary)))
            .into()
    }
}

fn colorizer_from_style(styles: &str) -> Result<Option<Colorizer>> {
    match styles.strip_prefix("custom:") {
        None => Ok(None),
        Some(suffix) => serde_json::from_str(suffix).map_err(error::Error::from),
    }
}

/// Get WMS Legend Graphic
#[utoipa::path(
    tag = "OGC WMS",
    get,
    path = "/wms/{workflow}?request=GetLegendGraphic",
    responses(
        (status = 501, description = "Not implemented")
    ),
    params(
        ("workflow" = WorkflowId, description = "Workflow id"),
        GetLegendGraphic
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::unused_async)] // required by handler signature
async fn wms_legend_graphic_handler<C: Context>(
    _workflow: web::Path<WorkflowId>,
    _request: web::Query<GetLegendGraphic>,
    _ctx: web::Data<C>,
    _session: C::Session,
) -> HttpResponse {
    HttpResponse::NotImplemented().finish()
}

fn default_time_from_config() -> TimeInterval {
    get_config_element::<config::Wms>()
        .ok()
        .and_then(|wms| wms.default_time)
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
    use crate::contexts::{
        InMemoryContext, Session, SimpleContext, SimpleSession, /*SimpleSession*/
    };
    use crate::handlers::ErrorResponse;
    use crate::util::tests::{
        check_allowed_http_methods, read_body_string, register_ndvi_workflow_helper,
        send_test_request,
    };
    use actix_web::dev::ServiceResponse;
    use actix_web::http::header;
    use actix_web::http::Method;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::operations::image::{DefaultColors, RgbaColor};
    use geoengine_datatypes::raster::{GridShape2D, TilingSpecification};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_operators::engine::{ExecutionContext, RasterQueryProcessor};
    use geoengine_operators::source::GdalSourceProcessor;
    use geoengine_operators::util::gdal::create_ndvi_meta_data;
    use std::convert::TryInto;
    use std::marker::PhantomData;
    use xml::ParserConfig;

    async fn test_test_helper(method: Method, path: Option<&str>) -> ServiceResponse {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let req = actix_web::test::TestRequest::default()
            .method(method)
            .uri(path.unwrap_or("/wms/df756642-c5a3-4d72-8ad7-629d312ae993?request=GetMap&service=WMS&version=1.3.0&layers=df756642-c5a3-4d72-8ad7-629d312ae993&bbox=1,2,3,4&width=100&height=100&crs=EPSG:4326&styles=ssss&format=image/png"))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn test_invalid_method() {
        check_allowed_http_methods(|method| test_test_helper(method, None), &[Method::GET]).await;
    }

    #[tokio::test]
    async fn test_missing_fields() {
        let res = test_test_helper(Method::GET, Some("/wms/df756642-c5a3-4d72-8ad7-629d312ae993?service=WMS&request=GetMap&version=1.3.0&bbox=1,2,3,4&width=100&height=100&crs=EPSG:4326&styles=ssss&format=image/png")).await;

        ErrorResponse::assert(
            res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: missing field `layers`",
        )
        .await;
    }

    #[tokio::test]
    async fn test_invalid_fields() {
        let res = test_test_helper(Method::GET, Some("/wms/df756642-c5a3-4d72-8ad7-629d312ae993?request=GetMap&service=WMS&version=1.3.0&layers=df756642-c5a3-4d72-8ad7-629d312ae993&bbox=1,2,3,4&width=XYZ&height=100&crs=EPSG:4326&styles=ssss&format=image/png")).await;

        ErrorResponse::assert(
            res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: could not parse string",
        )
        .await;
    }

    async fn get_capabilities_test_helper(method: Method) -> ServiceResponse {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let req = actix_web::test::TestRequest::with_uri(&format!(
            "/wms/{id}?request=GetCapabilities&service=WMS"
        ))
        .method(method)
        .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn test_get_capabilities() {
        let res = get_capabilities_test_helper(Method::GET).await;

        assert_eq!(res.status(), 200);

        // TODO: validate against schema
        let body = actix_web::test::read_body(res).await;
        let reader = ParserConfig::default().create_reader(body.as_ref());

        for event in reader {
            assert!(event.is_ok());
        }
    }

    #[tokio::test]
    async fn get_capabilities_invalid_method() {
        check_allowed_http_methods(get_capabilities_test_helper, &[Method::GET]).await;
    }

    // The result should be similar to the GDAL output of this command: gdalwarp -tr 1 1 -r near -srcnodata 0 -dstnodata 0  MOD13A2_M_NDVI_2014-01-01.TIFF MOD13A2_M_NDVI_2014-01-01_360_180_near_0.TIFF
    #[tokio::test]
    async fn png_from_stream_non_full() {
        let ctx = InMemoryContext::test_default();
        let exe_ctx = ctx.execution_context(SimpleSession::default()).unwrap();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification: exe_ctx.tiling_specification(),
            meta_data: Box::new(create_ndvi_meta_data()),
            _phantom_data: PhantomData,
        };

        let query_partition =
            SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let image_bytes = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_partition,
                time_interval: geoengine_datatypes::primitives::TimeInterval::new(
                    1_388_534_400_000,
                    1_388_534_400_000 + 1000,
                )
                .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(1.0, 1.0),
            },
            ctx.query_context(SimpleSession::default()).unwrap(),
            360,
            180,
            None,
            None,
            Box::pin(futures::future::pending()),
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "raster_small.png");

        assert_eq!(
            include_bytes!("../../../test_data/wms/raster_small.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    async fn get_map_test_helper(method: Method, path: Option<&str>) -> ServiceResponse {
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

        let req = actix_web::test::TestRequest::with_uri(path.unwrap_or(&format!("/wms/{id}?request=GetMap&service=WMS&version=1.3.0&layers={id}&bbox=20,-10,80,50&width=600&height=600&crs=EPSG:4326&styles=ssss&format=image/png&time=2014-01-01T00:00:00.0Z", id = id.to_string())))
            .method(method)
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, ctx).await
    }

    #[tokio::test]
    async fn get_map() {
        let res = get_map_test_helper(Method::GET, None).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "get_map.png");

        assert_eq!(
            include_bytes!("../../../test_data/wms/get_map.png") as &[u8],
            image_bytes
        );
    }

    #[tokio::test]
    async fn get_map_ndvi() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let req = actix_web::test::TestRequest::get().uri(&format!("/wms/{id}?service=WMS&version=1.3.0&request=GetMap&layers={id}&styles=&width=335&height=168&crs=EPSG:4326&bbox=-90.0,-180.0,90.0,180.0&format=image/png&transparent=FALSE&bgcolor=0xFFFFFF&exceptions=application/json&time=2014-04-01T12%3A00%3A00.000%2B00%3A00", id = id.to_string())).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, ctx).await;

        assert_eq!(
            response.status(),
            200,
            "{:?}",
            actix_web::test::read_body(response).await
        );

        let image_bytes = actix_web::test::read_body(response).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "get_map_ndvi.png");

        assert_eq!(
            include_bytes!("../../../test_data/wms/get_map_ndvi.png") as &[u8],
            image_bytes
        );
    }

    ///Actix uses serde_urlencoded inside web::Query which does not support this
    #[tokio::test]
    async fn get_map_uppercase() {
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

        let req = actix_web::test::TestRequest::get().uri(&format!("/wms/{id}?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS={id}&CRS=EPSG:4326&STYLES=&WIDTH=600&HEIGHT=600&BBOX=20,-10,80,50&time=2014-01-01T00:00:00.0Z", id = id.to_string())).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "get_map.png");

        assert_eq!(
            include_bytes!("../../../test_data/wms/get_map.png") as &[u8],
            image_bytes
        );
    }

    #[tokio::test]
    async fn get_map_invalid_method() {
        check_allowed_http_methods(|method| get_map_test_helper(method, None), &[Method::GET])
            .await;
    }

    #[tokio::test]
    async fn get_map_missing_fields() {
        let res = get_map_test_helper(Method::GET, Some("/wms/df756642-c5a3-4d72-8ad7-629d312ae993?request=GetMap&service=WMS&version=1.3.0&bbox=20,-10,80,50&width=600&height=600&crs=EPSG:4326&styles=ssss&format=image/png&time=2014-01-01T00:00:00.0Z")).await;

        ErrorResponse::assert(
            res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: missing field `layers`",
        )
        .await;
    }

    #[tokio::test]
    async fn get_map_colorizer() {
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

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (1.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            DefaultColors::OverUnder {
                over_color: RgbaColor::white(),
                under_color: RgbaColor::black(),
            },
        )
        .unwrap();

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            ("bbox", "20,-10,80,50"),
            ("width", "600"),
            ("height", "600"),
            ("crs", "EPSG:4326"),
            (
                "styles",
                &format!("custom:{}", serde_json::to_string(&colorizer).unwrap()),
            ),
            ("format", "image/png"),
            ("time", "2014-01-01T00:00:00.0Z"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "get_map_colorizer.png");

        assert_eq!(
            include_bytes!("../../../test_data/wms/get_map_colorizer.png") as &[u8],
            image_bytes
        );
    }

    #[tokio::test]
    async fn it_zoomes_very_far() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            DefaultColors::OverUnder {
                over_color: RgbaColor::white(),
                under_color: RgbaColor::black(),
            },
        )
        .unwrap();

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            (
                "bbox",
                "1.95556640625,0.90087890625,1.9775390625,0.9228515625",
            ),
            ("width", "256"),
            ("height", "256"),
            ("crs", "EPSG:4326"),
            (
                "styles",
                &format!("custom:{}", serde_json::to_string(&colorizer).unwrap()),
            ),
            ("format", "image/png"),
            ("time", "2014-04-01T12:00:00.0Z"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);
    }

    #[tokio::test]
    async fn default_error() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            DefaultColors::OverUnder {
                over_color: RgbaColor::white(),
                under_color: RgbaColor::black(),
            },
        )
        .unwrap();

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            (
                "bbox",
                "1.95556640625,0.90087890625,1.9775390625,0.9228515625",
            ),
            ("width", "256"),
            ("height", "256"),
            ("crs", "EPSG:432"),
            (
                "styles",
                &format!("custom:{}", serde_json::to_string(&colorizer).unwrap()),
            ),
            ("format", "image/png"),
            ("time", "2014-04-01T12:00:00.0Z"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        assert_eq!(res.status(), 200);
        let body = read_body_string(res).await;

        assert_eq!(
            body,
            r#"
<?xml version="1.0" encoding="UTF-8"?>
    <ServiceExceptionReport version="1.3.0" xmlns="http://www.opengis.net/ogc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/ogc https://cdc.dwd.de/geoserver/schemas/wms/1.3.0/exceptions_1_3_0.xsd">
    <ServiceException>
        Operator: DataTypeError: No CoordinateProjector available for: SpatialReference { authority: Epsg, code: 4326 } --> SpatialReference { authority: Epsg, code: 432 }
    </ServiceException>
</ServiceExceptionReport>"#
        );
    }

    #[tokio::test]
    async fn json_error() {
        let ctx = InMemoryContext::test_default();
        let session_id = ctx.default_session_ref().await.id();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            DefaultColors::OverUnder {
                over_color: RgbaColor::white(),
                under_color: RgbaColor::black(),
            },
        )
        .unwrap();

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            (
                "bbox",
                "1.95556640625,0.90087890625,1.9775390625,0.9228515625",
            ),
            ("width", "256"),
            ("height", "256"),
            ("crs", "EPSG:432"),
            (
                "styles",
                &format!("custom:{}", serde_json::to_string(&colorizer).unwrap()),
            ),
            ("format", "image/png"),
            ("time", "2014-04-01T12:00:00.0Z"),
            ("EXCEPTIONS", "application/json"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, ctx).await;

        ErrorResponse::assert(res, 200, "Operator", "Operator: DataTypeError: No CoordinateProjector available for: SpatialReference { authority: Epsg, code: 4326 } --> SpatialReference { authority: Epsg, code: 432 }").await;
    }
}
