use crate::api::model::datatypes::{
    RasterColorizer, SpatialReference, SpatialReferenceOption, TimeInterval,
};
use crate::api::model::responses::ErrorResponse;
use crate::api::ogc::util::{OgcProtocol, OgcRequestGuard, ogc_endpoint_url};
use crate::api::ogc::wms::request::{
    GetCapabilities, GetLegendGraphic, GetMap, GetMapExceptionFormat,
};
use crate::config;
use crate::config::get_config_element;
use crate::contexts::{ApplicationContext, SessionContext};
use crate::error::Result;
use crate::error::{self, Error};
use crate::util::server::{CacheControlHeader, connection_closed, not_implemented_handler};
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use actix_web::{FromRequest, HttpRequest, HttpResponse, web};
use geoengine_datatypes::primitives::SpatialResolution;
use geoengine_datatypes::primitives::{
    AxisAlignedRectangle, RasterQueryRectangle, SpatialPartition2D,
};
use geoengine_datatypes::primitives::{BandSelection, CacheHint};
use geoengine_operators::engine::{
    RasterOperator, ResultDescriptor, SingleRasterOrVectorSource, WorkflowOperatorPath,
};
use geoengine_operators::processing::{Reprojection, ReprojectionParams};
use geoengine_operators::util::input::RasterOrVectorOperator;
use geoengine_operators::{
    call_on_generic_raster_processor, util::raster_stream_to_png::raster_stream_to_png_bytes,
};
use reqwest::Url;
use snafu::ensure;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

pub(crate) fn init_wms_routes<C>(cfg: &mut web::ServiceConfig)
where
    C: ApplicationContext,
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
            //     xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="http://127.0.0.1:3030/api/wms/b709b27b-dea5-5a27-a074-ae3366c49498"/>
            //   </Service>
            //   <Capability>
            //     <Request>
            //       <GetCapabilities>
            //         <Format>text/xml</Format>
            //         <DCPType>
            //           <HTTP>
            //             <Get>
            //               <OnlineResource xlink:href="http://127.0.0.1:3030/api/wms/b709b27b-dea5-5a27-a074-ae3366c49498"/>
            //             </Get>
            //           </HTTP>
            //         </DCPType>
            //       </GetCapabilities>
            //       <GetMap>
            //         <Format>image/png</Format>
            //         <DCPType>
            //           <HTTP>
            //             <Get>
            //               <OnlineResource xlink:href="http://127.0.0.1:3030/api/wms/b709b27b-dea5-5a27-a074-ae3366c49498"/>
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
    // TODO: incorporate `GetCapabilities` request
    // _request: web::Query<GetCapabilities>,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse>
where
    C: ApplicationContext,
{
    let workflow_id = workflow.into_inner();
    let wms_url = wms_url(workflow_id)?;

    let ctx = app_ctx.session_context(session);

    let workflow = ctx.db().load_workflow(&workflow_id).await?;

    let exe_ctx = ctx.execution_context()?;
    let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

    let operator = workflow
        .operator
        .get_raster()?
        .initialize(workflow_operator_path_root, &exe_ctx)
        .await?;

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
    let web_config = crate::config::get_config_element::<crate::config::Web>()?;
    let base = web_config.api_url()?;

    ogc_endpoint_url(&base, OgcProtocol::Wms, workflow)
}

/// Get WMS Map
#[utoipa::path(
    tag = "OGC WMS",
    get,
    path = "/wms/{workflow}?request=GetMap",
    responses(
        (status = 200, response = crate::api::model::responses::PngResponse),
    ),
    params(
        ("workflow" = WorkflowId, description = "Workflow id"),
        GetMap
    ),
    security(
        ("session_token" = [])
    )
)]
#[allow(clippy::too_many_lines)]
async fn wms_map_handler<C: ApplicationContext>(
    req: HttpRequest,
    workflow: web::Path<WorkflowId>,
    request: web::Query<GetMap>,
    app_ctx: web::Data<C>,
    session: C::Session,
) -> Result<HttpResponse> {
    async fn compute_result<C: ApplicationContext>(
        req: HttpRequest,
        workflow: web::Path<WorkflowId>,
        request: &web::Query<GetMap>,
        app_ctx: web::Data<C>,
        session: C::Session,
    ) -> Result<(Vec<u8>, CacheHint)> {
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

        let ctx = app_ctx.session_context(session);

        let workflow_id = WorkflowId::from_str(&request.layers)?;
        let workflow = ctx.db().load_workflow(&workflow_id).await?;

        let operator = workflow.operator.get_raster()?;

        let execution_context = ctx.execution_context()?;

        let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

        let initialized = operator
            .clone()
            .initialize(workflow_operator_path_root, &execution_context)
            .await?;

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
                "WMS query srs: {request_spatial_ref}, workflow srs: {workflow_spatial_ref} --> injecting reprojection"
            );

            let reprojection_params = ReprojectionParams {
                target_spatial_reference: request_spatial_ref.into(),
            };

            // create the reprojection operator in order to get the canonic operator name
            let reprojected_workflow = Reprojection {
                params: reprojection_params,
                sources: SingleRasterOrVectorSource {
                    source: RasterOrVectorOperator::Raster(operator),
                },
            }
            .boxed();

            let workflow_operator_path_root = WorkflowOperatorPath::initialize_root();

            // TODO: avoid re-initialization and re-use unprojected workflow. However, this requires updating all operator paths

            // In order to check whether we need to inject a reprojection, we first need to initialize the
            // original workflow. Then we can check the result projection. Previously, we then just wrapped
            // the initialized workflow with an initialized reprojection. IMHO this is wrong because
            // initialization propagates the workflow path down the children and appends a new segment for
            // each level. So we can't re-use an already initialized workflow, because all the workflow path/
            // operator names will be wrong. That's why I now build a new workflow with a reprojection and
            // perform a full initialization. I only added the TODO because we did some optimization here
            // which broke at some point when the workflow operator paths were introduced but no one noticed.

            let irp = reprojected_workflow
                .initialize(workflow_operator_path_root, &execution_context)
                .await?;

            Box::new(irp)
        };

        let processor = initialized.query_processor()?;

        let query_bbox: SpatialPartition2D = request.bbox.bounds(request_spatial_ref)?;
        let x_query_resolution = query_bbox.size_x() / f64::from(request.width);
        let y_query_resolution = query_bbox.size_y() / f64::from(request.height);

        let raster_colorizer = raster_colorizer_from_style(&request.styles)?;

        let attributes = raster_colorizer.as_ref().map_or_else(
            || BandSelection::new_single(0),
            |colorizer: &RasterColorizer| {
                RasterColorizer::band_selection(colorizer)
                    .try_into()
                    .expect("conversion of usize to u32 succeeds for small band numbers")
            },
        );

        let query_rect = RasterQueryRectangle {
            spatial_bounds: query_bbox,
            time_interval: request.time.unwrap_or_else(default_time_from_config).into(),
            spatial_resolution: SpatialResolution::new_unchecked(
                x_query_resolution,
                y_query_resolution,
            ),
            attributes,
        };

        let query_ctx = ctx.query_context(workflow_id.0, Uuid::new_v4())?;

        call_on_generic_raster_processor!(
            processor,
            p =>
                raster_stream_to_png_bytes(p, query_rect, query_ctx, request.width, request.height, request.time.map(Into::into), raster_colorizer.map(Into::into), conn_closed).await
        ).map_err(error::Error::from)
    }

    match compute_result(req, workflow, &request, app_ctx, session).await {
        Ok((image_bytes, cache_hint)) => Ok(HttpResponse::Ok()
            .content_type(mime::IMAGE_PNG)
            .append_header(cache_hint.cache_control_header())
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
        GetMapExceptionFormat::Json => {
            HttpResponse::Ok().json(ErrorResponse::from_service_error(error))
        }
    }
}

fn raster_colorizer_from_style(styles: &str) -> Result<Option<RasterColorizer>> {
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
#[allow(
    clippy::unused_async, // the function signature of request handlers requires it
    clippy::no_effect_underscore_binding // need `_session` to quire authentication
)]
async fn wms_legend_graphic_handler<C: ApplicationContext>(
    // TODO: incorporate workflow and `GetLegendGraphic` query
    // _workflow: web::Path<WorkflowId>,
    // _request: web::Query<GetLegendGraphic>,
    // _app_ctx: web::Data<C>,
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
                        },
                        |time| time.time_interval(),
                    )
            },
            |time| time.time_interval(),
        )
        .into()
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::api::model::datatypes::MultiBandRasterColorizer;
    use crate::api::model::datatypes::SingleBandRasterColorizer;
    use crate::api::model::responses::ErrorResponse;
    use crate::contexts::PostgresContext;
    use crate::contexts::Session;
    use crate::datasets::DatasetName;
    use crate::datasets::listing::DatasetProvider;
    use crate::datasets::storage::DatasetStore;
    use crate::ge_context;
    use crate::users::UserAuth;
    use crate::util::tests::{
        MockQueryContext, check_allowed_http_methods, read_body_string,
        register_ndvi_workflow_helper_with_cache_ttl, register_ne2_multiband_workflow,
        send_test_request,
    };
    use crate::util::tests::{admin_login, register_ndvi_workflow_helper};
    use actix_http::header::{self, CONTENT_TYPE};
    use actix_web::dev::ServiceResponse;
    use actix_web::http::Method;
    use actix_web_httpauth::headers::authorization::Bearer;
    use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
    use geoengine_datatypes::primitives::CacheTtlSeconds;
    use geoengine_datatypes::raster::{GridShape2D, RasterDataType, TilingSpecification};
    use geoengine_datatypes::test_data;
    use geoengine_datatypes::util::assert_image_equals;
    use geoengine_operators::engine::{
        ExecutionContext, RasterQueryProcessor, RasterResultDescriptor,
    };
    use geoengine_operators::source::GdalSourceProcessor;
    use geoengine_operators::util::gdal::create_ndvi_meta_data;
    use std::convert::TryInto;
    use std::marker::PhantomData;
    use tokio_postgres::NoTls;
    use xml::ParserConfig;

    async fn test_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
        path: Option<&str>,
    ) -> ServiceResponse {
        let path = path.map(ToString::to_string);

        let session = app_ctx.create_anonymous_session().await.unwrap();
        let session_id = session.id();

        let req = actix_web::test::TestRequest::default()
            .method(method)
            .uri(&path.unwrap_or("/wms/df756642-c5a3-4d72-8ad7-629d312ae993?request=GetMap&service=WMS&version=1.3.0&layers=df756642-c5a3-4d72-8ad7-629d312ae993&bbox=1,2,3,4&width=100&height=100&crs=EPSG:4326&styles=ssss&format=image/png".to_string()))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        send_test_request(req, app_ctx).await
    }

    #[ge_context::test]
    async fn test_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| test_test_helper(app_ctx.clone(), method, None),
            &[Method::GET],
        )
        .await;
    }

    #[ge_context::test]
    async fn test_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let res = test_test_helper(
            app_ctx,
            Method::GET,
            Some("/wms/df756642-c5a3-4d72-8ad7-629d312ae993?service=WMS&request=GetMap&version=1.3.0&bbox=1,2,3,4&width=100&height=100&crs=EPSG:4326&styles=ssss&format=image/png"),
        ).await;

        ErrorResponse::assert(
            res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: missing field `layers`",
        )
        .await;
    }

    #[ge_context::test]
    async fn test_invalid_fields(app_ctx: PostgresContext<NoTls>) {
        let res = test_test_helper(
            app_ctx,
            Method::GET,
            Some("/wms/df756642-c5a3-4d72-8ad7-629d312ae993?request=GetMap&service=WMS&version=1.3.0&layers=df756642-c5a3-4d72-8ad7-629d312ae993&bbox=1,2,3,4&width=XYZ&height=100&crs=EPSG:4326&styles=ssss&format=image/png"),
        ).await;

        ErrorResponse::assert(
            res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: could not parse string",
        )
        .await;
    }

    async fn get_capabilities_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
    ) -> ServiceResponse {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = session.id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let req = actix_web::test::TestRequest::with_uri(&format!(
            "/wms/{id}?request=GetCapabilities&service=WMS"
        ))
        .method(method)
        .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx).await;

        // remove NDVI to allow calling this method again
        ctx.db()
            .delete_dataset(
                ctx.db()
                    .resolve_dataset_name_to_id(&DatasetName::new(None, "NDVI"))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .await
            .unwrap();

        response
    }

    #[ge_context::test]
    async fn test_get_capabilities(app_ctx: PostgresContext<NoTls>) {
        let res = get_capabilities_test_helper(app_ctx, Method::GET).await;

        assert_eq!(res.status(), 200);

        // TODO: validate against schema
        let body = actix_web::test::read_body(res).await;
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

    // The result should be similar to the GDAL output of this command: gdalwarp -tr 1 1 -r near -srcnodata 0 -dstnodata 0  MOD13A2_M_NDVI_2014-01-01.TIFF MOD13A2_M_NDVI_2014-01-01_360_180_near_0.TIFF
    #[ge_context::test]
    async fn png_from_stream_non_full(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());
        let exe_ctx = ctx.execution_context().unwrap();

        let gdal_source = GdalSourceProcessor::<u8> {
            result_descriptor: RasterResultDescriptor::with_datatype_and_num_bands(
                RasterDataType::U8,
                1,
            ),
            tiling_specification: exe_ctx.tiling_specification(),
            meta_data: Box::new(create_ndvi_meta_data()),
            _phantom_data: PhantomData,
        };

        let query_partition =
            SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let (image_bytes, _) = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_partition,
                time_interval: geoengine_datatypes::primitives::TimeInterval::new(
                    1_388_534_400_000,
                    1_388_534_400_000 + 1000,
                )
                .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(1.0, 1.0),
                attributes: BandSelection::first(),
            },
            ctx.mock_query_context().unwrap(),
            360,
            180,
            None,
            None,
            Box::pin(futures::future::pending()),
        )
        .await
        .unwrap();

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, test_data!("wms/raster_small.png"));

        assert_image_equals(test_data!("wms/raster_small.png"), &image_bytes);
    }

    /// override the pixel size since this test was designed for 600 x 600 pixel tiles
    fn get_map_test_helper_tiling_spec() -> TilingSpecification {
        TilingSpecification {
            origin_coordinate: (0., 0.).into(),
            tile_size_in_pixels: GridShape2D::new([600, 600]),
        }
    }

    async fn get_map_test_helper(
        app_ctx: PostgresContext<NoTls>,
        method: Method,
        path: Option<&str>,
    ) -> ServiceResponse {
        let session = admin_login(&app_ctx).await;
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let req = actix_web::test::TestRequest::with_uri(path.unwrap_or(&format!("/wms/{id}?request=GetMap&service=WMS&version=1.3.0&layers={id}&bbox=20,-10,80,50&width=600&height=600&crs=EPSG:4326&styles=ssss&format=image/png&time=2014-01-01T00:00:00.0Z", id = id.to_string())))
                .method(method)
                .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx).await;

        // remove NDVI to allow calling this method again
        ctx.db()
            .delete_dataset(
                ctx.db()
                    .resolve_dataset_name_to_id(&DatasetName::new(None, "NDVI"))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .await
            .unwrap();

        response
    }

    #[ge_context::test(tiling_spec = "get_map_test_helper_tiling_spec")]
    async fn get_map(app_ctx: PostgresContext<NoTls>) {
        let res = get_map_test_helper(app_ctx, Method::GET, None).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "get_map.png");

        assert_image_equals(test_data!("wms/get_map.png"), &image_bytes);
    }

    #[ge_context::test]
    async fn get_map_ndvi(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let req = actix_web::test::TestRequest::get().uri(&format!("/wms/{id}?service=WMS&version=1.3.0&request=GetMap&layers={id}&styles=&width=335&height=168&crs=EPSG:4326&bbox=-90.0,-180.0,90.0,180.0&format=image/png&transparent=FALSE&bgcolor=0xFFFFFF&exceptions=application/json&time=2014-04-01T12%3A00%3A00.000%2B00%3A00", id = id.to_string())).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx).await;

        assert_eq!(
            response.status(),
            200,
            "{:?}",
            actix_web::test::read_body(response).await
        );

        let image_bytes = actix_web::test::read_body(response).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "get_map_ndvi.png");

        assert_image_equals(test_data!("wms/get_map_ndvi.png"), &image_bytes);
    }

    ///Actix uses serde_urlencoded inside web::Query which does not support this
    #[ge_context::test(tiling_spec = "get_map_test_helper_tiling_spec")]
    async fn get_map_uppercase(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let req = actix_web::test::TestRequest::get().uri(&format!("/wms/{id}?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS={id}&CRS=EPSG:4326&STYLES=&WIDTH=600&HEIGHT=600&BBOX=20,-10,80,50&time=2014-01-01T00:00:00.0Z", id = id.to_string())).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "get_map.png");

        assert_image_equals(test_data!("wms/get_map.png"), &image_bytes);
    }

    #[ge_context::test(tiling_spec = "get_map_test_helper_tiling_spec")]
    async fn get_map_invalid_method(app_ctx: PostgresContext<NoTls>) {
        check_allowed_http_methods(
            |method| get_map_test_helper(app_ctx.clone(), method, None),
            &[Method::GET],
        )
        .await;
    }

    #[ge_context::test(tiling_spec = "get_map_test_helper_tiling_spec")]
    async fn get_map_missing_fields(app_ctx: PostgresContext<NoTls>) {
        let res = get_map_test_helper(
            app_ctx,
            Method::GET,
            Some("/wms/df756642-c5a3-4d72-8ad7-629d312ae993?request=GetMap&service=WMS&version=1.3.0&bbox=20,-10,80,50&width=600&height=600&crs=EPSG:4326&styles=ssss&format=image/png&time=2014-01-01T00:00:00.0Z"),
        ).await;

        ErrorResponse::assert(
            res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: missing field `layers`",
        )
        .await;
    }

    #[ge_context::test(tiling_spec = "get_map_test_helper_tiling_spec")]
    async fn get_map_colorizer(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (1.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let raster_colorizer = RasterColorizer::SingleBand(SingleBandRasterColorizer {
            r#type: Default::default(),
            band: 0,
            band_colorizer: colorizer.into(),
        });

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
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
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
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "get_map_colorizer.png");

        assert_image_equals(test_data!("wms/get_map_colorizer.png"), &image_bytes);
    }

    #[ge_context::test(tiling_spec = "get_map_test_helper_tiling_spec")]
    async fn it_supports_multiband_colorizer(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();

        let (_, id) = register_ne2_multiband_workflow(&app_ctx).await;

        let raster_colorizer = RasterColorizer::MultiBand(MultiBandRasterColorizer {
            r#type: Default::default(),
            red_band: 2,
            red_min: 0.,
            red_max: 255.,
            red_scale: 1.0,
            green_band: 1,
            green_min: 0.,
            green_max: 255.,
            green_scale: 1.0,
            blue_band: 0,
            blue_min: 0.,
            blue_max: 255.,
            blue_scale: 1.0,
            no_data_color: RgbaColor::transparent().into(),
        });

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            ("bbox", "-90,-180,90,180"),
            ("width", "600"),
            ("height", "300"),
            ("crs", "EPSG:4326"),
            (
                "styles",
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
            ),
            ("format", "image/png"),
            ("time", "2022-01-01T00:00:00.0Z"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(&image_bytes, "ne2_rgb_colorizer.png");

        assert_image_equals(test_data!("wms/ne2_rgb_colorizer.png"), &image_bytes);
    }

    #[ge_context::test(tiling_spec = "get_map_test_helper_tiling_spec")]
    async fn it_supports_multiband_colorizer_with_less_then_3_bands(
        app_ctx: PostgresContext<NoTls>,
    ) {
        let session = app_ctx.create_anonymous_session().await.unwrap();
        let ctx = app_ctx.session_context(session.clone());

        let session_id = ctx.session().id();

        let (_, id) = register_ne2_multiband_workflow(&app_ctx).await;

        let raster_colorizer = RasterColorizer::MultiBand(MultiBandRasterColorizer {
            r#type: Default::default(),
            red_band: 1,
            red_min: 0.,
            red_max: 255.,
            red_scale: 1.0,
            green_band: 1,
            green_min: 0.,
            green_max: 255.,
            green_scale: 1.0,
            blue_band: 1,
            blue_min: 0.,
            blue_max: 255.,
            blue_scale: 1.0,
            no_data_color: RgbaColor::transparent().into(),
        });

        let params = &[
            ("request", "GetMap"),
            ("service", "WMS"),
            ("version", "1.3.0"),
            ("layers", &id.to_string()),
            ("bbox", "-90,-180,90,180"),
            ("width", "600"),
            ("height", "300"),
            ("crs", "EPSG:4326"),
            (
                "styles",
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
            ),
            ("format", "image/png"),
            ("time", "2022-01-01T00:00:00.0Z"),
        ];

        let req = actix_web::test::TestRequest::get()
            .uri(&format!(
                "/wms/{}?{}",
                id,
                serde_urlencoded::to_string(params).unwrap()
            ))
            .append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);

        let image_bytes = actix_web::test::read_body(res).await;

        // geoengine_datatypes::util::test::save_test_bytes(
        //     &image_bytes,
        //     geoengine_datatypes::test_data!("wms/ne2_rgb_colorizer_gray.png")
        //         .to_str()
        //         .unwrap(),
        // );

        assert_image_equals(test_data!("wms/ne2_rgb_colorizer_gray.png"), &image_bytes);
    }

    #[ge_context::test]
    async fn it_zoomes_very_far(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let raster_colorizer = RasterColorizer::SingleBand(SingleBandRasterColorizer {
            r#type: Default::default(),
            band: 0,
            band_colorizer: colorizer.into(),
        });

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
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
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
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);
        assert_eq!(res.headers().get(CONTENT_TYPE).unwrap(), "image/png");
    }

    #[ge_context::test]
    async fn default_error(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let raster_colorizer = RasterColorizer::SingleBand(SingleBandRasterColorizer {
            r#type: Default::default(),
            band: 0,
            band_colorizer: colorizer.into(),
        });

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
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
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
        let res = send_test_request(req, app_ctx).await;

        assert_eq!(res.status(), 200);
        let body = read_body_string(res).await;

        assert_eq!(
            body,
            r#"
<?xml version="1.0" encoding="UTF-8"?>
    <ServiceExceptionReport version="1.3.0" xmlns="http://www.opengis.net/ogc" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.opengis.net/ogc https://cdc.dwd.de/geoserver/schemas/wms/1.3.0/exceptions_1_3_0.xsd">
    <ServiceException>
        No CoordinateProjector available for: SpatialReference { authority: Epsg, code: 4326 } --> SpatialReference { authority: Epsg, code: 432 }
    </ServiceException>
</ServiceExceptionReport>"#
        );
    }

    #[ge_context::test]
    async fn json_error(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (255.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::white(),
            RgbaColor::black(),
        )
        .unwrap();

        let raster_colorizer = RasterColorizer::SingleBand(SingleBandRasterColorizer {
            r#type: Default::default(),
            band: 0,
            band_colorizer: colorizer.into(),
        });

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
                &format!(
                    "custom:{}",
                    serde_json::to_string(&raster_colorizer).unwrap()
                ),
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
        let res = send_test_request(req, app_ctx).await;

        ErrorResponse::assert(res, 200, "NoCoordinateProjector", "No CoordinateProjector available for: SpatialReference { authority: Epsg, code: 4326 } --> SpatialReference { authority: Epsg, code: 432 }").await;
    }

    #[ge_context::test]
    async fn it_sets_cache_control_header_no_cache(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, id) = register_ndvi_workflow_helper(&app_ctx).await;

        let req = actix_web::test::TestRequest::get().uri(&format!("/wms/{id}?service=WMS&version=1.3.0&request=GetMap&layers={id}&styles=&width=335&height=168&crs=EPSG:4326&bbox=-90.0,-180.0,90.0,180.0&format=image/png&transparent=FALSE&bgcolor=0xFFFFFF&exceptions=application/json&time=2014-04-01T12%3A00%3A00.000%2B00%3A00", id = id.to_string())).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx).await;

        assert_eq!(
            response.status(),
            200,
            "{:?}",
            actix_web::test::read_body(response).await
        );

        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-cache"
        );
    }

    #[ge_context::test]
    async fn it_sets_cache_control_header_with_cache(app_ctx: PostgresContext<NoTls>) {
        let session = app_ctx.create_anonymous_session().await.unwrap();

        let session_id = session.id();

        let (_, id) =
            register_ndvi_workflow_helper_with_cache_ttl(&app_ctx, CacheTtlSeconds::new(60)).await;

        let req = actix_web::test::TestRequest::get().uri(&format!("/wms/{id}?service=WMS&version=1.3.0&request=GetMap&layers={id}&styles=&width=335&height=168&crs=EPSG:4326&bbox=-90.0,-180.0,90.0,180.0&format=image/png&transparent=FALSE&bgcolor=0xFFFFFF&exceptions=application/json&time=2014-04-01T12%3A00%3A00.000%2B00%3A00", id = id.to_string())).append_header((header::AUTHORIZATION, Bearer::new(session_id.to_string())));
        let response = send_test_request(req, app_ctx).await;

        assert_eq!(
            response.status(),
            200,
            "{:?}",
            actix_web::test::read_body(response).await
        );

        let cache_header = response.headers().get(header::CACHE_CONTROL).unwrap();

        // defensive check here. Between creation of the tiles and the response of the handler might be some time, so the ttl of the output may be a bit lower than the defined ttl for the dataset
        assert!(
            cache_header == "private, max-age=60"
                || cache_header == "private, max-age=59"
                || cache_header == "private, max-age=58"
        );
    }
}
