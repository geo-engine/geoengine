use log::debug;
use snafu::ResultExt;
use warp::reply::Reply;
use warp::{http::Response, Filter, Rejection};

use geoengine_datatypes::primitives::{AxisAlignedRectangle, SpatialPartition2D};
use geoengine_datatypes::{
    operations::image::{Colorizer, ToPng},
    primitives::SpatialResolution,
    raster::Grid2D,
    spatial_reference::SpatialReference,
};

use crate::contexts::MockableSession;
use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::ogc::wms::request::{GetCapabilities, GetLegendGraphic, GetMap, WmsRequest};
use crate::util::config;
use crate::util::config::get_config_element;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;

use geoengine_datatypes::primitives::{TimeInstance, TimeInterval};
use geoengine_operators::engine::{RasterOperator, RasterQueryRectangle, ResultDescriptor};
use geoengine_operators::processing::{Reprojection, ReprojectionParams};
use geoengine_operators::{
    call_on_generic_raster_processor, util::raster_stream_to_png::raster_stream_to_png_bytes,
};
use num_traits::AsPrimitive;

use std::str::FromStr;

pub(crate) fn wms_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("wms")
        .and(warp::get())
        .and(
            warp::query::raw().and_then(|query_string: String| async move {
                debug!("{}", query_string);

                // TODO: make case insensitive by using serde-aux instead
                let query_string = query_string.replace("REQUEST", "request");

                serde_urlencoded::from_str::<WmsRequest>(&query_string)
                    .context(error::UnableToParseQueryString)
                    .map_err(Rejection::from)
            }),
        )
        // .and(warp::query::<WMSRequest>())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(wms)
}

// TODO: move into handler once async closures are available?
async fn wms<C: Context>(
    request: WmsRequest,
    ctx: C,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: authentication
    // TODO: more useful error output than "invalid query string"
    match request {
        WmsRequest::GetCapabilities(request) => get_capabilities(&request),
        WmsRequest::GetMap(request) => get_map(&request, &ctx).await,
        WmsRequest::GetLegendGraphic(request) => get_legend_graphic(&request, &ctx),
        _ => Ok(Box::new(
            warp::http::StatusCode::NOT_IMPLEMENTED.into_response(),
        )),
    }
}

/// Gets details about the web map service provider and lists available operations.
///
/// # Example
///
/// ```text
/// GET /wms?request=GetCapabilities&service=WMS
/// ```
/// Response:
/// ```xml
/// <WMS_Capabilities xmlns="http://www.opengis.net/wms" xmlns:sld="http://www.opengis.net/sld" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.3.0" xsi:schemaLocation="http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/sld_capabilities.xsd">
///   <Service>
///   <Name>WMS</Name>
///   <Title>Geo Engine WMS</Title>
///   <OnlineResource xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="http://localhost"/>
/// </Service>
/// <Capability>
///   <Request>
///     <GetCapabilities>
///       <Format>text/xml</Format>
///       <DCPType>
///         <HTTP>
///           <Get>
///             <OnlineResource xlink:href="http://localhost/wms"/>
///           </Get>
///         </HTTP>
///       </DCPType>
///     </GetCapabilities>
///     <GetMap>
///       <Format>image/png</Format>
///       <DCPType>
///         <HTTP>
///           <Get>
///             <OnlineResource xlink:href="http://localhost/wms"/>
///           </Get>
///         </HTTP>
///       </DCPType>
///     </GetMap>
///   </Request>
///   <Exception>
///     <Format>XML</Format>
///     <Format>INIMAGE</Format>
///     <Format>BLANK</Format>
///   </Exception>
///   <Layer queryable="1">
///     <Name>Test</Name>
///     <Title>Test</Title>
///     <CRS>EPSG:4326</CRS>
///     <EX_GeographicBoundingBox>
///       <westBoundLongitude>-180</westBoundLongitude>
///       <eastBoundLongitude>180</eastBoundLongitude>
///       <southBoundLatitude>-90</southBoundLatitude>
///       <northBoundLatitude>90</northBoundLatitude>
///     </EX_GeographicBoundingBox>
///     <BoundingBox CRS="EPSG:4326" minx="-90.0" miny="-180.0" maxx="90.0" maxy="180.0"/>
///   </Layer>
/// </Capability>
/// </WMS_Capabilities>
/// ```
#[allow(clippy::unnecessary_wraps)] // TODO: remove line once implemented fully
fn get_capabilities(_request: &GetCapabilities) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: implement
    // TODO: inject correct url of the instance and return data for the default layer
    let wms_url = "http://localhost/wms".to_string();
    let mock = format!(
        r#"<WMS_Capabilities xmlns="http://www.opengis.net/wms" xmlns:sld="http://www.opengis.net/sld" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.3.0" xsi:schemaLocation="http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/sld_capabilities.xsd">
    <Service>
        <Name>WMS</Name>
        <Title>Geo Engine WMS</Title>
        <OnlineResource xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="http://localhost"/>
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
            <Format>INIMAGE</Format>
            <Format>BLANK</Format>
        </Exception>
        <Layer queryable="1">
            <Name>Test</Name>
            <Title>Test</Title>
            <CRS>EPSG:4326</CRS>
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
        wms_url = wms_url
    );

    Ok(Box::new(warp::reply::html(mock)))
}

/// Renders a map as raster image.
///
/// # Example
///
/// ```text
/// GET /wms?request=GetMap&service=WMS&version=2.0.0&layers=mock_raster&bbox=1,2,3,4&width=100&height=100&crs=EPSG%3A4326&styles=ssss&format=image%2Fpng
/// ```
/// Response:
/// PNG image
async fn get_map<C: Context>(
    request: &GetMap,
    ctx: &C,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: validate request?
    if request.layers == "mock_raster" {
        return get_map_mock(request);
    }

    let workflow = ctx
        .workflow_registry_ref()
        .await
        .load(&WorkflowId::from_str(&request.layers)?)
        .await?;

    let operator = workflow.operator.get_raster().context(error::Operator)?;

    // TODO: use correct session when WMS uses authenticated access
    let execution_context = ctx.execution_context(C::Session::mock())?;

    let initialized = operator
        .clone()
        .initialize(&execution_context)
        .await
        .context(error::Operator)?;

    // handle request and workflow crs matching
    let workflow_spatial_ref: Option<SpatialReference> =
        initialized.result_descriptor().spatial_reference().into();
    let workflow_spatial_ref = workflow_spatial_ref.ok_or(error::Error::InvalidSpatialReference)?;

    // TODO: use a default spatial reference if it is not set?
    let request_spatial_ref: SpatialReference =
        request.crs.ok_or(error::Error::InvalidSpatialReference)?;

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

    // TODO: use proj for determining axis order
    let query_bbox: SpatialPartition2D = request.bbox.bounds(request_spatial_ref)?;
    let x_query_resolution = query_bbox.size_x() / f64::from(request.width);
    let y_query_resolution = query_bbox.size_y() / f64::from(request.height);

    let query_rect = RasterQueryRectangle {
        spatial_bounds: query_bbox,
        time_interval: request.time.unwrap_or_else(default_time_from_config),
        spatial_resolution: SpatialResolution::new_unchecked(
            x_query_resolution,
            y_query_resolution,
        ),
    };

    let query_ctx = ctx.query_context()?;

    let colorizer = colorizer_from_style(&request.styles)?;

    let image_bytes = call_on_generic_raster_processor!(
        processor,
        p =>
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request.width, request.height, request.time, colorizer, no_data_value.map(AsPrimitive::as_)).await
    ).map_err(error::Error::from)?;

    Ok(Box::new(
        Response::builder()
            .header("Content-Type", "image/png")
            .body(image_bytes)
            .context(error::Http)?,
    ))
}

fn colorizer_from_style(styles: &str) -> Result<Option<Colorizer>> {
    match styles.strip_prefix("custom:") {
        None => Ok(None),
        Some(suffix) => serde_json::from_str(suffix).map_err(error::Error::from),
    }
}

#[allow(clippy::unnecessary_wraps)] // TODO: remove line once implemented fully
fn get_legend_graphic<C: Context>(
    _request: &GetLegendGraphic,
    _ctx: &C,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: implement
    Ok(Box::new(
        warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    ))
}

fn get_map_mock(request: &GetMap) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let raster = Grid2D::new(
        [2, 2].into(),
        vec![
            0xFF00_00FF_u32,
            0x0000_00FF_u32,
            0x00FF_00FF_u32,
            0x0000_00FF_u32,
        ],
        None,
    )
    .context(error::DataType)?;

    let colorizer = Colorizer::rgba();
    let image_bytes = raster
        .to_png(request.width, request.height, &colorizer)
        .context(error::DataType)?;

    Ok(Box::new(
        Response::builder()
            .header("Content-Type", "image/png")
            .body(image_bytes)
            .context(error::Http)?,
    ))
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
    use super::*;
    use crate::contexts::{InMemoryContext, SimpleSession};
    use crate::handlers::{handle_rejection, ErrorResponse};
    use crate::util::tests::{check_allowed_http_methods, register_ndvi_workflow_helper};
    use geoengine_datatypes::operations::image::RgbaColor;
    use geoengine_datatypes::primitives::SpatialPartition2D;
    use geoengine_operators::engine::{
        ExecutionContext, RasterQueryProcessor, RasterQueryRectangle,
    };
    use geoengine_operators::source::GdalSourceProcessor;
    use geoengine_operators::util::gdal::create_ndvi_meta_data;
    use std::convert::TryInto;
    use warp::hyper::body::Bytes;
    use xml::ParserConfig;

    async fn test_test_helper(method: &str, path: Option<&str>) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        warp::test::request()
            .method(method)
            .path(path.unwrap_or("/wms?request=GetMap&service=WMS&version=1.3.0&layers=mock_raster&bbox=1,2,3,4&width=100&height=100&crs=EPSG:4326&styles=ssss&format=image/png"))
            .reply(&wms_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn test() {
        let res = test_test_helper("GET", None).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../datatypes/test-data/colorizer/rgba.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }

    #[tokio::test]
    async fn test_invalid_method() {
        check_allowed_http_methods(|method| test_test_helper(method, None), &["GET"]).await;
    }

    #[tokio::test]
    async fn test_missing_fields() {
        let res = test_test_helper("GET", Some("/wms?service=WMS&version=1.3.0&layers=mock_raster&bbox=1,2,3,4&width=100&height=100&crs=foo&styles=ssss&format=image/png")).await;

        ErrorResponse::assert(
            &res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: missing field `request`",
        );
    }

    #[tokio::test]
    async fn test_invalid_fields() {
        let res = test_test_helper("GET", Some("/wms?request=GetMap&service=WMS&version=1.3.0&layers=mock_raster&bbox=1,2,3,4&width=XYZ&height=100&crs=EPSG:4326&styles=ssss&format=image/png")).await;

        ErrorResponse::assert(
            &res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: could not parse string",
        );
    }

    async fn get_capabilities_test_helper(method: &str) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        warp::test::request()
            .method(method)
            .path("/wms?request=GetCapabilities&service=WMS")
            .reply(&wms_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn get_capabilities() {
        let res = get_capabilities_test_helper("GET").await;

        assert_eq!(res.status(), 200);

        // TODO: validate against schema
        let reader = ParserConfig::default().create_reader(res.body().as_ref());

        for event in reader {
            assert!(event.is_ok());
        }
    }

    #[tokio::test]
    async fn get_capabilities_invalid_method() {
        check_allowed_http_methods(get_capabilities_test_helper, &["GET"]).await;
    }

    #[tokio::test]
    async fn png_from_stream_non_full() {
        let ctx = InMemoryContext::default();
        let exe_ctx = ctx.execution_context(SimpleSession::default()).unwrap();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification: exe_ctx.tiling_specification(),
            meta_data: Box::new(create_ndvi_meta_data()),
            phantom_data: Default::default(),
        };

        let query_partition =
            SpatialPartition2D::new((-180., 90.).into(), (180., -90.).into()).unwrap();

        let image_bytes = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            RasterQueryRectangle {
                spatial_bounds: query_partition,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(1.0, 1.0),
            },
            ctx.query_context().unwrap(),
            360,
            180,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            include_bytes!("../../../services/test-data/wms/raster_small.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    async fn get_map_test_helper(method: &str, path: Option<&str>) -> Response<Bytes> {
        let ctx = InMemoryContext::default();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        warp::test::request()
            .method(method)
            .path(path.unwrap_or(&format!("/wms?request=GetMap&service=WMS&version=1.3.0&layers={}&bbox=20,-10,80,50&width=600&height=600&crs=EPSG:4326&styles=ssss&format=image/png&time=2014-01-01T00:00:00.0Z", id.to_string())))
            .reply(&wms_handler(ctx).recover(handle_rejection))
            .await
    }

    #[tokio::test]
    async fn get_map() {
        let res = get_map_test_helper("GET", None).await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../services/test-data/wms/get_map.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }

    #[tokio::test]
    async fn get_map_ndvi() {
        let ctx = InMemoryContext::default();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let response = warp::test::request()
            .method("GET")
            .path(&format!("/wms?service=WMS&version=1.3.0&request=GetMap&layers={}&styles=&width=335&height=168&crs=EPSG:4326&bbox=-90.0,-180.0,90.0,180.0&format=image/png&transparent=FALSE&bgcolor=0xFFFFFF&exceptions=XML&time=2014-04-01T12%3A00%3A00.000%2B00%3A00", id.to_string()))
            .reply(&wms_handler(ctx).recover(handle_rejection))
            .await;

        assert_eq!(response.status(), 200, "{:?}", response.body());

        assert_eq!(
            include_bytes!("../../../services/test-data/wms/get_map_ndvi.png") as &[u8],
            response.body().to_vec().as_slice()
        );
    }

    #[tokio::test]
    async fn get_map_uppercase() {
        let ctx = InMemoryContext::default();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/wms?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS={}&CRS=EPSG:4326&STYLES=&WIDTH=600&HEIGHT=600&BBOX=20,-10,80,50&time=2014-01-01T00:00:00.0Z", id.to_string()))
            .reply(&wms_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../services/test-data/wms/get_map.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }

    #[tokio::test]
    async fn get_map_invalid_method() {
        check_allowed_http_methods(|method| get_map_test_helper(method, None), &["GET"]).await;
    }

    #[tokio::test]
    async fn get_map_missing_fields() {
        let res = get_map_test_helper("GET", Some("/wms?request=GetMap&service=WMS&version=1.3.0&bbox=20,-10,80,50&width=600&height=600&crs=EPSG:4326&styles=ssss&format=image/png&time=2014-01-01T00:00:00.0Z")).await;

        ErrorResponse::assert(
            &res,
            400,
            "UnableToParseQueryString",
            "Unable to parse query string: missing field `layers`",
        );
    }

    #[tokio::test]
    async fn get_map_colorizer() {
        let ctx = InMemoryContext::default();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let colorizer = Colorizer::linear_gradient(
            vec![
                (0.0, RgbaColor::white()).try_into().unwrap(),
                (1.0, RgbaColor::black()).try_into().unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
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

        let res = warp::test::request()
            .method("GET")
            .path(&format!(
                "/wms?{}",
                serde_urlencoded::to_string(params).unwrap()
            ))
            .reply(&wms_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../services/test-data/wms/get_map_colorizer.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }
}
