use snafu::ResultExt;
use warp::reply::Reply;
use warp::{http::Response, Filter, Rejection};

use geoengine_datatypes::{
    operations::image::{Colorizer, ToPng},
    primitives::SpatialResolution,
    raster::Grid2D,
    raster::RasterTile2D,
    spatial_reference::SpatialReferenceOption,
};
use geoengine_datatypes::{
    primitives::BoundingBox2D,
    raster::{Blit, GeoTransform, Pixel},
};

use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::ogc::wms::request::{GetCapabilities, GetLegendGraphic, GetMap, WmsRequest};
use crate::users::session::Session;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use futures::StreamExt;
use geoengine_datatypes::operations::image::RgbaColor;
use geoengine_datatypes::primitives::{TimeInstance, TimeInterval};
use geoengine_operators::call_on_generic_raster_processor;
use geoengine_operators::engine::{
    QueryContext, QueryRectangle, RasterQueryProcessor, ResultDescriptor,
};
use num_traits::AsPrimitive;
use std::convert::TryInto;
use std::str::FromStr;

pub(crate) fn wms_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("wms")
        .and(warp::get())
        .and(
            warp::query::raw().and_then(|query_string: String| async move {
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
    let execution_context = ctx.execution_context(&Session::mock())?;

    let initialized = operator
        .initialize(&execution_context)
        .context(error::Operator)?;

    // handle request and workflow crs matching
    let workflow_spatial_ref = initialized.result_descriptor().spatial_reference();
    let request_spatial_ref: SpatialReferenceOption = request.crs.into();
    // TODO: use a default spatial reference if it is not set?
    snafu::ensure!(
        request_spatial_ref.is_spatial_ref(),
        error::InvalidSpatialReference
    );
    // TODO: inject projection Operator
    snafu::ensure!(
        workflow_spatial_ref == request_spatial_ref,
        error::SpatialReferenceMissmatch {
            found: request_spatial_ref,
            expected: workflow_spatial_ref,
        }
    );

    let processor = initialized.query_processor().context(error::Operator)?;

    let query_bbox = BoundingBox2D::new(
        (request.bbox.lower_left().y, request.bbox.lower_left().x).into(),
        (request.bbox.upper_right().y, request.bbox.upper_right().x).into(),
    )
    .context(error::DataType)?; // FIXME: handle WGS84 reverse order axes
    let x_query_resolution = query_bbox.size_x() / f64::from(request.width);
    let y_query_resolution = query_bbox.size_y() / f64::from(request.height);

    let query_rect = QueryRectangle {
        bbox: query_bbox,
        time_interval: request.time.unwrap_or_else(|| {
            let time = TimeInstance::from(chrono::offset::Utc::now());
            TimeInterval::new_unchecked(time, time)
        }),
        spatial_resolution: SpatialResolution::new_unchecked(
            x_query_resolution,
            y_query_resolution,
        ),
    };

    let query_ctx = ctx.query_context()?;

    let image_bytes = call_on_generic_raster_processor!(
        processor,
        p => raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
    )?;

    Ok(Box::new(
        Response::builder()
            .header("Content-Type", "image/png")
            .body(image_bytes)
            .context(error::Http)?,
    ))
}

async fn raster_stream_to_png_bytes<T, C: QueryContext>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: QueryRectangle,
    query_ctx: C,
    request: &GetMap,
) -> Result<Vec<u8>>
where
    T: Pixel,
{
    let tile_stream = processor.raster_query(query_rect, &query_ctx)?;

    let x_query_resolution = query_rect.bbox.size_x() / f64::from(request.width);
    let y_query_resolution = query_rect.bbox.size_y() / f64::from(request.height);

    // build png
    let dim = [request.height as usize, request.width as usize];
    let query_geo_transform = GeoTransform::new(
        query_rect.bbox.upper_left(),
        x_query_resolution,
        -y_query_resolution, // TODO: negative, s.t. geo transform fits...
    );

    let output_raster = Grid2D::new_filled(dim.into(), T::zero(), None);
    let output_tile = Ok(RasterTile2D::new_without_offset(
        request.time.unwrap_or_default(),
        query_geo_transform,
        output_raster,
    ));

    let output_tile = tile_stream
        .fold(output_tile, |raster2d, tile| {
            let result: Result<RasterTile2D<T>> = match (raster2d, tile) {
                (Ok(mut raster2d), Ok(tile)) => match raster2d.blit(tile) {
                    Ok(_) => Ok(raster2d),
                    Err(error) => Err(error.into()),
                },
                (Err(error), _) => Err(error),
                (_, Err(error)) => Err(error.into()),
            };

            match result {
                Ok(updated_raster2d) => futures::future::ok(updated_raster2d),
                Err(error) => futures::future::err(error),
            }
        })
        .await?;

    let colorizer = match request.styles.strip_prefix("custom:") {
        None => Colorizer::linear_gradient(
            vec![
                (AsPrimitive::<f64>::as_(T::min_value()), RgbaColor::black())
                    .try_into()
                    .unwrap(),
                (AsPrimitive::<f64>::as_(T::max_value()), RgbaColor::white())
                    .try_into()
                    .unwrap(),
            ],
            RgbaColor::transparent(),
            RgbaColor::pink(),
        )
        .unwrap(),
        Some(suffix) => serde_json::from_str(suffix)?,
    };

    Ok(output_tile.to_png(request.width, request.height, &colorizer)?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::handlers::{handle_rejection, ErrorResponse};
    use crate::ogc::wms::request::GetMapFormat;
    use crate::util::tests::{check_allowed_http_methods, register_ndvi_workflow_helper};
    use geoengine_datatypes::operations::image::RgbaColor;
    use geoengine_operators::engine::ExecutionContext;
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
    async fn png_from_stream() {
        let ctx = InMemoryContext::default();
        let exe_ctx = ctx.execution_context(&Session::mock()).unwrap();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification: exe_ctx.tiling_specification(),
            meta_data: Box::new(create_ndvi_meta_data()),
            phantom_data: Default::default(),
        };

        let query_bbox = BoundingBox2D::new((-10., 20.).into(), (50., 80.).into()).unwrap();

        let image_bytes = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            QueryRectangle {
                bbox: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::zero_point_one(),
            },
            ctx.query_context().unwrap(),
            &GetMap {
                version: "".to_string(),
                width: 600,
                height: 600,
                bbox: query_bbox,
                format: GetMapFormat::ImagePng,
                layers: "".to_string(),
                crs: None,
                styles: "".to_string(),
                time: None,
                transparent: None,
                bgcolor: None,
                sld: None,
                sld_body: None,
                elevation: None,
                exceptions: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            include_bytes!("../../../services/test-data/wms/raster.png") as &[u8],
            image_bytes.as_slice()
        );
    }

    #[tokio::test]
    async fn png_from_stream_non_full() {
        let ctx = InMemoryContext::default();
        let exe_ctx = ctx.execution_context(&Session::mock()).unwrap();

        let gdal_source = GdalSourceProcessor::<u8> {
            tiling_specification: exe_ctx.tiling_specification(),
            meta_data: Box::new(create_ndvi_meta_data()),
            phantom_data: Default::default(),
        };

        let query_bbox = BoundingBox2D::new((-180., -90.).into(), (180., 90.).into()).unwrap();

        let image_bytes = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            QueryRectangle {
                bbox: query_bbox,
                time_interval: TimeInterval::new(1_388_534_400_000, 1_388_534_400_000 + 1000)
                    .unwrap(),
                spatial_resolution: SpatialResolution::new_unchecked(1.0, 1.0),
            },
            ctx.query_context().unwrap(),
            &GetMap {
                version: "".to_string(),
                width: 360,
                height: 180,
                bbox: query_bbox,
                format: GetMapFormat::ImagePng,
                layers: "".to_string(),
                crs: None,
                styles: "".to_string(),
                time: None,
                transparent: None,
                bgcolor: None,
                sld: None,
                sld_body: None,
                elevation: None,
                exceptions: None,
            },
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
            include_bytes!("../../../services/test-data/wms/raster.png") as &[u8],
            res.body().to_vec().as_slice()
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
            include_bytes!("../../../services/test-data/wms/raster.png") as &[u8],
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
            include_bytes!("../../../services/test-data/wms/raster_colorizer.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }
}
