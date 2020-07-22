use std::sync::Arc;

use snafu::ResultExt;
use tokio::sync::RwLock;
use warp::reply::Reply;
use warp::{http::Response, Filter};

use geoengine_datatypes::operations::image::{Colorizer, ToPng};
use geoengine_datatypes::raster::Raster2D;

use crate::error;
use crate::ogc::wms::request::{GetCapabilities, GetLegendGraphic, GetMap, WMSRequest};
use crate::workflows::registry::WorkflowRegistry;

type WR<T> = Arc<RwLock<T>>;

pub fn wms_handler<T: WorkflowRegistry>(
    workflow_registry: WR<T>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("wms"))
        .and(warp::query::<WMSRequest>())
        .and(warp::any().map(move || Arc::clone(&workflow_registry)))
        .and_then(wms)
}

// TODO: move into handler once async closures are available?
async fn wms<T: WorkflowRegistry>(
    request: WMSRequest,
    workflow_registry: WR<T>,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: authentication
    // TODO: more useful error output than "invalid query string"
    match request {
        WMSRequest::GetCapabilities(request) => get_capabilities(&request),
        WMSRequest::GetMap(request) => get_map(&request, &workflow_registry),
        WMSRequest::GetLegendGraphic(request) => get_legend_graphic(&request, &workflow_registry),
        _ => Ok(Box::new(
            warp::http::StatusCode::NOT_IMPLEMENTED.into_response(),
        )),
    }
}

fn get_capabilities(_request: &GetCapabilities) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: implement
    // TODO: at least inject correct url of the instance and return data for the default layer
    let mock = r#"<WMS_Capabilities xmlns="http://www.opengis.net/wms" xmlns:sld="http://www.opengis.net/sld" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.3.0" xsi:schemaLocation="http://www.opengis.net/wms http://schemas.opengis.net/wms/1.3.0/capabilities_1_3_0.xsd http://www.opengis.net/sld http://schemas.opengis.net/sld/1.1.0/sld_capabilities.xsd">
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
                            <OnlineResource xlink:href="http://localhost"/>
                        </Get>
                    </HTTP>
                </DCPType>
            </GetCapabilities>
            <GetMap>
                <Format>image/png</Format>
                <DCPType>
                    <HTTP>
                        <Get>
                            <OnlineResource xlink:href="http://localhost"/>
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
</WMS_Capabilities>"#;

    Ok(Box::new(warp::reply::html(mock)))
}

fn get_map<T: WorkflowRegistry>(
    request: &GetMap,
    _workflow_registry: &WR<T>,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: validate request?
    // TODO: properly handle request
    if request.layer == "test" {
        get_map_mock(request)
    } else {
        // workflow_registry.read().await.load(WorkflowIdentifier::from_uuid(request.layer.clone() as Uuid));
        Ok(Box::new(
            warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        ))
    }
}

fn get_legend_graphic<T: WorkflowRegistry>(
    _request: &GetLegendGraphic,
    _workflow_registry: &WR<T>,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: implement
    Ok(Box::new(
        warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    ))
}

fn get_map_mock(request: &GetMap) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let raster = Raster2D::new(
        [2, 2].into(),
        vec![
            0xFF00_00FF_u32,
            0x00FF_00FF_u32,
            0x0000_00FF_u32,
            0x0000_00FF_u32,
        ],
        None,
        Default::default(),
        Default::default(),
    )
    .context(error::DataType)
    .map_err(warp::reject::custom)?;

    let colorizer = Colorizer::rgba();
    let image_bytes = raster
        .to_png(request.width, request.height, &colorizer)
        .context(error::DataType)
        .map_err(warp::reject::custom)?;

    Ok(Box::new(
        Response::builder()
            .header("Content-Type", "image/png")
            .body(image_bytes)
            .context(error::HTTP)
            .map_err(warp::reject::custom)?,
    ))
}

#[cfg(test)]
mod tests {
    use crate::workflows::registry::HashMapRegistry;

    use super::*;

    #[tokio::test]
    async fn test() {
        let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));

        let res = warp::test::request()
            .method("GET")
            .path("/wms?request=GetMap&service=WMS&version=1.3.0&layer=test&bbox=1,2,3,4&width=100&height=100&crs=foo&styles=ssss&format=image/png")
            .reply(&wms_handler(workflow_registry))
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../datatypes/test-data/colorizer/rgba.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }

    #[tokio::test]
    async fn get_capabilities() {
        let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));

        let res = warp::test::request()
            .method("GET")
            .path("/wms?request=GetCapabilities&service=WMS")
            .reply(&wms_handler(workflow_registry))
            .await;
        assert_eq!(res.status(), 200);

        // TODO: validate xml?
    }

    #[tokio::test]
    async fn png_from_stream() {
        // let global_size_in_pixels = (1800, 3600);
        // let tile_size_in_pixels = (600, 600);

        // let grid_tile_provider = GdalSourceTileGridProvider {
        //     global_pixel_size: global_size_in_pixels.into(),
        //     tile_pixel_size: tile_size_in_pixels.into(),
        //     dataset_geo_transform: Default::default(),
        // };
        //
        // let time_interval_provider = vec![TimeInterval::new_unchecked(1, 2)];
        //
        // let gdal_params = GdalSourceParameters {
        //     base_path: "../operators/test-data/raster/modis_ndvi".into(),
        //     file_name_with_time_placeholder: "MOD13A2_M_NDVI_2014-01-01.TIFF".into(),
        //     time_format: "".into(),
        //     channel: None,
        // };

        // let gdal_source = GdalSource {
        //     time_interval_provider,
        //     grid_tile_provider,
        //     gdal_params,
        // };

        // let mut img = RgbaImage::new(255, 255);

        // gdal_source.tile_stream::<u8>().for_each(|tile| {
        //     if let Ok(tile) = tile {
        //         let width = tile.data.dimension().size_of_x_axis();
        //         let height = tile.data.dimension().size_of_y_axis();
        //
        //         for y in 0..height {
        //             for x in 0..width {
        //                 // TODO: calculate x, y in output img
        //                 // TODO: calculate Rgba color from pixel value at x, y
        //                 // TODO: img.put_pixel(x, y, Rgb([255, 0, 0]));
        //                 img.put_pixel(0, 0, Rgba([255, 0, 0, 255]));
        //             }
        //         }
        //     }
        //     futures::future::ready(())
        // }).await;

        // gdal_source.tile_stream::<u8>().fold(Raster::new(), |acc, tile| {
        //     if let Ok(tile) = tile {ll

        // let mut buffer = Vec::new();
        // DynamicImage::ImageRgba8(img)
        //     .write_to(&mut buffer, ImageFormat::Png).unwrap();

        // image::save_buffer(&Path::new("image.png"), &buffer, 255, 255, ColorType::Rgba8).unwrap();
        // img.save(&Path::new("image.png")).unwrap()

        // TODO: validate output image
    }
}
