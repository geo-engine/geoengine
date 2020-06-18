use warp::{Filter, http::Response};
use warp::reply::Reply;

use crate::ogc::wms::request::{WMSRequest, GetCapabilities, GetMap};
use geoengine_datatypes::raster::Raster2D;
use geoengine_datatypes::operations::image::{Colorizer, ToPng};
use crate::error::Error;

pub fn wms_handler() -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("wms"))
        .and(warp::query::<WMSRequest>())
        .and_then(wms)
}

// TODO: move into handler once async closures are available?
async fn wms(request: WMSRequest) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: authentication
    // TODO: more useful error output than "invalid query string"
    match request {
        WMSRequest::GetCapabilities(request) => Ok(Box::new(get_capabilities(&request)?)),
        WMSRequest::GetMap(request) => get_map(&request),
        // TODO: support other requests
        _ => Ok(Box::new(warp::http::StatusCode::NOT_IMPLEMENTED.into_response()))
    }
}

fn get_capabilities(_request: &GetCapabilities) -> Result<impl warp::Reply, warp::Rejection> {
    // TODO: implement
    Ok(warp::http::StatusCode::NOT_IMPLEMENTED.into_response())
}

fn get_map(request: &GetMap) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: validate request?
    // TODO: properly handle request
    if request.layer == "test" {
        get_map_mock()
    } else {
        Ok(Box::new(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()))
    }
}

fn get_map_mock() -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let raster = Raster2D::new(
        [2, 2].into(),
        vec![0xFF00_00FF_u32, 0x00FF_00FF_u32, 0x0000_00FF_u32, 0x0000_00FF_u32],
        None,
        Default::default(),
        Default::default(),
    ).map_err(Error::from).map_err(warp::reject::custom)?;

    let colorizer = Colorizer::rgba();
    let image_bytes = raster.to_png(100, 100, &colorizer)
        .map_err(Error::from).map_err(warp::reject::custom)?;

    Ok(
        Box::new(Response::builder()
            .header("Content-Type", "image/png")
            .body(image_bytes).map_err(Error::from).map_err(warp::reject::custom)?
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() {
        let res = warp::test::request()
            .method("GET")
            .path("/wms?request=GetMap&service=WMS&version=1.3.0&layer=test&bbox=1,2,3,4&width=2&height=2&crs=foo&styles=ssss&format=image/png")
            .reply(&wms_handler())
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../datatypes/test-data/colorizer/rgba.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }
}
