use warp::{Filter, http::Response};
use warp::reply::Reply;
use std::sync::Arc;
use tokio::sync::RwLock;
use snafu::ResultExt;

use crate::ogc::wms::request::{WMSRequest, GetCapabilities, GetMap, GetLegendGraphic};
use geoengine_datatypes::raster::Raster2D;
use geoengine_datatypes::operations::image::{Colorizer, ToPng};
use crate::workflows::registry::WorkflowRegistry;
use crate::error;

type WR<T> = Arc<RwLock<T>>;

pub fn wms_handler<T: WorkflowRegistry>(workflow_registry: WR<T>) -> impl Filter<Extract=impl warp::Reply, Error=warp::Rejection> + Clone {
    warp::get()
        .and(warp::path!("wms"))
        .and(warp::query::<WMSRequest>())
        .and(warp::any().map(move || Arc::clone(&workflow_registry)))
        .and_then(wms)
}

// TODO: move into handler once async closures are available?
async fn wms<T: WorkflowRegistry>(request: WMSRequest, workflow_registry: WR<T>) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: authentication
    // TODO: more useful error output than "invalid query string"
    match request {
        WMSRequest::GetCapabilities(request) => get_capabilities(&request),
        WMSRequest::GetMap(request) => get_map(&request, &workflow_registry),
        WMSRequest::GetLegendGraphic(request) => get_legend_graphic(&request, &workflow_registry),
        _ => Ok(Box::new(warp::http::StatusCode::NOT_IMPLEMENTED.into_response()))
    }
}

fn get_capabilities(_request: &GetCapabilities) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: implement
    Ok(Box::new(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

fn get_map<T: WorkflowRegistry>(request: &GetMap, _workflow_registry: &WR<T>) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: validate request?
    // TODO: properly handle request
    if request.layer == "test" {
        get_map_mock()
    } else {
        // workflow_registry.read().await.load(WorkflowIdentifier::from_uuid(request.layer.clone() as Uuid));
        Ok(Box::new(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()))
    }
}

fn get_legend_graphic<T: WorkflowRegistry>(_request: &GetLegendGraphic, _workflow_registry: &WR<T>) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: implement
    Ok(Box::new(warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()))
}

fn get_map_mock() -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let raster = Raster2D::new(
        [2, 2].into(),
        vec![0xFF00_00FF_u32, 0x00FF_00FF_u32, 0x0000_00FF_u32, 0x0000_00FF_u32],
        None,
        Default::default(),
        Default::default(),
    ).context(error::DataType).map_err(warp::reject::custom)?;

    let colorizer = Colorizer::rgba();
    let image_bytes = raster.to_png(100, 100, &colorizer)
        .context(error::DataType).map_err(warp::reject::custom)?;

    Ok(
        Box::new(Response::builder()
            .header("Content-Type", "image/png")
            .body(image_bytes).context(error::HTTP).map_err(warp::reject::custom)?
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflows::registry::HashMapRegistry;

    #[tokio::test]
    async fn test() {
        let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));

        let res = warp::test::request()
            .method("GET")
            .path("/wms?request=GetMap&service=WMS&version=1.3.0&layer=test&bbox=1,2,3,4&width=2&height=2&crs=foo&styles=ssss&format=image/png")
            .reply(&wms_handler(workflow_registry))
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../datatypes/test-data/colorizer/rgba.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }
}
