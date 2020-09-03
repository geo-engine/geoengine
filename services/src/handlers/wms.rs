use std::sync::Arc;

use snafu::ResultExt;
use tokio::sync::RwLock;
use uuid::Uuid;
use warp::reply::Reply;
use warp::{http::Response, Filter};

use geoengine_datatypes::operations::image::{Colorizer, ToPng};
use geoengine_datatypes::raster::{Blit, GeoTransform, Pixel, Raster2D};

use crate::error;
use crate::error::Result;
use crate::ogc::wms::request::{GetCapabilities, GetLegendGraphic, GetMap, WMSRequest};
use crate::util::identifiers::Identifier;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;
use futures::StreamExt;
use geoengine_datatypes::primitives::{TimeInstance, TimeInterval};
use geoengine_operators::engine::{
    ExecutionContext, QueryContext, QueryRectangle, RasterQueryProcessor, TypedRasterQueryProcessor,
};

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
        WMSRequest::GetMap(request) => get_map(&request, &workflow_registry).await,
        WMSRequest::GetLegendGraphic(request) => get_legend_graphic(&request, &workflow_registry),
        _ => Ok(Box::new(
            warp::http::StatusCode::NOT_IMPLEMENTED.into_response(),
        )),
    }
}

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

async fn get_map<T: WorkflowRegistry>(
    request: &GetMap,
    workflow_registry: &WR<T>,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: validate request?
    if request.layer == "test" {
        return get_map_mock(request);
    }

    let workflow = workflow_registry.read().await.load(&WorkflowId::from_uuid(
        Uuid::parse_str(&request.layer).context(error::Uuid)?,
    ));

    let workflow = if let Some(workflow) = workflow {
        workflow
    } else {
        // TODO: output error
        // TODO: respect GetMapExceptionFormat
        return Ok(Box::new(
            warp::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        ));
    };
    let operator = workflow.operator.get_raster().context(error::Operator)?;

    let execution_context = ExecutionContext;
    let initialized = operator
        .initialize(execution_context)
        .context(error::Operator)?;

    let processor = initialized.query_processor().context(error::Operator)?;

    let query_rect = QueryRectangle {
        bbox: request.bbox,
        time_interval: request.time.unwrap_or_else(|| {
            let time = TimeInstance::from(chrono::offset::Utc::now());
            TimeInterval::new_unchecked(time, time)
        }),
    };
    let query_ctx = QueryContext {
        // TODO: define meaningful query context
        chunk_byte_size: 1024,
    };

    let image_bytes = match processor {
        TypedRasterQueryProcessor::U8(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::U16(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::U32(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::U64(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::I8(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::I16(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::I32(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::I64(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::F32(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
        TypedRasterQueryProcessor::F64(p) => {
            raster_stream_to_png_bytes(p, query_rect, query_ctx, request).await
        }
    }?;

    Ok(Box::new(
        Response::builder()
            .header("Content-Type", "image/png")
            .body(image_bytes)
            .context(error::HTTP)?,
    ))
}

async fn raster_stream_to_png_bytes<T>(
    processor: Box<dyn RasterQueryProcessor<RasterType = T>>,
    query_rect: QueryRectangle,
    query_ctx: QueryContext,
    request: &GetMap,
) -> Result<Vec<u8>>
where
    T: Pixel,
{
    let tile_stream = processor.raster_query(query_rect, query_ctx);

    // build png
    let dim = [request.height as usize, request.width as usize];
    let data: Vec<T> = vec![T::zero(); dim[0] * dim[1]];
    let query_geo_transform = GeoTransform::new(
        query_rect.bbox.upper_left(),
        query_rect.bbox.size_x() / f64::from(request.width),
        -query_rect.bbox.size_y() / f64::from(request.height), // TODO: negativ, s.t. geo transform fits...
    );

    let output_raster: Result<Raster2D<T>> = Raster2D::new(
        dim.into(),
        data,
        None,
        request.time.unwrap_or_default(),
        query_geo_transform,
    )
    .context(error::DataType);

    let output_raster = tile_stream
        .fold(output_raster, |raster2d, tile| {
            let result: Result<Raster2D<T>> = match (raster2d, tile) {
                (Ok(mut raster2d), Ok(tile)) => match raster2d.blit(tile.data) {
                    Ok(_) => Ok(raster2d),
                    Err(error) => Err(error.into()),
                },
                (Err(error), _) => Err(error),
                (_, Err(error)) => Err(error.into()),
            };

            match result {
                Ok(updated_rasted2d) => futures::future::ok(updated_rasted2d),
                Err(error) => futures::future::err(error),
            }
        })
        .await?;

    let colorizer = Colorizer::rgba(); // TODO: create colorizer from request

    Ok(output_raster.to_png(request.width, request.height, &colorizer)?)
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
            0x0000_00FF_u32,
            0x00FF_00FF_u32,
            0x0000_00FF_u32,
        ],
        None,
        Default::default(),
        Default::default(),
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
            .context(error::HTTP)?,
    ))
}

#[cfg(test)]
mod tests {
    use geoengine_datatypes::primitives::{BoundingBox2D, TimeInterval};
    use geoengine_operators::engine::{RasterOperator, TypedOperator};
    use geoengine_operators::source::{
        gdal_source::GdalSourceProcessor, GdalSource, GdalSourceParameters,
    };

    use crate::workflows::registry::HashMapRegistry;

    use super::*;
    use crate::ogc::wms::request::GetMapFormat;
    use crate::workflows::workflow::Workflow;
    use xml::ParserConfig;

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

        // TODO: validate against schema
        let reader = ParserConfig::default().create_reader(res.body().as_ref());

        for event in reader {
            assert!(event.is_ok());
        }
    }

    #[tokio::test]
    async fn png_from_stream() {
        let gdal_params = GdalSourceParameters {
            dataset_id: "test".to_owned(),
            channel: None,
        };

        let gdal_source =
            GdalSourceProcessor::<_, u8>::from_params_with_json_provider(gdal_params).unwrap();

        let query_bbox = BoundingBox2D::new((-10., 20.).into(), (50., 80.).into()).unwrap();

        let image_bytes = raster_stream_to_png_bytes(
            gdal_source.boxed(),
            QueryRectangle {
                bbox: query_bbox,
                time_interval: TimeInterval::default(),
            },
            QueryContext { chunk_byte_size: 0 },
            &GetMap {
                version: "".to_string(),
                width: 600,
                height: 600,
                bbox: query_bbox,
                format: GetMapFormat::ImagePng,
                layer: "".to_string(),
                crs: "".to_string(),
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
    async fn get_map() {
        let workflow_registry = Arc::new(RwLock::new(HashMapRegistry::default()));

        let workflow = Workflow {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters {
                        dataset_id: "test".to_owned(),
                        channel: None,
                    },
                }
                .boxed(),
            ),
        };

        let id = workflow_registry.write().await.register(workflow.clone());

        let res = warp::test::request()
            .method("GET")
            .path(&format!("/wms?request=GetMap&service=WMS&version=1.3.0&layer={}&bbox=-10,20,50,80&width=600&height=600&crs=foo&styles=ssss&format=image/png", id.to_string()))
            .reply(&wms_handler(workflow_registry))
            .await;
        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../services/test-data/wms/raster.png") as &[u8],
            res.body().to_vec().as_slice()
        );
    }
}
