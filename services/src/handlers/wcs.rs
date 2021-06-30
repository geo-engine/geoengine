use std::str::FromStr;

use geoengine_operators::util::raster_stream_to_geotiff::raster_stream_to_geotiff_bytes;
use snafu::ResultExt;
use warp::{http::Response, Filter};

use geoengine_datatypes::primitives::BoundingBox2D;
use geoengine_datatypes::{primitives::SpatialResolution, spatial_reference::SpatialReference};

use crate::contexts::MockableSession;
use crate::error;
use crate::error::Result;
use crate::handlers::Context;
use crate::ogc::wcs::request::{GetCoverage, WcsRequest};
use crate::util::config::get_config_element;
use crate::workflows::registry::WorkflowRegistry;
use crate::workflows::workflow::WorkflowId;

use geoengine_datatypes::primitives::{TimeInstance, TimeInterval};
use geoengine_operators::engine::RasterOperator;
use geoengine_operators::engine::{QueryRectangle, ResultDescriptor};
use geoengine_operators::processing::{Reprojection, ReprojectionParams};

pub(crate) fn wcs_handler<C: Context>(
    ctx: C,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("wcs")
        .and(warp::get())
        .and(warp::query::<WcsRequest>())
        .and(warp::any().map(move || ctx.clone()))
        .and_then(wcs)
}

// TODO: move into handler once async closures are available?
async fn wcs<C: Context>(
    request: WcsRequest,
    ctx: C,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: authentication
    // TODO: more useful error output than "invalid query string"
    match request {
        WcsRequest::GetCoverage(request) => get_coverage(&request, &ctx).await,
        _ => Err(error::Error::NotYetImplemented.into()),
    }
}

#[allow(clippy::too_many_lines)]
async fn get_coverage<C: Context>(
    request: &GetCoverage,
    ctx: &C,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    // TODO: validate request?

    let workflow = ctx
        .workflow_registry_ref()
        .await
        .load(&WorkflowId::from_str(&request.coverageid)?)
        .await?;

    let operator = workflow.operator.get_raster().context(error::Operator)?;

    // TODO: use correct session when WCS uses authenticated access
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
    let request_spatial_ref: SpatialReference = request
        .srsname
        .ok_or(error::Error::InvalidSpatialReference)?;

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

    let query_bbox = if let (Some(subset_x), Some(subset_y)) = (request.subset_x, request.subset_y)
    {
        BoundingBox2D::new(
            (subset_x.0, subset_y.0).into(),
            (subset_x.1, subset_y.1).into(),
        )
        .context(error::DataType)?
    } else {
        let area_of_use = request_spatial_ref
            .area_of_use_projected()
            .context(error::DataType)?;

        let subset_x = request
            .subset_x
            .unwrap_or_else(|| (area_of_use.lower_left().x, area_of_use.upper_right().x));
        let subset_y = request
            .subset_y
            .unwrap_or_else(|| (area_of_use.lower_left().y, area_of_use.upper_right().y));

        BoundingBox2D::new(
            (subset_x.0, subset_y.0).into(),
            (subset_x.1, subset_y.1).into(),
        )
        .context(error::DataType)?
    };

    let x_query_resolution = query_bbox.size_x() / f64::from(request.size_x);
    let y_query_resolution = query_bbox.size_y() / f64::from(request.size_y);

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

    let bytes = match processor {
        geoengine_operators::engine::TypedRasterQueryProcessor::U8(p) => {
            raster_stream_to_geotiff_bytes(
                p,
                query_rect,
                query_ctx,
                no_data_value,
                request_spatial_ref,
                Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
            )
            .await
        }
        geoengine_operators::engine::TypedRasterQueryProcessor::U16(p) => {
            raster_stream_to_geotiff_bytes(
                p,
                query_rect,
                query_ctx,
                no_data_value,
                request_spatial_ref,
                Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
            )
            .await
        }
        geoengine_operators::engine::TypedRasterQueryProcessor::U32(p) => {
            raster_stream_to_geotiff_bytes(
                p,
                query_rect,
                query_ctx,
                no_data_value,
                request_spatial_ref,
                Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
            )
            .await
        }
        geoengine_operators::engine::TypedRasterQueryProcessor::I16(p) => {
            raster_stream_to_geotiff_bytes(
                p,
                query_rect,
                query_ctx,
                no_data_value,
                request_spatial_ref,
                Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
            )
            .await
        }
        geoengine_operators::engine::TypedRasterQueryProcessor::I32(p) => {
            raster_stream_to_geotiff_bytes(
                p,
                query_rect,
                query_ctx,
                no_data_value,
                request_spatial_ref,
                Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
            )
            .await
        }
        geoengine_operators::engine::TypedRasterQueryProcessor::F32(p) => {
            raster_stream_to_geotiff_bytes(
                p,
                query_rect,
                query_ctx,
                no_data_value,
                request_spatial_ref,
                Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
            )
            .await
        }
        geoengine_operators::engine::TypedRasterQueryProcessor::F64(p) => {
            raster_stream_to_geotiff_bytes(
                p,
                query_rect,
                query_ctx,
                no_data_value,
                request_spatial_ref,
                Some(get_config_element::<crate::util::config::Wcs>()?.tile_limit),
            )
            .await
        }
        _ => return Err(error::Error::RasterDataTypeNotSupportByGdal.into()),
    }
    .map_err(error::Error::from)?;

    Ok(Box::new(
        Response::builder()
            .header("Content-Type", "image/tiff")
            .body(bytes)
            .context(error::Http)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contexts::InMemoryContext;
    use crate::ogc::wcs::request::GetCoverageFormat;
    use crate::util::tests::register_ndvi_workflow_helper;

    #[test]
    fn deserialize() {
        let params = &[
            ("request", "GetCoverage"),
            ("version", "2.0.1"),
            ("coverageid", "1234"),
            ("subset_x", "(-10,50)"),
            ("subset_y", "(20,80)"),
            ("size_x", "600"),
            ("size_y", "600"),
            ("srsname", "EPSG:4326"),
            ("format", "image/tiff"),
            ("time", "2014-01-01T00:00:00.0Z"),
        ];
        let string = serde_urlencoded::to_string(params).unwrap();

        let coverage: WcsRequest = serde_urlencoded::from_str(&string).unwrap();
        assert_eq!(
            WcsRequest::GetCoverage(GetCoverage {
                version: "2.0.1".to_owned(),
                format: GetCoverageFormat::ImageTiff,
                coverageid: "1234".to_owned(),
                time: Some(TimeInterval::new_instant(1_388_534_400_000).unwrap()),
                subset_x: Some((-10., 50.)),
                subset_y: Some((20., 80.)),
                size_x: 600,
                size_y: 600,
                srsname: Some(SpatialReference::epsg_4326()),
            }),
            coverage
        );
    }

    #[tokio::test]
    async fn get_coverage() {
        let ctx = InMemoryContext::default();

        let (_, id) = register_ndvi_workflow_helper(&ctx).await;

        let params = &[
            ("request", "GetCoverage"),
            ("version", "2.0.1"),
            ("coverageid", &id.to_string()),
            ("subset_x", "(-10,50)"),
            ("subset_y", "(20,80)"),
            ("size_x", "600"),
            ("size_y", "600"),
            ("srsname", "EPSG:4326"),
            ("format", "image/tiff"),
            ("time", "2014-01-01T00:00:00.0Z"),
        ];

        let res = warp::test::request()
            .method("GET")
            .path(&format!(
                "/wcs?{}",
                serde_urlencoded::to_string(params).unwrap()
            ))
            .reply(&wcs_handler(ctx))
            .await;

        assert_eq!(res.status(), 200);
        assert_eq!(
            include_bytes!("../../../operators/test-data/raster/geotiff_from_stream.tiff")
                as &[u8],
            res.body().to_vec().as_slice()
        );
    }
}
