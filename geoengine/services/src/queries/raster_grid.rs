//! Rasters on a requested grid: WMS maps as PNG and WCS coverages as `GeoTIFF`.

use crate::api::model::datatypes::{RasterColorizer, SpatialReference};
use crate::api::ogc::util::OgcBoundingBox;
use crate::config::{self, get_config_element};
use crate::contexts::SessionContext;
use crate::error::{self, Result};
use crate::quota::ComputationId;
use crate::workflows::{registry::WorkflowRegistry, workflow::WorkflowId};
use geoengine_datatypes::{
    primitives::{
        AxisAlignedRectangle, BandSelection, CacheHint, Coordinate2D, RasterQueryRectangle,
        SpatialPartition2D, SpatialResolution, TimeInterval,
    },
    raster::GridIntersection,
    util::Identifier,
};
use geoengine_operators::{
    call_on_generic_raster_processor, call_on_generic_raster_processor_gdal_types,
    engine::{ExecutionContext, InitializedRasterOperator, WorkflowOperatorPath},
    util::{
        WrapWithProjectionAndResample,
        raster_stream_to_geotiff::{
            GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
            raster_stream_to_multiband_geotiff_bytes,
        },
        raster_stream_to_png::{default_colorizer_gradient, raster_stream_to_png_bytes},
    },
};
use tracing::debug;

/// A WMS `GetMap` request, validated by the handler
pub struct MapQuery {
    pub workflow: WorkflowId,
    pub bbox: OgcBoundingBox,
    /// `None` uses the workflow's spatial reference
    pub crs: Option<SpatialReference>,
    pub width: u32,
    pub height: u32,
    pub time: TimeInterval,
    pub colorizer: Option<RasterColorizer>,
}

/// A WCS `GetCoverage` request, validated by the handler
pub struct CoverageQuery {
    pub workflow: WorkflowId,
    pub partition: SpatialPartition2D,
    pub crs: geoengine_datatypes::spatial_reference::SpatialReference,
    pub resolution: Option<SpatialResolution>,
    pub time: TimeInterval,
    pub no_data_value: Option<f64>,
}

/// The query's spatial grid follows the tiling of the result after wrapping it into the
/// requested projection and resolution.
async fn initialize_on_grid<C: SessionContext>(
    ctx: &C,
    workflow: WorkflowId,
    target: impl FnOnce(
        &dyn InitializedRasterOperator,
    ) -> Result<(
        geoengine_datatypes::spatial_reference::SpatialReference,
        SpatialPartition2D,
        Option<Coordinate2D>,
        Option<SpatialResolution>,
    )>,
) -> Result<(
    WrapWithProjectionAndResample,
    SpatialPartition2D,
    C::ExecutionContext,
)> {
    let operator = ctx
        .db()
        .load_workflow(&workflow)
        .await?
        .operator
        .get_raster()?;

    let execution_context = ctx.execution_context()?;

    let initialized = operator
        .clone()
        .initialize(WorkflowOperatorPath::initialize_root(), &execution_context)
        .await?;

    let (crs, bounds, origin, resolution) = target(initialized.as_ref())?;

    let wrapped =
        WrapWithProjectionAndResample::new_create_result_descriptor(operator, initialized)
            .wrap_with_projection_and_resample(origin, resolution, crs, &execution_context)
            .await?;

    Ok((wrapped, bounds, execution_context))
}

/// Renders a WMS map as PNG
///
/// # Panics
/// Panics if the colorizer selects more bands than fit into `u32`.
pub async fn map_png<C: SessionContext>(
    ctx: C,
    query: MapQuery,
) -> Result<(Vec<u8>, CacheHint, ComputationId)> {
    let MapQuery {
        workflow,
        bbox,
        crs,
        width,
        height,
        time,
        colorizer,
    } = query;

    let (wrapped, request_bounds, execution_context) =
        initialize_on_grid(&ctx, workflow, |initialized| {
            // Use the datasets crs as default spatial reference if none is requested
            let request_spatial_ref: SpatialReference = crs
                .or(initialized
                    .result_descriptor()
                    .spatial_reference
                    .as_option()
                    .map(Into::into))
                .ok_or(error::Error::MissingSpatialReference)?;

            let request_bounds: SpatialPartition2D = bbox.bounds(request_spatial_ref)?;
            let x_request_res = request_bounds.size_x() / f64::from(width);
            let y_request_res = request_bounds.size_y() / f64::from(height);
            let request_resolution =
                SpatialResolution::new(x_request_res.abs(), y_request_res.abs())?;

            Ok((
                request_spatial_ref.into(),
                request_bounds,
                None, // this needs to be `None`` to avoid moving the data origin to a png pixel origin. Since the WMS requests are not consistent with their origins using them distortes the results more then it helps.
                Some(request_resolution),
            ))
        })
        .await?;
    // TODO: add a resammple operator for downsampling AND resample push down!

    let tiling_spec = execution_context.tiling_specification();
    let computation_id = ComputationId::new();

    let processor = wrapped.initialized_operator.query_processor()?;

    let query_tiling_pixel_grid = wrapped
        .result_descriptor
        .spatial_grid_descriptor()
        .tiling_grid_definition(tiling_spec)
        .tiling_spatial_grid_definition()
        .spatial_bounds_to_compatible_spatial_grid(request_bounds);

    let attributes = colorizer.as_ref().map_or_else(
        // TODO: move this to a method of RasterColorizer
        || BandSelection::new_single(0),
        |colorizer: &RasterColorizer| {
            RasterColorizer::band_selection(colorizer)
                .try_into()
                .expect("conversion of usize to u32 succeeds for small band numbers")
        },
    );

    let raster_colorizer = colorizer.map_or_else(
        || geoengine_datatypes::operations::image::RasterColorizer::SingleBand {
            band: 0,
            band_colorizer: default_colorizer_gradient::<u8>(),
        },
        Into::into,
    );

    let result_descriptor_intersects_query_time = wrapped
        .result_descriptor
        .time
        .bounds
        .is_none_or(|b| b.intersects(&time));

    let result_descriptor_intersects_query_space = wrapped
        .result_descriptor
        .spatial_grid
        .tiling_grid_definition(tiling_spec)
        .tiling_grid_bounds()
        .intersects(&query_tiling_pixel_grid.grid_bounds());

    if !result_descriptor_intersects_query_time || !result_descriptor_intersects_query_space {
        debug!(
            "WMS request does not intersect data bounds (time: {}, space: {}), returning empty image",
            result_descriptor_intersects_query_time, result_descriptor_intersects_query_space
        );

        let empty_image =
            geoengine_datatypes::operations::image::create_empty_no_data_color_png_bytes(
                width,
                height,
                raster_colorizer.no_data_color(),
            )?;

        return Ok((
            empty_image,
            execution_context.default_cache_ttl().into(),
            computation_id,
        ));
    }

    debug!("WMS re-scale-project: {:?}", query_tiling_pixel_grid);

    let query_rect =
        RasterQueryRectangle::new(query_tiling_pixel_grid.grid_bounds(), time, attributes);

    debug!("WMS query rect: {:?}", query_rect);

    let query_ctx = ctx.query_context(workflow, computation_id)?;

    // cancellation is handled by the task that runs this function
    let not_canceled = Box::pin(futures::future::pending());

    // The raster to png code already resamples when the tiles are filled. We should add resample for lower resolutions
    let (image_bytes, cache_hint) = call_on_generic_raster_processor!(
        processor,
        p =>
            raster_stream_to_png_bytes(p, query_rect, query_ctx, width, height, Some(time), Some(raster_colorizer), not_canceled).await
    )?;

    Ok((image_bytes, cache_hint, computation_id))
}

/// Writes a WCS coverage as `GeoTIFF`
pub async fn coverage_geotiff<C: SessionContext>(
    ctx: C,
    query: CoverageQuery,
) -> Result<(Vec<u8>, CacheHint, ComputationId)> {
    let CoverageQuery {
        workflow,
        partition,
        crs,
        resolution,
        time,
        no_data_value,
    } = query;

    let (wrapped, partition, execution_context) = initialize_on_grid(&ctx, workflow, |_| {
        Ok((
            crs,
            partition,
            Some(partition.upper_left()), // TODO: set none if not changed? But how to handle mapping to grid?
            resolution,
        ))
    })
    .await?;

    let tiling_spec = execution_context.tiling_specification();

    let query_tiling_pixel_grid = wrapped
        .result_descriptor
        .spatial_grid_descriptor()
        .tiling_grid_definition(tiling_spec)
        .tiling_spatial_grid_definition()
        .spatial_bounds_to_compatible_spatial_grid(partition);

    let query_rect = RasterQueryRectangle::new(
        query_tiling_pixel_grid.grid_bounds(),
        time,
        BandSelection::first(), // TODO: support multi bands in API and set the selection here
    );

    let processor = wrapped.initialized_operator.query_processor()?;

    let computation_id = ComputationId::new();

    let query_ctx = ctx.query_context(workflow, computation_id)?;

    // cancellation is handled by the task that runs this function
    let not_canceled = Box::pin(futures::future::pending());

    let (bytes, cache_hint) = call_on_generic_raster_processor_gdal_types!(processor, p =>
        raster_stream_to_multiband_geotiff_bytes(
            p,
            query_rect,
            query_ctx,
            GdalGeoTiffDatasetMetadata {
                no_data_value,
                spatial_reference: crs,
            },
            GdalGeoTiffOptions {
                compression_num_threads: get_config_element::<config::Gdal>()?.compression_num_threads,
                as_cog: false,
                force_big_tiff: false,
            },
            Some(get_config_element::<config::Wcs>()?.tile_limit),
            not_canceled,
            tiling_spec,
        )
        .await)??;

    Ok((bytes, cache_hint, computation_id))
}
