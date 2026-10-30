//! Rasters on a requested grid, encoded as PNG or `GeoTIFF`.

use crate::config::{self, get_config_element};
use crate::contexts::SessionContext;
use crate::error::Result;
use crate::processing_graphs::{
    processing_graph::ProcessingGraphId, registry::ProcessingGraphRegistry,
};
use crate::quota::ComputationId;
use geoengine_datatypes::{
    operations::image::RasterColorizer,
    primitives::{
        BandSelection, CacheHint, Coordinate2D, RasterQueryRectangle, SpatialPartition2D,
        SpatialResolution, TimeInterval,
    },
    raster::GridIntersection,
    spatial_reference::SpatialReference,
    util::Identifier,
};
use geoengine_operators::{
    call_on_generic_raster_processor, call_on_generic_raster_processor_gdal_types,
    engine::{ExecutionContext, ProcessingGraphOperatorPath},
    util::{
        WrapWithProjectionAndResample,
        raster_stream_to_geotiff::{
            GdalGeoTiffDatasetMetadata, GdalGeoTiffOptions,
            raster_stream_to_multiband_geotiff_bytes,
        },
        raster_stream_to_png::raster_stream_to_png_bytes,
    },
};
use tracing::debug;

/// A raster processing graph, projected and resampled onto the requested grid
pub struct RasterGridQuery {
    pub processing_graph: ProcessingGraphId,
    pub crs: SpatialReference,
    pub bounds: SpatialPartition2D,
    /// `None` keeps the processing graph's resolution
    pub resolution: Option<SpatialResolution>,
    /// `None` keeps the processing graph's grid origin
    pub origin: Option<Coordinate2D>,
    pub time: TimeInterval,
    pub bands: BandSelection,
}

pub enum RasterEncoding {
    /// An image of `width` × `height` pixels; empty if the query misses the data
    Png {
        width: u32,
        height: u32,
        colorizer: RasterColorizer,
    },
    /// The first band as `GeoTIFF`, at most `[wcs] tile_limit` tiles
    GeoTiff { no_data_value: Option<f64> },
}

/// Queries `query` and encodes the result as `encoding`
#[allow(clippy::too_many_lines)]
pub async fn raster_grid<C: SessionContext>(
    ctx: C,
    query: RasterGridQuery,
    encoding: RasterEncoding,
) -> Result<(Vec<u8>, CacheHint, ComputationId)> {
    let RasterGridQuery {
        processing_graph,
        crs,
        bounds,
        resolution,
        origin,
        time,
        bands,
    } = query;

    let operator = ctx
        .db()
        .load_processing_graph(&processing_graph)
        .await?
        .operator
        .get_raster()?;

    let execution_context = ctx.execution_context()?;
    let tiling_spec = execution_context.tiling_specification();

    let initialized = operator
        .clone()
        .initialize(
            ProcessingGraphOperatorPath::initialize_root(),
            &execution_context,
        )
        .await?;

    // TODO: add a resammple operator for downsampling AND resample push down!
    let wrapped =
        WrapWithProjectionAndResample::new_create_result_descriptor(operator, initialized)
            .wrap_with_projection_and_resample(origin, resolution, crs, &execution_context)
            .await?;

    let query_tiling_pixel_grid = wrapped
        .result_descriptor
        .spatial_grid_descriptor()
        .tiling_grid_definition(tiling_spec)
        .tiling_spatial_grid_definition()
        .spatial_bounds_to_compatible_spatial_grid(bounds);

    debug!(
        "raster grid re-scale-project: {:?}",
        query_tiling_pixel_grid
    );

    let computation_id = ComputationId::new();

    if let RasterEncoding::Png {
        width,
        height,
        colorizer,
    } = &encoding
    {
        let intersects_time = wrapped
            .result_descriptor
            .time
            .bounds
            .is_none_or(|b| b.intersects(&time));

        let intersects_space = wrapped
            .result_descriptor
            .spatial_grid
            .tiling_grid_definition(tiling_spec)
            .tiling_grid_bounds()
            .intersects(&query_tiling_pixel_grid.grid_bounds());

        if !intersects_time || !intersects_space {
            debug!(
                "raster grid query does not intersect data bounds (time: {intersects_time}, space: {intersects_space}), returning empty image"
            );

            let empty_image =
                geoengine_datatypes::operations::image::create_empty_no_data_color_png_bytes(
                    *width,
                    *height,
                    colorizer.no_data_color(),
                )?;

            return Ok((
                empty_image,
                execution_context.default_cache_ttl().into(),
                computation_id,
            ));
        }
    }

    let query_rect = RasterQueryRectangle::new(query_tiling_pixel_grid.grid_bounds(), time, bands);

    debug!("raster grid query rect: {:?}", query_rect);

    let processor = wrapped.initialized_operator.query_processor()?;

    let query_ctx = ctx.query_context(processing_graph, computation_id)?;

    // cancellation is handled by the task that runs this function
    let not_canceled = Box::pin(futures::future::pending());

    let (bytes, cache_hint) = match encoding {
        RasterEncoding::Png {
            width,
            height,
            colorizer,
        } => {
            // The raster to png code already resamples when the tiles are filled. We should add resample for lower resolutions
            call_on_generic_raster_processor!(
                processor,
                p => raster_stream_to_png_bytes(p, query_rect, query_ctx, width, height, Some(time), Some(colorizer), not_canceled).await
            )?
        }
        RasterEncoding::GeoTiff { no_data_value } => {
            call_on_generic_raster_processor_gdal_types!(processor, p =>
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
                .await)??
        }
    };

    Ok((bytes, cache_hint, computation_id))
}
