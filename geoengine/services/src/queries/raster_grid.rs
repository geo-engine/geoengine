//! Rasters on a requested grid, encoded as PNG or `GeoTIFF`.

use crate::config::{self, get_config_element};
use crate::contexts::SessionContext;
use crate::error::Result;
use crate::processing_graphs::{
    processing_graph::ProcessingGraphId, registry::ProcessingGraphRegistry,
};
use crate::quota::ComputationId;
use geoengine_datatypes::{
    operations::image::{RasterColorizer, create_empty_no_data_color_png_bytes},
    primitives::{
        BandSelection, CacheHint, Coordinate2D, RasterQueryRectangle, SpatialPartition2D,
        SpatialResolution, TimeInterval,
    },
    raster::{GridBoundingBox2D, GridIntersection},
    spatial_reference::SpatialReference,
    util::Identifier,
};
use geoengine_operators::{
    call_on_generic_raster_processor, call_on_generic_raster_processor_gdal_types,
    engine::{
        ExecutionContext, ProcessingGraphOperatorPath, QueryContext, RasterResultDescriptor,
        TypedRasterQueryProcessor,
    },
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
    /// The first band as `GeoTIFF`, at most `[wcs] tile_limit` tiles. Unlike a PNG it has no
    /// requested size: its pixels are the query's grid, i.e. `bounds` at `resolution`.
    GeoTiff { no_data_value: Option<f64> },
}

/// Queries `query` and encodes the result as `encoding`
pub async fn raster_grid<C: SessionContext>(
    ctx: C,
    query: RasterGridQuery,
    encoding: RasterEncoding,
) -> Result<(Vec<u8>, CacheHint, ComputationId)> {
    let execution_context = ctx.execution_context()?;

    let wrapped = load_on_grid(&ctx, &query, &execution_context).await?;

    let grid_bounds = wrapped
        .result_descriptor
        .spatial_grid_descriptor()
        .tiling_grid_definition()
        .to_spatial_grid()
        .spatial_bounds_to_compatible_spatial_grid(query.bounds)
        .grid_bounds();

    let computation_id = ComputationId::new();

    if let RasterEncoding::Png {
        width,
        height,
        colorizer,
    } = &encoding
        && misses_data(&wrapped.result_descriptor, grid_bounds, query.time)
    {
        let empty_image =
            create_empty_no_data_color_png_bytes(*width, *height, colorizer.no_data_color())?;
        return Ok((
            empty_image,
            execution_context.default_cache_ttl().into(),
            computation_id,
        ));
    }

    let query_rect = RasterQueryRectangle::new(grid_bounds, query.time, query.bands);
    debug!("raster grid query rect: {:?}", query_rect);

    let processor = wrapped.initialized_operator.query_processor()?;
    let query_ctx = ctx.query_context(query.processing_graph, computation_id)?;

    let (bytes, cache_hint) = encode(processor, query_rect, query_ctx, encoding, query.crs).await?;

    Ok((bytes, cache_hint, computation_id))
}

/// Loads and initializes the processing graph, projected and resampled onto the query's grid
async fn load_on_grid<C: SessionContext>(
    ctx: &C,
    query: &RasterGridQuery,
    execution_context: &C::ExecutionContext,
) -> Result<WrapWithProjectionAndResample> {
    let operator = ctx
        .db()
        .load_processing_graph(&query.processing_graph)
        .await?
        .operator
        .get_raster()?;

    let initialized = operator
        .clone()
        .initialize(
            ProcessingGraphOperatorPath::initialize_root(),
            execution_context,
        )
        .await?;

    // TODO: add a resammple operator for downsampling AND resample push down!
    Ok(
        WrapWithProjectionAndResample::new_create_result_descriptor(operator, initialized)
            .wrap_with_projection_and_resample(
                query.origin,
                query.resolution,
                query.crs,
                execution_context,
            )
            .await?,
    )
}

/// Whether the query lies outside the data in time or space, so the result is empty
fn misses_data(
    result_descriptor: &RasterResultDescriptor,
    grid_bounds: GridBoundingBox2D,
    time: TimeInterval,
) -> bool {
    let intersects_time = result_descriptor
        .time
        .bounds
        .is_none_or(|b| b.intersects(&time));

    let intersects_space = result_descriptor
        .spatial_grid
        .tiling_grid_definition()
        .pixel_bounds
        .intersects(&grid_bounds);

    if !intersects_time || !intersects_space {
        debug!(
            "raster grid query does not intersect data bounds (time: {intersects_time}, space: {intersects_space})"
        );
        return true;
    }

    false
}

async fn encode<Q: QueryContext + 'static>(
    processor: TypedRasterQueryProcessor,
    query_rect: RasterQueryRectangle,
    query_ctx: Q,
    encoding: RasterEncoding,
    crs: SpatialReference,
) -> Result<(Vec<u8>, CacheHint)> {
    // cancellation is handled by the task that runs the query
    let not_canceled = Box::pin(futures::future::pending());

    let time = query_rect.time_interval();

    Ok(match encoding {
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
                )
                .await)??
        }
    })
}
