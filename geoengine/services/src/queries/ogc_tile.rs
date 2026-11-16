//! OGC API map tiles of a layer as PNG.

use crate::config::{self, get_config_element};
use crate::processing_graphs::processing_graph::ProcessingGraph;
use crate::{
    api::handlers::ogc::{
        OgcApiResult, error,
        error::OgcApiError,
        tms_spec::{TileMatrixSetProvider, TypedTileMatrixSetProvider},
        util::{get_initialized_raster_operator, load_layer, reproject_if_necessary},
    },
    api::model::datatypes::{DataProviderId, LayerId},
    contexts::{ApplicationContext, SessionContext},
    layers::layer::Layer,
    processing_graphs::registry::ProcessingGraphRegistry,
    projects::Symbology,
    quota::ComputationId,
};
use geoengine_datatypes::{
    error::BoxedResultExt,
    operations::image::RasterColorizer,
    primitives::{CacheHint, RasterQueryRectangle, TimeInterval},
    raster::GridShape2D,
    util::Identifier,
};
#[cfg(debug_assertions)]
use geoengine_operators::engine::RasterResultDescriptor;
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, InitializedRasterOperator, ProcessingGraphOperatorPath, TypedOperator,
        TypedRasterOperator, TypedRasterQueryProcessor,
    },
    util::{
        WrapWithProjectionAndResample,
        raster_stream_to_png::{default_raster_colorizer, raster_stream_to_png_bytes},
    },
};
use ogcapi_types::tiles::TileMatrixSetId;
use tracing::{Instrument, debug_span};

/// A tile of an OGC API tile matrix set
pub struct TileQuery {
    pub data_connector_id: DataProviderId,
    pub layer_id: LayerId,

    pub time_interval: TimeInterval,

    pub tile_matrix_set_id: TileMatrixSetId,
    pub tile_matrix: u8,
    pub tile_row: u32,
    pub tile_col: u32,
}

impl TileQuery {
    fn check_inside_bounds(
        &self,
        expected_number_of_tiles_at_zoom_level: GridShape2D,
    ) -> OgcApiResult<()> {
        let (tile_row, tile_col) = (self.tile_row as usize, self.tile_col as usize);
        if tile_row >= expected_number_of_tiles_at_zoom_level.y()
            || tile_col >= expected_number_of_tiles_at_zoom_level.x()
        {
            return Err(OgcApiError::TileCoordinatesOutOfBounds {
                matrix: self.tile_matrix.to_string(),
                row: self.tile_row,
                col: self.tile_col,
            });
        }

        Ok(())
    }
}

/// Renders the tile of `query`
pub async fn render_tile<C: ApplicationContext>(
    ctx: C::SessionContext,
    query: TileQuery,
) -> OgcApiResult<(Vec<u8>, CacheHint, ComputationId)> {
    let execution_context = ctx.execution_context()?;

    let mut layer: Layer = load_layer::<C>(&ctx, query.data_connector_id, query.layer_id.clone())
        .instrument(debug_span!("load_processing_graph"))
        .await?;
    let layer_operator = layer
        .processing_graph
        .operator
        .clone()
        .get_raster()
        .map_err(crate::error::Error::from)?;
    let mut initialized_operator =
        get_initialized_raster_operator::<C::SessionContext>(&layer, &execution_context)
            .instrument(debug_span!("initialize"))
            .await?;
    let tiling_specification = execution_context.tiling_specification();

    let required_origin_and_resolution =
        TypedTileMatrixSetProvider::required_origin_and_resolution(
            &query.tile_matrix_set_id,
            initialized_operator.result_descriptor(),
        )?;
    reproject_if_necessary::<C::SessionContext>(
        &mut layer,
        &mut initialized_operator,
        &execution_context,
        TypedTileMatrixSetProvider::required_srs(&query.tile_matrix_set_id),
        required_origin_and_resolution,
    )
    .instrument(debug_span!("initialize"))
    .await?;

    let tms_spec = TypedTileMatrixSetProvider::resolve(
        &query.tile_matrix_set_id,
        initialized_operator.result_descriptor(),
        tiling_specification,
    )?;

    let (expected_number_of_tiles_at_zoom_level, _) =
        tms_spec.grid_shape_and_origin(query.tile_matrix);
    query.check_inside_bounds(expected_number_of_tiles_at_zoom_level)?;

    #[cfg(debug_assertions)]
    let original_result_descriptor = initialized_operator.result_descriptor().clone();

    let spatial_resolution = tms_spec.spatial_resolution(query.tile_matrix);
    if spatial_resolution
        != initialized_operator
            .result_descriptor()
            .spatial_grid
            .spatial_resolution()
    {
        initialized_operator = initialized_operator
            .optimize_and_reinitialize(spatial_resolution, &execution_context)
            .instrument(debug_span!("initialize"))
            .await
            .boxed_context(error::InitializingProcessingGraph)?;
    }

    #[cfg(debug_assertions)]
    assert_multiple_of_original_resolution(
        &original_result_descriptor,
        initialized_operator.result_descriptor(),
    );

    let tile_size_in_pixels = tms_spec.tile_size_in_pixels(query.tile_matrix);
    let (tile_width, tile_height) = (
        u32::try_from(tile_size_in_pixels.x()).unwrap_or(u32::MAX),
        u32::try_from(tile_size_in_pixels.y()).unwrap_or(u32::MAX),
    );

    // The tile matrix's grid and the tile's pixels in it come from the reprojection and resampling
    // above. The tile itself is computed in one `Warp` from the layer onto that grid, as for a WMS
    // `GetMap`, with the resampling methods for the colorizer it is rendered with.
    let tile_bounds = tms_spec.tile_grid_bbox(
        &initialized_operator
            .result_descriptor()
            .tiling_grid_definition(tiling_specification),
        query.tile_matrix,
        query.tile_row,
        query.tile_col,
    )?;
    let (initialized_operator, colorizer) = warp_onto_grid(
        &mut layer,
        layer_operator,
        &*initialized_operator,
        &execution_context,
    )
    .await?;

    let query_rect =
        RasterQueryRectangle::new(tile_bounds, query.time_interval, colorizer.band_selection());

    let (processor, query_ctx, computation_id) =
        create_query_processor_and_query_context(&layer, &*initialized_operator, &ctx).await?;

    // cancellation is handled by the task that runs this function
    let not_canceled = Box::pin(futures::future::pending());

    let (image_bytes, cache_hint) = call_on_generic_raster_processor!(
        processor,
        p => raster_stream_to_png_bytes(
            p,
            query_rect,
            query_ctx,
            tile_width,
            tile_height,
            Some(query.time_interval),
            Some(colorizer),
            not_canceled,
        ).await
    )
    .map_err(crate::error::Error::from)?;

    Ok((image_bytes, cache_hint, computation_id))
}

#[cfg(debug_assertions)]
fn assert_multiple_of_original_resolution(
    original_result_descriptor: &RasterResultDescriptor,
    new_result_descriptor: &RasterResultDescriptor,
) {
    let original_pixel_resolution = original_result_descriptor
        .spatial_grid_descriptor()
        .grid_shape();
    let new_pixel_resolution = new_result_descriptor.spatial_grid_descriptor().grid_shape();

    // This should hold, but currently does not for all layers, so it only logs an error.
    // Re-enable the asserts once it holds.
    // debug_assert!(
    //     original_pixel_resolution
    //         .x()
    //         .is_multiple_of(new_pixel_resolution.x()),
    //     "New resolution is not a multiple of the original resolution: original: {original_pixel_resolution:?}, new: {new_pixel_resolution:?}",
    // );
    // debug_assert!(
    //     original_pixel_resolution
    //         .y()
    //         .is_multiple_of(new_pixel_resolution.y()),
    //     "New resolution is not a multiple of the original resolution: original: {original_pixel_resolution:?}, new: {new_pixel_resolution:?}",
    // );

    if !original_pixel_resolution
        .x()
        .is_multiple_of(new_pixel_resolution.x())
        || !original_pixel_resolution
            .y()
            .is_multiple_of(new_pixel_resolution.y())
    {
        tracing::error!(
            "New resolution is not a multiple of the original resolution: original: {original_pixel_resolution:?}, new: {new_pixel_resolution:?}"
        );
    }
}

/// Puts `layer_operator` onto the grid of `grid_operator` in one `Warp` and makes it the processing
/// graph of `layer`. Returns it with the colorizer to render it with: the one of the layer's
/// symbology, or else [`default_raster_colorizer`]. The colorizer chooses the resampling methods
/// (see [`geoengine_operators::util::WarpMethods::for_symbology`]), unless `[ogc] map_resampling`
/// is `"nearest"`.
async fn warp_onto_grid(
    layer: &mut Layer,
    layer_operator: TypedRasterOperator,
    grid_operator: &dyn InitializedRasterOperator,
    execution_context: &dyn ExecutionContext,
) -> OgcApiResult<(Box<dyn InitializedRasterOperator>, RasterColorizer)> {
    let layer_initialized = layer_operator
        .clone()
        .initialize(
            ProcessingGraphOperatorPath::initialize_root(),
            execution_context,
        )
        .instrument(debug_span!("initialize_layer"))
        .await
        .boxed_context(error::InitializingProcessingGraph)?;

    let colorizer = layer
        .symbology
        .as_ref()
        .and_then(Symbology::as_raster_symbology)
        .map_or_else(
            || default_raster_colorizer(layer_initialized.result_descriptor()),
            |symbology| symbology.raster_colorizer.clone(),
        );
    let methods = get_config_element::<config::Ogc>()?
        .map_resampling
        .methods(&colorizer);
    let grid = grid_operator.result_descriptor().spatial_grid_descriptor();
    let spatial_reference = grid_operator
        .result_descriptor()
        .spatial_reference
        .as_option()
        .ok_or(OgcApiError::MissingSpatialReference)?;

    let wrapped = WrapWithProjectionAndResample::new_create_result_descriptor(
        layer_operator,
        layer_initialized,
    )
    .wrap_with_projection_and_resample(
        Some(grid.geo_transform().origin_coordinate),
        Some(grid.spatial_resolution()),
        spatial_reference,
        methods,
        execution_context,
    )
    .instrument(debug_span!("initialize_warp"))
    .await
    .boxed_context(error::InitializingProcessingGraph)?;

    layer.processing_graph = ProcessingGraph {
        operator: TypedOperator::Raster(wrapped.operator),
    };
    Ok((wrapped.initialized_operator, colorizer))
}

async fn create_query_processor_and_query_context<C: SessionContext>(
    layer: &Layer,
    initialized_operator: &dyn InitializedRasterOperator,
    ctx: &C,
) -> OgcApiResult<(TypedRasterQueryProcessor, C::QueryContext, ComputationId)> {
    let processing_graph_id = ctx
        .db()
        .register_processing_graph(layer.processing_graph.clone())
        .instrument(debug_span!("register_processing_graph"))
        .await?; // TODO: can we get this without re-registering it?
    let computation_id = ComputationId::new();
    let query_ctx = ctx.query_context(processing_graph_id, computation_id)?;

    let query_processor = initialized_operator
        .query_processor()
        .boxed_context(error::InitializingProcessingGraph)?;

    Ok((query_processor, query_ctx, computation_id))
}
