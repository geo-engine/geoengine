//! OGC API map tiles of a layer as PNG.

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
    primitives::{BandSelection, CacheHint, RasterQueryRectangle, TimeInterval},
    raster::GridShape2D,
    util::Identifier,
};
#[cfg(debug_assertions)]
use geoengine_operators::engine::RasterResultDescriptor;
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{ExecutionContext, InitializedRasterOperator, TypedRasterQueryProcessor},
    util::raster_stream_to_png::raster_stream_to_png_bytes,
};
use ogcapi_types::tiles::TileMatrixSetId;

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

    let mut layer: Layer =
        load_layer::<C>(&ctx, query.data_connector_id, query.layer_id.clone()).await?;
    let mut initialized_operator =
        get_initialized_raster_operator::<C::SessionContext>(&layer, &execution_context).await?;
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

    let query_rect = RasterQueryRectangle::new(
        tms_spec.tile_grid_bbox(
            &initialized_operator
                .result_descriptor()
                .tiling_grid_definition(tiling_specification),
            query.tile_matrix,
            query.tile_row,
            query.tile_col,
        )?,
        query.time_interval,
        band_selection(&layer),
    );

    let (processor, query_ctx, computation_id) =
        create_query_processor_and_query_context(&layer, &initialized_operator, &ctx).await?;

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
            layer.symbology.and_then(Symbology::into_raster_symbology).map(|symbology| symbology.raster_colorizer),
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

fn band_selection(layer: &Layer) -> BandSelection {
    layer
        .symbology
        .as_ref()
        .and_then(Symbology::as_raster_symbology)
        .map(|symbology| &symbology.raster_colorizer)
        .map_or_else(
            BandSelection::first, // matches to fallback linear gradient colorizer
            RasterColorizer::band_selection,
        )
}

async fn create_query_processor_and_query_context<C: SessionContext>(
    layer: &Layer,
    initialized_operator: &dyn InitializedRasterOperator,
    ctx: &C,
) -> OgcApiResult<(TypedRasterQueryProcessor, C::QueryContext, ComputationId)> {
    let processing_graph_id = ctx
        .db()
        .register_processing_graph(layer.processing_graph.clone())
        .await?; // TODO: can we get this without re-registering it?
    let computation_id = ComputationId::new();
    let query_ctx = ctx.query_context(processing_graph_id, computation_id)?;

    let query_processor = initialized_operator
        .query_processor()
        .boxed_context(error::InitializingProcessingGraph)?;

    Ok((query_processor, query_ctx, computation_id))
}
