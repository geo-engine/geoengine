//! OGC API map tiles of a layer as PNG.

use crate::{
    api::handlers::ogc::{
        OgcApiResult, error,
        error::OgcApiError,
        tms_spec::{TileMatrixSetProvider, TypedTileMatrixSetProvider},
        util::{initialize_raster_graph, load_layer},
    },
    api::model::datatypes::{DataProviderId, LayerId},
    contexts::{ApplicationContext, SessionContext},
    layers::layer::Layer,
    processing_graphs::{processing_graph::ProcessingGraph, registry::ProcessingGraphRegistry},
    projects::Symbology,
    quota::ComputationId,
};
use geoengine_datatypes::{
    error::BoxedResultExt,
    operations::image::RasterColorizer,
    primitives::{BandSelection, CacheHint, RasterQueryRectangle, TimeInterval},
    raster::{GridBoundingBox2D, GridShape2D},
    util::Identifier,
};
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, InitializedRasterOperator, TypedOperator, TypedRasterQueryProcessor,
    },
    util::{WrapWithProjectionAndResample, raster_stream_to_png::raster_stream_to_png_bytes},
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

    let layer: Layer = load_layer::<C>(&ctx, query.data_connector_id, query.layer_id.clone())
        .instrument(debug_span!("load_processing_graph"))
        .await?;

    let tile = tile_on_grid(layer.processing_graph.clone(), &execution_context, &query)
        .instrument(debug_span!("initialize"))
        .await?;

    let query_rect = RasterQueryRectangle::new(
        tile.grid_bounds,
        query.time_interval,
        band_selection(&layer),
    );

    let (processor, query_ctx, computation_id) = create_query_processor_and_query_context(
        tile.processing_graph,
        &tile.initialized_operator,
        &ctx,
    )
    .await?;

    // cancellation is handled by the task that runs this function
    let not_canceled = Box::pin(futures::future::pending());

    let (image_bytes, cache_hint) = call_on_generic_raster_processor!(
        processor,
        p => raster_stream_to_png_bytes(
            p,
            query_rect,
            query_ctx,
            tile.width,
            tile.height,
            Some(query.time_interval),
            layer.symbology.and_then(Symbology::into_raster_symbology).map(|symbology| symbology.raster_colorizer),
            not_canceled,
        ).await
    )
    .map_err(crate::error::Error::from)?;

    Ok((image_bytes, cache_hint, computation_id))
}

/// A layer's processing graph on the grid of a tile matrix, and the pixels of one tile in that grid
pub struct TileOnGrid {
    /// the processing graph the tile is computed with: the layer's graph on the tile matrix's grid
    pub processing_graph: ProcessingGraph,
    pub initialized_operator: Box<dyn InitializedRasterOperator>,
    /// the tile's pixels in the grid of `initialized_operator`
    pub grid_bounds: GridBoundingBox2D,
    /// the size of the encoded tile
    pub width: u32,
    pub height: u32,
}

/// Puts `processing_graph` onto the grid of the tile matrix of `query` with one `Warp`, the same as
/// for a WMS `GetMap`, and finds the tile's pixels in it
///
/// # Errors
/// If the graph is no raster or cannot be initialized, or the tile is outside the tile matrix.
pub async fn tile_on_grid(
    processing_graph: ProcessingGraph,
    execution_context: &dyn ExecutionContext,
    query: &TileQuery,
) -> OgcApiResult<TileOnGrid> {
    let initialized_operator = initialize_raster_graph(&processing_graph, execution_context)
        .instrument(debug_span!("initialize_layer"))
        .await?;
    let tiling_specification = execution_context.tiling_specification();

    let layer_descriptor = TypedTileMatrixSetProvider::layer_descriptor(
        &query.tile_matrix_set_id,
        initialized_operator.result_descriptor(),
    )?;
    let tms_spec = TypedTileMatrixSetProvider::resolve(
        &query.tile_matrix_set_id,
        &layer_descriptor,
        tiling_specification,
    )?;

    let (expected_number_of_tiles_at_zoom_level, origin) =
        tms_spec.grid_shape_and_origin(query.tile_matrix);
    query.check_inside_bounds(expected_number_of_tiles_at_zoom_level)?;

    let operator = processing_graph
        .operator
        .get_raster()
        .map_err(crate::error::Error::from)?;
    let spatial_reference =
        match TypedTileMatrixSetProvider::required_srs(&query.tile_matrix_set_id) {
            Some(spatial_reference) => spatial_reference,
            None => layer_descriptor
                .spatial_reference
                .as_option()
                .ok_or(OgcApiError::MissingSpatialReference)?,
        };
    let wrapped =
        WrapWithProjectionAndResample::new_create_result_descriptor(operator, initialized_operator)
            .wrap_with_projection_and_resample(
                Some(origin),
                Some(tms_spec.spatial_resolution(query.tile_matrix)),
                spatial_reference,
                execution_context,
            )
            .instrument(debug_span!("initialize_warp"))
            .await
            .boxed_context(error::InitializingProcessingGraph)?;

    let grid_bounds = tms_spec.tile_grid_bbox(
        &wrapped
            .result_descriptor
            .tiling_grid_definition(tiling_specification),
        query.tile_matrix,
        query.tile_row,
        query.tile_col,
    )?;
    let tile_size_in_pixels = tms_spec.tile_size_in_pixels(query.tile_matrix);

    Ok(TileOnGrid {
        processing_graph: ProcessingGraph {
            operator: TypedOperator::Raster(wrapped.operator),
        },
        initialized_operator: wrapped.initialized_operator,
        grid_bounds,
        width: u32::try_from(tile_size_in_pixels.x()).unwrap_or(u32::MAX),
        height: u32::try_from(tile_size_in_pixels.y()).unwrap_or(u32::MAX),
    })
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
    processing_graph: ProcessingGraph,
    initialized_operator: &dyn InitializedRasterOperator,
    ctx: &C,
) -> OgcApiResult<(TypedRasterQueryProcessor, C::QueryContext, ComputationId)> {
    let processing_graph_id = ctx
        .db()
        .register_processing_graph(processing_graph)
        .instrument(debug_span!("register_processing_graph"))
        .await?; // TODO: can we get this without re-registering it?
    let computation_id = ComputationId::new();
    let query_ctx = ctx.query_context(processing_graph_id, computation_id)?;

    let query_processor = initialized_operator
        .query_processor()
        .boxed_context(error::InitializingProcessingGraph)?;

    Ok((query_processor, query_ctx, computation_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use geoengine_datatypes::{primitives::DateTime, util::test::TestDefault};
    use geoengine_operators::{
        engine::{MockExecutionContext, RasterOperator},
        source::{GdalSource, GdalSourceParameters},
        util::gdal::add_ndvi_dataset,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tracing_subscriber::{Layer as _, layer::SubscriberExt};

    /// Counts the debug events `Initialize GdalSource …`, one per initialization of the graph
    struct CountInitializations(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountInitializations {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Message(String);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut message = Message(String::new());
            event.record(&mut message);
            if message.0.starts_with("Initialize GdalSource") {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The layer graph is initialized once per tile, and once more only to read an overview level
    #[tokio::test]
    async fn it_initializes_the_graph_once_per_tile() {
        let mut exe_ctx = MockExecutionContext::test_default();
        let processing_graph = ProcessingGraph {
            operator: TypedOperator::Raster(
                GdalSource {
                    params: GdalSourceParameters::new(add_ndvi_dataset(&mut exe_ctx)),
                }
                .typed(),
            ),
        };

        let count = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(
            CountInitializations(count.clone())
                .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG),
        );
        let _guard = tracing::subscriber::set_default(subscriber);

        // NDVI: 0.1° in EPSG:4326, about 11 km in EPSG:3857
        for (tile_matrix_set_id, tile_matrix, tile_row, tile_col, initializations) in [
            // finer than the data: the full resolution
            (TileMatrixSetId::WebMercatorQuad, 5, 10, 16, 1),
            // coarser: an overview level
            (TileMatrixSetId::WebMercatorQuad, 2, 1, 2, 2),
            (TileMatrixSetId::Custom("Custom".to_owned()), 2, 0, 4, 1),
            (TileMatrixSetId::Custom("Custom".to_owned()), 1, 0, 2, 2),
            // its finest tile matrix is the layer reprojected with `DataBounds`, about 28 km, more
            // than twice the about 11 km the `Warp` estimates for the source: an overview level
            (
                TileMatrixSetId::Custom("CustomWebMercator".to_owned()),
                2,
                1,
                2,
                2,
            ),
        ] {
            count.store(0, Ordering::Relaxed);
            let name = format!("{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}");
            let query = TileQuery {
                data_connector_id: DataProviderId::from_u128(0),
                layer_id: LayerId("ndvi".to_owned()),
                time_interval: TimeInterval::new_instant(DateTime::new_utc(2014, 4, 1, 0, 0, 0))
                    .unwrap(),
                tile_matrix_set_id,
                tile_matrix,
                tile_row,
                tile_col,
            };

            tile_on_grid(processing_graph.clone(), &exe_ctx, &query)
                .await
                .unwrap();

            assert_eq!(count.load(Ordering::Relaxed), initializations, "{name}");
        }
    }
}
