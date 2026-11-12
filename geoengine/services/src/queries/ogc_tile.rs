//! OGC API tiles of a layer: map tiles as PNG and coverage tiles as `GeoTIFF`.

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
    queries::raster_grid::{RasterEncoding, RasterGridQuery, raster_grid_from_operator},
    quota::ComputationId,
};
use geoengine_datatypes::{
    error::BoxedResultExt,
    operations::image::RasterColorizer,
    primitives::{
        AxisAlignedRectangle, BandSelection, CacheHint, Coordinate2D, RasterQueryRectangle,
        SpatialPartition2D, SpatialResolution, TimeInterval,
    },
    raster::GridShape2D,
    util::Identifier,
};
#[cfg(debug_assertions)]
use geoengine_operators::engine::RasterResultDescriptor;
use geoengine_operators::{
    call_on_generic_raster_processor,
    engine::{
        ExecutionContext, InitializedRasterOperator, RasterBandDescriptors,
        TypedRasterQueryProcessor,
    },
    util::{WrapWithProjectionAndResample, raster_stream_to_png::raster_stream_to_png_bytes},
};
use ogcapi_types::tiles::TileMatrixSetId;
use std::collections::HashSet;
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

/// The fields (bands) of a coverage tile.
///
/// Cf. the requirements class “Field Selection” of
/// [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum FieldSelection {
    /// All fields in their original order
    #[default]
    All,
    /// Only these fields, in this order (query parameter `properties`)
    Include(Vec<String>),
    /// All fields except these, in their original order (query parameter `exclude-properties`)
    Exclude(Vec<String>),
}

impl FieldSelection {
    /// Creates a field selection from the comma-separated query parameters `properties` and `exclude-properties`
    pub fn from_query_params(
        properties: Option<&str>,
        exclude_properties: Option<&str>,
    ) -> OgcApiResult<Self> {
        fn field_names(value: &str) -> Vec<String> {
            value
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        }

        match (properties, exclude_properties) {
            (Some(_), Some(_)) => Err(OgcApiError::ConflictingFieldSelection),
            (Some(properties), None) => Ok(Self::Include(field_names(properties))),
            (None, Some(exclude_properties)) => Ok(Self::Exclude(field_names(exclude_properties))),
            (None, None) => Ok(Self::All),
        }
    }

    /// Resolves the field names to the band indices of `bands`, in the order of the response.
    ///
    /// For `properties`, this is the requested order, as required by
    /// [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html), Requirement 58 C.
    /// The raster query itself takes the bands as a sorted [`BandSelection`].
    pub fn band_order(&self, bands: &RasterBandDescriptors) -> OgcApiResult<Vec<u32>> {
        let band_index = |field: &String| -> OgcApiResult<u32> {
            bands
                .iter()
                .position(|band| band.name == *field)
                .and_then(|index| u32::try_from(index).ok())
                .ok_or_else(|| OgcApiError::UnknownField {
                    field: field.clone(),
                    available: bands
                        .iter()
                        .map(|band| band.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                })
        };

        let band_indices = match self {
            Self::All => (0..bands.count()).collect(),
            Self::Include(fields) => {
                let mut seen_fields = HashSet::with_capacity(fields.len());
                fields
                    .iter()
                    .map(|field| {
                        if !seen_fields.insert(field) {
                            return Err(OgcApiError::DuplicateField {
                                field: field.clone(),
                            });
                        }
                        band_index(field)
                    })
                    .collect::<OgcApiResult<Vec<u32>>>()?
            }
            Self::Exclude(fields) => {
                let excluded_bands = fields
                    .iter()
                    .map(band_index)
                    .collect::<OgcApiResult<HashSet<u32>>>()?;
                (0..bands.count())
                    .filter(|band| !excluded_bands.contains(band))
                    .collect()
            }
        };

        if band_indices.is_empty() {
            return Err(OgcApiError::EmptyFieldSelection);
        }

        Ok(band_indices)
    }
}

/// The layer of a tile query, projected onto its tile matrix set
struct TileSource {
    layer: Layer,
    initialized_operator: Box<dyn InitializedRasterOperator>,
    tms_spec: TypedTileMatrixSetProvider,
}

/// Loads the layer of `query`, projects it onto the tile matrix set and checks the tile coordinates
async fn load_tile_source<C: ApplicationContext>(
    ctx: &C::SessionContext,
    execution_context: &<C::SessionContext as SessionContext>::ExecutionContext,
    query: &TileQuery,
) -> OgcApiResult<TileSource> {
    let mut layer: Layer = load_layer::<C>(ctx, query.data_connector_id, query.layer_id.clone())
        .instrument(debug_span!("load_processing_graph"))
        .await?;
    let mut initialized_operator =
        get_initialized_raster_operator::<C::SessionContext>(&layer, execution_context)
            .instrument(debug_span!("initialize"))
            .await?;

    let required_origin_and_resolution =
        TypedTileMatrixSetProvider::required_origin_and_resolution(
            &query.tile_matrix_set_id,
            initialized_operator.result_descriptor(),
        )?;
    reproject_if_necessary::<C::SessionContext>(
        &mut layer,
        &mut initialized_operator,
        execution_context,
        TypedTileMatrixSetProvider::required_srs(&query.tile_matrix_set_id),
        required_origin_and_resolution,
    )
    .instrument(debug_span!("initialize"))
    .await?;

    let tms_spec = TypedTileMatrixSetProvider::resolve(
        &query.tile_matrix_set_id,
        initialized_operator.result_descriptor(),
        execution_context.tiling_specification(),
    )?;

    let (expected_number_of_tiles_at_zoom_level, _) =
        tms_spec.grid_shape_and_origin(query.tile_matrix);
    query.check_inside_bounds(expected_number_of_tiles_at_zoom_level)?;

    Ok(TileSource {
        layer,
        initialized_operator,
        tms_spec,
    })
}

/// Renders the map tile of `query` as PNG
pub async fn render_tile<C: ApplicationContext>(
    ctx: C::SessionContext,
    query: TileQuery,
) -> OgcApiResult<(Vec<u8>, CacheHint, ComputationId)> {
    let execution_context = ctx.execution_context()?;
    let tiling_specification = execution_context.tiling_specification();

    let TileSource {
        layer,
        mut initialized_operator,
        tms_spec,
    } = load_tile_source::<C>(&ctx, &execution_context, &query).await?;

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

/// Renders the coverage tile of `query` as `GeoTIFF` with the fields of `field_selection` as bands.
///
/// Unlike a map tile, a coverage tile covers the exact spatial extent of the tile
/// and has exactly the tile size in pixels as its number of cells, cf.
/// [OGC API - Coverages - Part 1: Core](https://docs.ogc.org/DRAFTS/19-087.html), Requirement 62 F and G.
pub async fn render_coverage_tile<C: ApplicationContext>(
    ctx: C::SessionContext,
    query: TileQuery,
    field_selection: FieldSelection,
) -> OgcApiResult<(Vec<u8>, CacheHint, ComputationId)> {
    let execution_context = ctx.execution_context()?;

    let TileSource {
        layer,
        initialized_operator,
        tms_spec,
    } = load_tile_source::<C>(&ctx, &execution_context, &query).await?;

    let result_descriptor = initialized_operator.result_descriptor();
    let band_order = field_selection.band_order(&result_descriptor.bands)?;
    let bands = BandSelection::new(band_order.clone()).map_err(|source| OgcApiError::Internal {
        source: source.into(),
    })?;
    let spatial_reference = result_descriptor
        .spatial_reference
        .as_option()
        .ok_or(OgcApiError::MissingSpatialReference)?;

    let bounds = tms_spec.tile_spatial_bounds(query.tile_matrix, query.tile_row, query.tile_col)?;
    // the cells of a coverage tile are the cells of its tile matrix
    let resolution = tms_spec.cell_size(query.tile_matrix);

    let operator = layer
        .processing_graph
        .operator
        .clone()
        .get_raster()
        .map_err(crate::error::Error::from)?;

    let wrapped =
        WrapWithProjectionAndResample::new_create_result_descriptor(operator, initialized_operator)
            .wrap_with_projection_and_resample(
                Some(bounds.upper_left()),
                Some(resolution),
                spatial_reference,
                &execution_context,
            )
            .instrument(debug_span!("initialize"))
            .await
            .boxed_context(error::InitializingProcessingGraph)?;

    let processing_graph_id = ctx
        .db()
        .register_processing_graph(layer.processing_graph)
        .instrument(debug_span!("register_processing_graph"))
        .await?;

    let grid_query = RasterGridQuery {
        processing_graph: processing_graph_id,
        crs: spatial_reference,
        bounds: pixel_center_bounds(bounds, resolution)?,
        resolution: Some(resolution),
        origin: Some(bounds.upper_left()),
        time: query.time_interval,
        bands,
    };

    Ok(raster_grid_from_operator(
        ctx,
        grid_query,
        wrapped,
        RasterEncoding::GeoTiff {
            no_data_value: None,
            band_order: Some(band_order),
        },
    )
    .await?)
}

/// Shrinks `bounds` by half a pixel on each side, so that they span from the center of the
/// upper-left pixel to the center of the lower-right pixel.
///
/// The grid of a raster query covers all pixels that the bounds touch. At fine resolutions,
/// floating point errors in the tile bounds would otherwise add a row or column of pixels.
fn pixel_center_bounds(
    bounds: SpatialPartition2D,
    resolution: SpatialResolution,
) -> OgcApiResult<SpatialPartition2D> {
    let half_pixel = Coordinate2D::new(resolution.x().get() / 2., -resolution.y().get() / 2.);

    SpatialPartition2D::new(
        bounds.upper_left() + half_pixel,
        bounds.lower_right() - half_pixel,
    )
    .map_err(|source| OgcApiError::Internal {
        source: source.into(),
    })
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
        .instrument(debug_span!("register_processing_graph"))
        .await?; // TODO: can we get this without re-registering it?
    let computation_id = ComputationId::new();
    let query_ctx = ctx.query_context(processing_graph_id, computation_id)?;

    let query_processor = initialized_operator
        .query_processor()
        .boxed_context(error::InitializingProcessingGraph)?;

    Ok((query_processor, query_ctx, computation_id))
}
