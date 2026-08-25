use crate::{
    engine::{
        BoxRasterQueryProcessor, CanonicOperatorName, ExecutionContext, InitializedRasterOperator,
        InitializedSources, Operator, OperatorName, QueryContext, QueryProcessor,
        RasterBandDescriptor, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
        SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
    },
    error,
    machine_learning::{
        MachineLearningError, MlModelInputNoDataHandling, MlModelLoadingInfo,
        error::{InputTypeMismatch, ModelOverlapRequired, Ort},
        onnx_util::{
            check_model_input_features, check_model_shape, check_model_shape_overlap,
            load_onnx_model_from_loading_info, model_data_shape,
        },
    },
    optimization::OptimizationError,
    util::Result,
};
use async_trait::async_trait;
use float_cmp::approx_eq;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::primitives::{
    BandSelection, Measurement, RasterQueryRectangle, SpatialResolution, TimeInterval,
};
use geoengine_datatypes::raster::{
    EmptyGrid2D, Grid2D, GridIdx2D, GridIndexAccess, GridIndexAccessMut, GridShape2D, GridSize,
    MaskedGrid, Pixel, RasterTile2D, TileInformation, TileOverlap, TileSize, UpdateIndexedElements,
};
use geoengine_datatypes::{
    machine_learning::MlModelName,
    raster::{GridBoundingBox2D, GridContains},
};
use ndarray::{Array2, Array4};
use ort::value::{IntoTensorElementType, PrimitiveTensorElementType, TensorRef};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

/// The Onnx operator applies an onnx model to a stack of raster bands and produces a single output tile from each stack.
/// *Each of the bands might be empty.*
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnnxParams {
    /// the name of the model
    pub model: MlModelName,
}

impl OnnxParams {
    pub fn new(model: MlModelName) -> Self {
        Self { model }
    }
}

/// This `QueryProcessor` applies a ml model in Onnx format on all bands of its input raster series.
/// For now, the model has to be for a single pixel and multiple bands.
pub type Onnx = Operator<OnnxParams, SingleRasterSource>;

impl OperatorName for Onnx {
    const TYPE_NAME: &'static str = "Onnx";
}

#[typetag::serde]
#[async_trait]
impl RasterOperator for Onnx {
    async fn _initialize(
        self: Box<Self>,
        path: WorkflowOperatorPath,
        context: &dyn ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        let name = CanonicOperatorName::from(&self);

        let source = self
            .sources
            .initialize_sources(path.clone(), context)
            .await?
            .raster;

        let in_descriptor = source.result_descriptor();

        let model_loading_info = context.ml_model_loading_info(&self.params.model).await?;

        // core tile size; the model's halos are derived relative to this
        let tiling_shape = context.tiling_specification().tile_size.grid_shape();
        let source_overlap = in_descriptor.spatial_grid.tile_overlap();

        // the halos implied by the model's fixed input / output pixel shapes
        let (halo_in, halo_out) = model_loading_info
            .metadata
            .input_output_overlap(&tiling_shape)?;

        // check that we can use the model input / output shape with the operator
        if halo_in.is_zero() && halo_out.is_zero() {
            // plain model: input and output pixel shapes both equal the core
            check_model_shape(&model_loading_info.metadata, tiling_shape)?;
            check_model_input_features(
                &model_loading_info.metadata,
                tiling_shape,
                in_descriptor.bands.count(),
            )?;
        } else {
            // convolution-like model: it consumes a halo, so the source must already
            // carry at least `halo_in` of overlap
            ensure!(
                source_overlap.axis_size_y() >= halo_in.axis_size_y()
                    && source_overlap.axis_size_x() >= halo_in.axis_size_x(),
                ModelOverlapRequired {
                    required: halo_in,
                    available: source_overlap,
                }
            );
            let in_data = model_data_shape(&tiling_shape, halo_in);
            check_model_shape_overlap(
                &model_loading_info.metadata,
                &tiling_shape,
                halo_in,
                halo_out,
            )?;
            check_model_input_features(
                &model_loading_info.metadata,
                in_data,
                in_descriptor.bands.count(),
            )?;
        }

        // check that input type fits model input type
        ensure!(
            model_loading_info.metadata.input_type == in_descriptor.data_type,
            InputTypeMismatch {
                model_input_type: model_loading_info.metadata.input_type,
                source_type: in_descriptor.data_type,
            }
        );

        let out_descriptor = RasterResultDescriptor {
            data_type: model_loading_info.metadata.output_type,
            spatial_reference: in_descriptor.spatial_reference,
            time: in_descriptor.time,
            // the output carries its own (possibly smaller) halo
            spatial_grid: in_descriptor.spatial_grid.with_tile_overlap(halo_out),
            bands: vec![RasterBandDescriptor::new(
                "prediction".to_string(), // TODO: parameter of the operator?
                Measurement::Unitless,    // TODO: get output measurement from model metadata
            )]
            .try_into()?,
        };

        Ok(Box::new(InitializedOnnx {
            name,
            path,
            model_name: self.params.model,
            result_descriptor: out_descriptor,
            source,
            model_loading_info,
            tile_shape: tiling_shape,
            halo_in,
            halo_out,
            source_overlap,
        }))
    }

    span_fn!(Onnx);
}

pub struct InitializedOnnx {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    model_name: MlModelName,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    model_loading_info: MlModelLoadingInfo,
    /// core tile size (the region addressed by each tile's `tile_position`)
    tile_shape: GridShape2D,
    /// halo the model consumes around the core on the input side
    halo_in: TileOverlap,
    /// halo the produced output carries around the core
    halo_out: TileOverlap,
    /// halo the source tiles actually carry (>= `halo_in`)
    source_overlap: TileOverlap,
}

impl InitializedRasterOperator for InitializedOnnx {
    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }

    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        let source = self.source.query_processor()?;

        Ok(call_on_generic_raster_processor!(
            source, input => {
                call_generic_raster_processor!(
                    self.model_loading_info.metadata.output_type,
                    OnnxProcessor::new(
                        input,
                        self.result_descriptor.clone(),
                        self.model_loading_info.clone(),
                        self.tile_shape,
                        self.halo_in,
                        self.halo_out,
                        self.source_overlap,
                    )
                    .boxed()
                )
            }
        ))
    }

    fn canonic_name(&self) -> CanonicOperatorName {
        self.name.clone()
    }

    fn name(&self) -> &'static str {
        Onnx::TYPE_NAME
    }

    fn path(&self) -> WorkflowOperatorPath {
        self.path.clone()
    }

    fn optimize(
        &self,
        target_resolution: SpatialResolution,
    ) -> Result<Box<dyn RasterOperator>, OptimizationError> {
        Ok(Onnx {
            params: OnnxParams {
                model: self.model_name.clone(),
            },
            sources: SingleRasterSource {
                raster: self.source.optimize(target_resolution)?,
            },
        }
        .boxed())
    }
}

pub(crate) struct OnnxProcessor<TIn, TOut> {
    source: BoxRasterQueryProcessor<TIn>, // as most ml algorithms work on f32 we use this as input type
    result_descriptor: RasterResultDescriptor,
    model_loading_info: MlModelLoadingInfo,
    phantom: std::marker::PhantomData<TOut>,
    /// core tile size
    tile_shape: GridShape2D,
    /// halo the model consumes around the core on the input side
    halo_in: TileOverlap,
    /// halo the produced output carries around the core
    halo_out: TileOverlap,
    /// halo the source tiles actually carry (>= `halo_in`)
    source_overlap: TileOverlap,
}

impl<TIn, TOut> OnnxProcessor<TIn, TOut> {
    pub fn new(
        source: BoxRasterQueryProcessor<TIn>,
        result_descriptor: RasterResultDescriptor,
        model_loading_info: MlModelLoadingInfo,
        tile_shape: GridShape2D,
        halo_in: TileOverlap,
        halo_out: TileOverlap,
        source_overlap: TileOverlap,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            model_loading_info,
            phantom: Default::default(),
            tile_shape,
            halo_in,
            halo_out,
            source_overlap,
        }
    }
}

#[async_trait]
impl<TIn, TOut> QueryProcessor for OnnxProcessor<TIn, TOut>
where
    TIn: Pixel + NoDataValueIn + IntoTensorElementType + PrimitiveTensorElementType,
    TOut: Pixel + NoDataValueOut + IntoTensorElementType + PrimitiveTensorElementType,
{
    type Output = RasterTile2D<TOut>;
    type SpatialBounds = GridBoundingBox2D;
    type Selection = BandSelection;
    type ResultDescription = RasterResultDescriptor;

    #[allow(clippy::too_many_lines)]
    async fn _query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<TOut>>>> {
        let num_bands = self.source.raster_result_descriptor().bands.count() as usize;

        let in_no_data_value = self
            .model_loading_info
            .metadata
            .input_no_data_handling
            .no_data_value_encoding()
            .map_or(TIn::NO_DATA_IN_FALLBACK, |v| TIn::from_(v));

        let out_no_data_value = self
            .model_loading_info
            .metadata
            .output_no_data_handling
            .no_data_value_encoding()
            .map(|v| TOut::from_(v))
            .or(TOut::NO_DATA_OUT_FALLBACK); // Int types return Some or None while float types fallback to Some(NaN)

        let source_query =
            query.select_attributes((0..num_bands as u32).collect::<Vec<u32>>().try_into()?);

        // TODO: re-use session accross queries?
        // TODO: use another method: https://github.com/pykeio/ort/issues/402#issuecomment-2949993914
        let mut session = load_onnx_model_from_loading_info(&self.model_loading_info)?;
        let input_name = session.inputs()[0].name().to_string(); // clone input name to avoid mutability problems

        let stream = self
            .source
            .raster_query(source_query, ctx)
            .await?
            .chunks(num_bands) // chunk the tiles to get all bands for a spatial index at once
            // TODO: this does not scale for large number of bands.
            //       In that case we would need to collect only a fixed number of pixel from each each,
            //       and repeat the process until the whole tile is finished
            .map(move |chunk| {
                // TODO: spawn task and await

                if chunk.len() != num_bands {
                    // if there are not exactly N tiles, it should mean the last tile was an error and the chunker ended prematurely
                    if let Some(Err(e)) = chunk.into_iter().next_back() {
                        return Err(e);
                    }
                    // if there is no error, the source did not produce all bands, which likely means a bug in an operator
                    return Err(error::Error::MustNotHappen {
                        message: "source did not produce all bands".to_string(),
                    });
                }

                // TODO: collect into a ndarray directly
                let tiles = chunk.into_iter().collect::<Result<Vec<_>>>()?;

                let first_tile = &tiles[0];
                let time = first_tile.time;
                let tile_position = first_tile.tile_position;
                let global_geo_transform = first_tile.global_geo_transform;
                let cache_hint = first_tile.cache_hint;

                let core = self.tile_shape;
                // full data extents: the core padded by the (possibly different) input / output halos
                let in_data = model_data_shape(&core, self.halo_in);
                let out_data = model_data_shape(&core, self.halo_out);
                let in_height = in_data.axis_size_y();
                let in_width = in_data.axis_size_x();
                let out_height = out_data.axis_size_y();
                let out_width = out_data.axis_size_x();
                let core_y = core.axis_size_y();
                let core_x = core.axis_size_x();
                // the source may carry a larger halo than the model consumes; crop to the model input
                let in_off_y = self.source_overlap.axis_size_y() - self.halo_in.axis_size_y();
                let in_off_x = self.source_overlap.axis_size_x() - self.halo_in.axis_size_x();

                // This determines if the tile the operator currently processes can be skipped entirely.
                // The Operator uses a "stack" of bands where each band is represented by a raster tile.
                // Each of the bands might be an empty tile. To determine if the processing should be skipped (onnx model is not called and output is an empty tile) we use the following match block:
                let skip_tile = match self.model_loading_info.metadata.input_no_data_handling {
                    // The production of the output tile is never skipped --> the onnx model is called even if all inputs are empty.
                    // The onnx model is not called if all inputs are empty. This is usefull if the onnx model can handle missing data.
                    MlModelInputNoDataHandling::EncodedNoData { no_data_value: _} => tiles.iter().all(geoengine_datatypes::raster::BaseTile::is_empty),
                    // The onnx model is not called if any band is empty. This is usefull if the onnx model can't handle missing data.
                    MlModelInputNoDataHandling::SkipIfNoData => tiles.iter().any(geoengine_datatypes::raster::BaseTile::is_empty),
                };
                tracing::debug!("skip_tile is set to {skip_tile} after evaluating input empty tiles.");

                // If the model was applied, we need to handle single pixels based on the input pixel validity (mask).
                // The validity mask is a positive mask (0 == no-data, 1 == valid data).
                // To generate the output mask, we fold over the input masks starting with an appropriate accu value which is genernated as follows:
                let output_mask_fill_value = match self.model_loading_info.metadata.input_no_data_handling {
                    MlModelInputNoDataHandling::EncodedNoData{no_data_value: _} => false,
                    MlModelInputNoDataHandling::SkipIfNoData => true
                };

                // the output validity mask lives on the output data grid; the input-premise is
                // seeded only over the shared core (the output halo region keeps the fill value)
                let mut output_mask = Grid2D::new_filled(out_data, output_mask_fill_value);
                if !(skip_tile) {
                    let halo_out_y = self.halo_out.axis_size_y();
                    let halo_out_x = self.halo_out.axis_size_x();
                    let src_off_y = self.source_overlap.axis_size_y();
                    let src_off_x = self.source_overlap.axis_size_x();
                    for c in &tiles {
                        if let Some(mg) = c.grid_array.as_masked_grid() {
                            let src_mask = mg.mask_ref();
                            // the source tile's core, in its local (core-anchored) coordinates
                            let core_bounds = c.local_core_pixel_bounds();
                            for cy in core_bounds.y_min()..=core_bounds.y_max() {
                                for cx in core_bounds.x_min()..=core_bounds.x_max() {
                                    let local = GridIdx2D::from([cy, cx]);
                                    let out_idx =
                                        local + GridIdx2D::from([halo_out_y as isize, halo_out_x as isize]);
                                    let src_idx =
                                        local + GridIdx2D::from([src_off_y as isize, src_off_x as isize]);
                                    let mask_value = src_mask.get_at_grid_index_unchecked(src_idx);
                                    let acc = output_mask.get_at_grid_index_unchecked(out_idx);
                                    let merged = match self.model_loading_info.metadata.input_no_data_handling {
                                        MlModelInputNoDataHandling::EncodedNoData { no_data_value: _ } => mask_value || acc, // if any pixel is valid this sticks to true
                                        MlModelInputNoDataHandling::SkipIfNoData => mask_value && acc, // if any pixel is invalid, this sticks to false
                                    };
                                    output_mask.set_at_grid_index_unchecked(out_idx, merged);
                                }
                            }
                        }
                    }
                }

                let skip_tile = skip_tile || output_mask.data.iter().all(|&v| !v);
                tracing::debug!("skip_tile is set to {skip_tile} after merging all input masks.");

                // the output tile carries its own halo; `TileInformation` is `Copy` so it is
                // shared between the skip path and the normal return below
                let out_tile_info = TileInformation::new_with_overlap(
                    tile_position,
                    TileSize(core),
                    global_geo_transform,
                    self.halo_out,
                );

                // if the tile is skipable or the output mask has no valid pixels:
                if skip_tile {
                    tracing::trace!("Skipping Tile {tile_position:?}");
                    return Ok(RasterTile2D::new_with_tile_info(
                        time,
                        out_tile_info,
                        0,
                        EmptyGrid2D::new(out_data).into(),
                        cache_hint,
                    ))
                }

                // TODO: use flat array instead of nested Vecs
                let mut move_axis_pixels: Vec<Vec<TIn>> = vec![vec![TIn::zero(); num_bands]; in_width * in_height];

                for (tile_index, tile) in tiles.into_iter().enumerate() {
                    // one-time per-tile bounds check, expressed in the source tile's local
                    // (core-anchored) coordinate space: the model input region (the core padded
                    // by the model's input halo) must be fully contained in the source tile's
                    // local total pixel bounds. The grid dimensions alone cannot express this
                    // because the local origin sits at the core corner while the halo lives at
                    // negative indices, so a source that does not carry enough overlap fails
                    // cleanly here instead of causing an out-of-bounds read below.
                    let halo_in_y = self.halo_in.axis_size_y() as isize;
                    let halo_in_x = self.halo_in.axis_size_x() as isize;
                    let model_input_local = GridBoundingBox2D::new_unchecked(
                        [-halo_in_y, -halo_in_x],
                        [
                            core_y as isize - 1 + halo_in_y,
                            core_x as isize - 1 + halo_in_x,
                        ],
                    );
                    let source_local_bounds = tile.local_total_pixel_bounds();
                    if !source_local_bounds.contains(&model_input_local) {
                        return Err(error::Error::MustNotHappen {
                            message: format!(
                                "source tile local pixel bounds {source_local_bounds:?} do not \
                                 contain the model input region {model_input_local:?}; the source \
                                 overlap is too small to read the model input"
                            ),
                        });
                    }
                    // TODO: use map_elements or map_elements_parallel to avoid the double loop
                    for y in 0..in_height {
                        for x in 0..in_width {
                            let pixel_index = y * in_width + x;
                            // read the model input region, cropped out of the (possibly larger) source halo
                            let pixel_value = tile
                                .get_at_grid_index_unchecked(GridIdx2D::from(
                                    [(y + in_off_y) as isize, (x + in_off_x) as isize],
                                ))
                                .unwrap_or(in_no_data_value); // TODO: properly handle missing values or skip the pixel entirely instead
                            move_axis_pixels[pixel_index][tile_index] = pixel_value;
                        }
                    }
                }

                let pixels = move_axis_pixels.into_iter().flatten().collect::<Vec<TIn>>();
                let outputs = if self.model_loading_info.metadata.input_is_single_pixel() {
                    let samples = Array2::from_shape_vec((in_width*in_height, num_bands), pixels).expect(
                        "Array2 should be valid because it is created from a Vec with the correct size",
                    );
                    session
                        .run(ort::inputs![&input_name => TensorRef::from_array_view(&samples).context(Ort)?])
                        .context(Ort)
                } else if self.model_loading_info.metadata.input_shape.yx_matches_tile_shape(&in_data){
                    let samples = Array4::from_shape_vec((1, in_height, in_width, num_bands), pixels).expect( // y,x, attributes
                        "Array4 should be valid because it is created from a Vec with the correct size",
                    );
                    session
                        .run(ort::inputs![&input_name => TensorRef::from_array_view(&samples).context(Ort)?])
                        .context(Ort)
                } else {
                    Err(
                        MachineLearningError::InvalidInputPixelShape {
                            tensor_shape: self.model_loading_info.metadata.input_shape,
                            tiling_shape: in_data
                        }
                    )
                }.map_err(error::Error::from)?;

                // assume the first output is the prediction and ignore the other outputs (e.g. probabilities for classification)
                // we don't access the output by name because it can vary, e.g. "output_label" vs "variable"
                let predictions = outputs[0].try_extract_tensor::<TOut>().context(Ort)?;

                // extract the values as a raw vector; the model returns one value per output pixel
                let (shape, out_tensor_data) = predictions.to_owned();
                debug_assert_eq!(shape.num_elements(), out_width * out_height); // TODO: use shape directly to check

                // transform the output into a grid sized to the model output extent
                let out_grid = Grid2D::new(out_data, Vec::from(out_tensor_data))?;

                // update the mask based on out no data value
                if let Some(out_no_data) = out_no_data_value { // For float types this will always be Some(NaN) while int might be None!
                    output_mask.update_indexed_elements(|idx: GridIdx2D, validity_mask| {
                        let out_value = out_grid.get_at_grid_index_unchecked(idx);
                        validity_mask && !out_no_data.is_no_data(out_value) // Impl for f32 and f64 will always set NaN as invalid
                    });
                }

                // if the validity mask has valid pixels --> return the model output + mask. Otherwise return empty tile.
                let final_grid = if output_mask.data.iter().any(|&v| v) {
                    MaskedGrid::new(out_grid, output_mask)?.into()
                } else {
                    EmptyGrid2D::new(out_data).into()
                };

                Ok(RasterTile2D::new_with_tile_info(
                    time,
                    out_tile_info,
                    0,
                    final_grid,
                    cache_hint,
                ))
            });

        Ok(stream.boxed())
    }

    fn result_descriptor(&self) -> &Self::ResultDescription {
        &self.result_descriptor
    }
}

#[async_trait]
impl<TIn, TOut> RasterQueryProcessor for OnnxProcessor<TIn, TOut>
where
    TIn: Pixel + NoDataValueIn + IntoTensorElementType + PrimitiveTensorElementType,
    TOut: Pixel + NoDataValueOut + IntoTensorElementType + PrimitiveTensorElementType,
{
    type RasterType = TOut;

    async fn _time_query<'a>(
        &'a self,
        query: TimeInterval,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<TimeInterval>>> {
        self.source.time_query(query, ctx).await
    }
}

// Trait to handle mapping masked pixels to model inputs for all datatypes.
trait NoDataValueIn {
    const NO_DATA_IN_FALLBACK: Self;
}

impl NoDataValueIn for f32 {
    const NO_DATA_IN_FALLBACK: Self = f32::NAN;
}

impl NoDataValueIn for f64 {
    const NO_DATA_IN_FALLBACK: Self = f64::NAN;
}

// Define a macro to implement NoDataValue for various types with NO_DATA as 0
macro_rules! impl_no_data_value_zero {
    ($($t:ty),*) => {
        $(
            impl NoDataValueIn for $t {
                const NO_DATA_IN_FALLBACK: Self = 0;
            }
        )*
    };
}

// Use the macro to implement NoDataValue for i8, u8, i16, u16, etc.
impl_no_data_value_zero!(i8, u8, i16, u16, i32, u32, i64, u64);

// Trait to handle mapping model outputs to masked pixels for all datatypes.
trait NoDataValueOut
where
    Self: Sized + PartialEq,
{
    const NO_DATA_OUT_FALLBACK: Option<Self>;

    fn is_no_data(&self, value: Self) -> bool {
        *self == value
    }
}

impl NoDataValueOut for f32 {
    const NO_DATA_OUT_FALLBACK: Option<Self> = Some(f32::NAN);

    fn is_no_data(&self, value: Self) -> bool {
        f32::is_nan(value) || approx_eq!(f32, *self, value)
    }
}

impl NoDataValueOut for f64 {
    const NO_DATA_OUT_FALLBACK: Option<Self> = Some(f64::NAN);

    fn is_no_data(&self, value: Self) -> bool {
        f64::is_nan(value) || approx_eq!(f64, *self, value)
    }
}

// Define a macro to implement NoDataValue for various types with NO_DATA as 0
macro_rules! impl_no_data_value_none {
    ($($t:ty),*) => {
        $(
            impl NoDataValueOut for $t {
                const NO_DATA_OUT_FALLBACK: Option<Self> = None;
            }
        )*
    };
}

// Use the macro to implement NoDataValue for i8, u8, i16, u16, etc.
impl_no_data_value_none!(i8, u8, i16, u16, i32, u32, i64, u64);

#[cfg(test)]
mod tests {

    use crate::engine::TimeDescriptor;
    use crate::machine_learning::MlModelInputNoDataHandling;
    use crate::machine_learning::MlModelLoadingInfo;
    use crate::machine_learning::MlModelMetadata;
    use crate::machine_learning::MlModelOutputNoDataHandling;
    use crate::machine_learning::onnx::Onnx;
    use crate::machine_learning::onnx::OnnxParams;
    use crate::{
        engine::{
            MockExecutionContext, MultipleRasterSources, RasterBandDescriptors, RasterOperator,
            RasterResultDescriptor, SingleRasterSource, SpatialGridDescriptor,
            WorkflowOperatorPath,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{AddTileOverlap, AddTileOverlapParams, RasterStacker, RasterStackerParams},
        util::Result,
    };
    use approx::assert_abs_diff_eq;
    use futures::StreamExt;
    use geoengine_datatypes::primitives::TimeStep;
    use geoengine_datatypes::raster::{
        GridBoundingBox2D, RasterTile2D, SpatialGridDefinition, TileIdx, TileOverlap,
        TilesEqualIgnoringCacheHint,
    };
    use geoengine_datatypes::{
        machine_learning::{MlModelName, MlTensorShape3D},
        primitives::{CacheHint, RasterQueryRectangle, TimeInterval},
        raster::{Grid, GridOrEmpty, RasterDataType, RenameBands, TileSize},
        spatial_reference::SpatialReference,
        test_data,
        util::test::TestDefault,
    };
    use ndarray::{Array1, Array2, arr2, array};
    use ort::value::TensorRef;

    #[test]
    fn ort() {
        let mut session = ort::session::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("ml/onnx/test_classification.onnx"))
            .unwrap();

        let input_name = &session.inputs()[0].name().to_string();

        let new_samples = arr2(&[[0.1f32, 0.2], [0.2, 0.3], [0.2, 0.2], [0.3, 0.1]]);

        let outputs = session
            .run(ort::inputs![input_name => TensorRef::from_array_view(&new_samples).unwrap()])
            .unwrap();

        let (_shape, data) = outputs["output_label"]
            .try_extract_tensor::<i64>()
            .unwrap()
            .to_owned();

        assert_eq!(data, &[33i64, 33, 42, 42]);
    }

    #[test]
    fn ort_dynamic() {
        let mut session = ort::session::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("ml/onnx/test_classification.onnx"))
            .unwrap();

        let input_name = &session.inputs()[0].name().to_string();

        let pixels = vec![
            vec![0.1f32, 0.2],
            vec![0.2, 0.3],
            vec![0.2, 0.2],
            vec![0.3, 0.1],
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<f32>>();

        let rows = 4;
        let cols = 2;

        let new_samples = Array2::from_shape_vec((rows, cols), pixels).unwrap();

        let outputs = session
            .run(ort::inputs![input_name =>  TensorRef::from_array_view(&new_samples).unwrap()])
            .unwrap();

        let (_shape, data) = outputs["output_label"]
            .try_extract_tensor::<i64>()
            .unwrap()
            .to_owned();

        assert_eq!(data, &[33i64, 33, 42, 42]);
    }

    #[test]
    fn regression() {
        let mut session = ort::session::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("ml/onnx/test_regression.onnx"))
            .unwrap();

        let input_name = &session.inputs()[0].name().to_string();

        let pixels = vec![
            vec![0.1f32, 0.1, 0.2],
            vec![0.1, 0.2, 0.2],
            vec![0.2, 0.2, 0.2],
            vec![0.2, 0.2, 0.1],
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<f32>>();

        let rows = 4;
        let cols = 3;

        let new_samples = Array2::from_shape_vec((rows, cols), pixels).unwrap();

        let outputs = session
            .run(ort::inputs![input_name =>  TensorRef::from_array_view(&new_samples).unwrap()])
            .unwrap();

        let predictions: Array1<f32> = outputs["variable"]
            .try_extract_array::<f32>()
            .unwrap()
            .to_owned()
            .to_shape((4,))
            .unwrap()
            .to_owned();

        assert!(predictions.abs_diff_eq(&array![0.4f32, 0.5, 0.6, 0.5], 1e-6));
    }

    // TOODO: add test using neural network model

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_classifies_tiles() {
        let data: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.1, 0.2, 0.2])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.2f32, 0.2, 0.1, 0.1])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let data2: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.2f32, 0.2, 0.1, 0.1])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.1, 0.2, 0.2])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        None,
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
                        TileSize::new_y_x(256, 256),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        None,
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
                        TileSize::new_y_x(256, 256),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                output_origin: None,
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        // load a very simple model that checks whether the first band is greater than the second band
        let model_name = MlModelName {
            namespace: None,
            name: "test_classification".into(),
        };

        let onnx = Onnx {
            params: OnnxParams::new(model_name.clone()),
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let ml_model_loading_info = MlModelLoadingInfo {
            storage_path: test_data!("ml/onnx/test_classification.onnx").to_owned(),
            metadata: MlModelMetadata {
                input_type: RasterDataType::F32,
                input_shape: MlTensorShape3D::new_single_pixel_bands(2),
                output_shape: MlTensorShape3D::new_single_pixel_single_band(),
                output_type: RasterDataType::I64,
                input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
                output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
            },
        };

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new_y_x(2, 2);
        exe_ctx.ml_models.insert(model_name, ml_model_loading_info);

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            [0].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = onnx
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_i64().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<RasterTile2D<i64>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![33i64, 33, 42, 42])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![42i64, 42, 33, 33])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_regresses_tiles() {
        let data: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.2, 0.3, 0.4])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.5f32, 0.6, 0.7, 0.8])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let data2: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.9f32, 0.8, 0.7, 0.6])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.5f32, 0.4, 0.3, 0.22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let data3: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.2, 0.3, 0.4])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.5f32, 0.6, 0.7, 0.8])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        None,
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
                        TileSize::new_y_x(256, 256),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        None,
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
                        TileSize::new_y_x(256, 256),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let mrs3 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data3.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        None,
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
                        TileSize::new_y_x(256, 256),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                output_origin: None,
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2, mrs3],
            },
        }
        .boxed();

        // load a very simple model that performs regression to predict the sum of the three bands
        let model_name = MlModelName {
            namespace: None,
            name: "test_regression".into(),
        };

        let onnx = Onnx {
            params: OnnxParams::new(model_name.clone()),
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let ml_model_loading_info = MlModelLoadingInfo {
            storage_path: test_data!("ml/onnx/test_regression.onnx").to_owned(),
            metadata: MlModelMetadata {
                input_type: RasterDataType::F32,
                input_shape: MlTensorShape3D::new_single_pixel_bands(3),
                output_shape: MlTensorShape3D::new_single_pixel_single_band(),
                output_type: RasterDataType::F32,
                input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
                output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
            },
        };

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new_y_x(2, 2);
        exe_ctx.ml_models.insert(model_name, ml_model_loading_info);

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-2, -1, 0, 3).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            [0].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = onnx
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_f32().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(result.len(), 2);

        let expected = vec![vec![1.1f32, 1.2, 1.3, 1.4], vec![1.5f32, 1.6, 1.7, 1.8]];

        for (tile, expected) in result.iter().zip(expected) {
            let GridOrEmpty::Grid(result_array) = &tile.grid_array else {
                panic!("no result array")
            };

            assert_abs_diff_eq!(
                result_array.inner_grid.data.as_slice(),
                expected.as_slice(),
                epsilon = 0.1
            );
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_adds_tiles_3d() {
        let data: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![0.1f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![1.0f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let data2: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![0.2f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![2.0f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::F32,
            spatial_reference: SpatialReference::epsg_4326().into(),
            spatial_grid: SpatialGridDescriptor::new_source(
                SpatialGridDefinition::new(
                    TestDefault::test_default(),
                    GridBoundingBox2D::new_min_max(-512, -1, 0, 1023).unwrap(),
                ),
                TileSize::new_y_x(256, 256),
            ),
            time: TimeDescriptor::new_regular_with_epoch(None, TimeStep::millis(5).unwrap()),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: result_descriptor.clone(),
            },
        }
        .boxed();

        let mrs2 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data2.clone(),
                result_descriptor,
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
                output_origin: None,
                rename_bands: RenameBands::Default,
            },
            sources: MultipleRasterSources {
                rasters: vec![mrs1, mrs2],
            },
        }
        .boxed();

        // load a very simple model that checks whether the first band is greater than the second band
        let model_name = MlModelName {
            namespace: None,
            name: "test_a_plus_b".into(),
        };

        let onnx = Onnx {
            params: OnnxParams::new(model_name.clone()),
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let ml_model_loading_info = MlModelLoadingInfo {
            storage_path: test_data!("ml/onnx/test_a_plus_b.onnx").to_owned(),
            metadata: MlModelMetadata {
                input_type: RasterDataType::F32,
                input_shape: MlTensorShape3D::new_y_x_bands(512, 512, 2),
                output_shape: MlTensorShape3D::new_y_x_bands(512, 512, 1),
                output_type: RasterDataType::F32,
                input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
                output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
            },
        };

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new_y_x(512, 512);
        exe_ctx.ml_models.insert(model_name, ml_model_loading_info);

        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(-512, -1, 0, 1023).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            [0].try_into().unwrap(),
        );

        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = onnx
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();

        let qp = op.query_processor().unwrap().get_f32().unwrap();

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        let expected: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 0),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![0.3f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: TileIdx::new_y_x(-1, 1),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![3.0f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
                overlap: TileOverlap::zero(),
            },
        ];

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }

    // A "convolution-like" model: input 4x4 (a 2x2 core padded by a 1px halo) -> output 2x2.
    fn ml_info(in_yx: (u32, u32), out_yx: (u32, u32)) -> MlModelLoadingInfo {
        MlModelLoadingInfo {
            storage_path: test_data!("ml/onnx/test_conv_overlap.onnx").to_owned(),
            metadata: MlModelMetadata {
                input_type: RasterDataType::F32,
                output_type: RasterDataType::F32,
                input_shape: MlTensorShape3D::new_y_x_bands(in_yx.0, in_yx.1, 1),
                output_shape: MlTensorShape3D::new_y_x_bands(out_yx.0, out_yx.1, 1),
                input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
                output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
            },
        }
    }

    // A source whose descriptor carries the given tile overlap (no data tiles).
    fn overlap_source(overlap: TileOverlap) -> Box<dyn RasterOperator> {
        MockRasterSource {
            params: MockRasterSourceParams {
                data: Vec::<RasterTile2D<f32>>::new(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        None,
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(0, 0, 2, 2).unwrap(),
                        TileSize::new(2, 2),
                    )
                    .with_tile_overlap(overlap),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed()
    }

    // A conv model needs a 1px input halo; a source with *less* overlap must be rejected.
    #[tokio::test]
    async fn it_rejects_insufficient_overlap() {
        let model_name = MlModelName {
            namespace: None,
            name: "conv".into(),
        };
        let onnx = Onnx {
            params: OnnxParams::new(model_name.clone()),
            sources: SingleRasterSource {
                raster: overlap_source(TileOverlap::zero()),
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new_y_x(2, 2);
        exe_ctx
            .ml_models
            .insert(model_name, ml_info((4, 4), (2, 2)));

        let err = match onnx
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
        {
            Ok(_) => panic!("expected initialization to fail"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("requires input tile overlap"),
            "expected ModelOverlapRequired, got: {msg}"
        );
    }

    // A source with *more* overlap than the model needs is fine: it initialises,
    // and the extra halo is cropped away during the query.
    #[tokio::test]
    async fn it_accepts_excess_overlap() {
        let model_name = MlModelName {
            namespace: None,
            name: "conv".into(),
        };
        let onnx = Onnx {
            params: OnnxParams::new(model_name.clone()),
            sources: SingleRasterSource {
                raster: overlap_source(TileOverlap::new(2, 2)),
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new_y_x(2, 2);
        exe_ctx
            .ml_models
            .insert(model_name, ml_info((4, 4), (2, 2)));

        // must not error: 2px available >= 1px required
        onnx.initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .expect("excess overlap must be accepted");
    }

    // The halo must be symmetric: an odd (in - core) difference per axis is rejected.
    #[tokio::test]
    async fn it_rejects_asymmetric_overlap() {
        let model_name = MlModelName {
            namespace: None,
            name: "conv".into(),
        };
        let onnx = Onnx {
            params: OnnxParams::new(model_name.clone()),
            sources: SingleRasterSource {
                raster: overlap_source(TileOverlap::zero()),
            },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new_y_x(2, 2);
        // 5x5 input on a 2x2 core => an odd 3px difference => asymmetric
        exe_ctx
            .ml_models
            .insert(model_name, ml_info((5, 5), (2, 2)));

        let err = match onnx
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
        {
            Ok(_) => panic!("expected initialization to fail"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("overlap is asymmetric"),
            "expected AsymmetricModelOverlap, got: {msg}"
        );
    }

    // End-to-end: a plain source is padded with a 1px halo by `AddTileOverlap`, and the
    // conv model reads that halo. The center tile's output rows depend on neighbor-tile
    // pixels (values 1..9 laid out around the center's 5s), so a no-overlap implementation
    // could not produce the expected values.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_conv_applies_overlap() {
        // 3x3 grid of 2x2 core tiles; tile (row, col) is constant 3*row + col + 1.
        let mut data: Vec<RasterTile2D<f32>> = Vec::new();
        for row in 0..3 {
            for col in 0..3 {
                let value = (3 * row + col + 1) as f32;
                data.push(RasterTile2D {
                    time: TimeInterval::new_unchecked(0, 5),
                    tile_position: TileIdx::new_y_x(row as isize, col as isize),
                    band: 0,
                    global_geo_transform: TestDefault::test_default(),
                    grid_array: Grid::new([2, 2].into(), vec![value; 4]).unwrap().into(),
                    properties: Default::default(),
                    cache_hint: CacheHint::default(),
                    overlap: TileOverlap::zero(),
                });
            }
        }

        // the plain source (no overlap): 2x2 tiles over a 3x3 grid of tiles (6x6 pixels)
        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data,
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: TimeDescriptor::new_regular_with_epoch(
                        None,
                        TimeStep::millis(5).unwrap(),
                    ),
                    spatial_grid: SpatialGridDescriptor::source_from_parts(
                        TestDefault::test_default(),
                        GridBoundingBox2D::new_min_max(0, 5, 0, 5).unwrap(),
                        TileSize::new(2, 2),
                    ),
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        // pad every core tile with a 1px halo, blitted from its neighbours
        let with_halo = AddTileOverlap {
            params: AddTileOverlapParams {
                overlap: TileOverlap::new(1, 1),
            },
            sources: SingleRasterSource { raster: mrs },
        }
        .boxed();

        let model_name = MlModelName {
            namespace: None,
            name: "conv".into(),
        };
        let onnx = Onnx {
            params: OnnxParams::new(model_name.clone()),
            sources: SingleRasterSource { raster: with_halo },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size = TileSize::new_y_x(2, 2);
        exe_ctx
            .ml_models
            .insert(model_name, ml_info((4, 4), (2, 2)));
        let query_ctx = exe_ctx.mock_query_context_test_default();

        let op = onnx
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .unwrap();
        let qp = op.query_processor().unwrap().get_f32().unwrap();

        // query the full 3x3 grid so the center tile's halo is fully populated
        let query_rect = RasterQueryRectangle::new(
            GridBoundingBox2D::new_min_max(0, 5, 0, 5).unwrap(),
            TimeInterval::new_unchecked(0, 5),
            [0].try_into().unwrap(),
        );

        let result = qp
            .raster_query(query_rect, &query_ctx)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        let result = result.into_iter().collect::<Result<Vec<_>>>().unwrap();

        // The center tile's 4x4 model input (halo blitted from neighbours 1..9 around
        // the 5s core) is:
        //   [1, 2, 2, 3]  -> row sum 8
        //   [4, 5, 5, 6]  -> row sum 20
        //   [4, 5, 5, 6]  -> row sum 20
        //   [7, 8, 8, 9]  -> row sum 32
        // out = row-major (2x2) of the four row sums = [[8, 20], [20, 32]].
        // The halo rows (0 and 3) come from *neighbour* tiles, so a no-overlap
        // implementation (which would only ever see the 5s core) cannot produce this.
        let target_position: [isize; 2] = [1, 1];
        let center = result
            .iter()
            .find(|t| t.tile_position == target_position.into())
            .expect("center tile must be present");
        let mg = center
            .grid_array
            .as_masked_grid()
            .expect("center tile must be a valid (non-empty) grid");
        assert_eq!(mg.inner_grid.data, vec![8., 20., 20., 32.]);
        assert_eq!(mg.validity_mask.data, vec![true; 4]);
    }
}
