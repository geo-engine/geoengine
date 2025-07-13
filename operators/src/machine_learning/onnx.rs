use crate::engine::{
    CanonicOperatorName, ExecutionContext, InitializedRasterOperator, InitializedSources, Operator,
    OperatorName, QueryContext, RasterBandDescriptor, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, SingleRasterSource, TypedRasterQueryProcessor, WorkflowOperatorPath,
};
use crate::error;
use crate::machine_learning::MachineLearningError;
use crate::machine_learning::error::{InputTypeMismatch, Ort};
use crate::machine_learning::onnx_util::{check_model_input_features, check_model_shape};
use crate::util::Result;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use geoengine_datatypes::machine_learning::{
    MlModelMetadata, MlModelName,  SkipOnNoData,
};
use geoengine_datatypes::primitives::{Measurement, RasterQueryRectangle};
use geoengine_datatypes::raster::{
    EmptyGrid2D, Grid2D, GridIdx2D, GridIndexAccess, GridShape2D, GridShapeAccess, GridSize, MaskedGrid, Pixel, RasterTile2D, UpdateIndexedElements
};
use ndarray::{Array2, Array4};
use ort::{
    tensor::{IntoTensorElementType, PrimitiveTensorElementType},
    value::TensorRef,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use super::onnx_util::load_onnx_model_from_metadata;

/// The Onnx operator applies an onnx model to a stack of raster bands and produces a single output tile from each stack.
/// *Each of the bands might be empty.*
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnnxParams {
    /// the name of the model
    pub model: MlModelName,
    /// To determine output validity and the possibility to skip model application, the following strategies are available:
    /// - Never: The onnx model is always called and output pixels are always valid. Even if all input bands are empty.
    /// - IfAllInputsAreEmpty: If all inputs are empty (no-data), the output is also empty (no-data). This is usefull if the model can handle missing data.
    /// - IfAnyInputIsEmpty: If any input model is empty (no-data), the output is also empty (no-data). This is usefull if the model can't handle missing data.
    #[serde(default)]
    pub skip_mode: SkipOnNoData,
}

impl OnnxParams {
    pub fn new_with_defaults(model: MlModelName) -> Self {
        Self {
            model,
            skip_mode: SkipOnNoData::default(),
        }
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

        let model_metadata = context.ml_model_metadata(&self.params.model).await?;

        let tiling_shape = context.tiling_specification().tile_size_in_pixels;

        // check that we can use the model input / output shape with the operator
        check_model_shape(&model_metadata, tiling_shape)?;
        check_model_input_features(&model_metadata, tiling_shape, in_descriptor.bands.count())?;

        // check that input type fits model input type
        ensure!(
            model_metadata.input_type == in_descriptor.data_type,
            InputTypeMismatch {
                model_input_type: model_metadata.input_type,
                source_type: in_descriptor.data_type,
            }
        );

        let out_descriptor = RasterResultDescriptor {
            data_type: model_metadata.output_type,
            spatial_reference: in_descriptor.spatial_reference,
            time: in_descriptor.time,
            bbox: in_descriptor.bbox,
            resolution: in_descriptor.resolution,
            bands: vec![RasterBandDescriptor::new(
                "prediction".to_string(), // TODO: parameter of the operator?
                Measurement::Unitless,    // TODO: get output measurement from model metadata
            )]
            .try_into()?,
        };

        Ok(Box::new(InitializedOnnx {
            name,
            path,
            result_descriptor: out_descriptor,
            source,
            model_metadata,
            tile_shape: context.tiling_specification().grid_shape(),
            skip_mode: self.params.skip_mode,
        }))
    }

    span_fn!(Onnx);
}

pub struct InitializedOnnx {
    name: CanonicOperatorName,
    path: WorkflowOperatorPath,
    result_descriptor: RasterResultDescriptor,
    source: Box<dyn InitializedRasterOperator>,
    model_metadata: MlModelMetadata,
    tile_shape: GridShape2D,
    skip_mode: SkipOnNoData,
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
                    self.model_metadata.output_type,
                    OnnxProcessor::new(
                        input,
                        self.result_descriptor.clone(),
                        self.model_metadata.clone(),
                        self.tile_shape,
                        self.skip_mode,
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
}

pub(crate) struct OnnxProcessor<TIn, TOut> {
    source: Box<dyn RasterQueryProcessor<RasterType = TIn>>, // as most ml algorithms work on f32 we use this as input type
    result_descriptor: RasterResultDescriptor,
    model_metadata: MlModelMetadata,
    phantom: std::marker::PhantomData<TOut>,
    tile_shape: GridShape2D,
    skip_mode: SkipOnNoData,
}

impl<TIn, TOut> OnnxProcessor<TIn, TOut> {
    pub fn new(
        source: Box<dyn RasterQueryProcessor<RasterType = TIn>>,
        result_descriptor: RasterResultDescriptor,
        model_metadata: MlModelMetadata,
        tile_shape: GridShape2D,
        skip_mode: SkipOnNoData,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            model_metadata,
            phantom: Default::default(),
            tile_shape,
            skip_mode,
        }
    }
}

#[async_trait]
impl<TIn, TOut> RasterQueryProcessor for OnnxProcessor<TIn, TOut>
where
    TIn: Pixel + NoDataValueInFallback + IntoTensorElementType + PrimitiveTensorElementType,
    TOut: Pixel + NoDataValueOutFallback + IntoTensorElementType + PrimitiveTensorElementType,
{
    type RasterType = TOut;

    #[allow(clippy::too_many_lines)]
    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<TOut>>>> {
        let num_bands = self.source.raster_result_descriptor().bands.count() as usize;

        let skip_mode = self.skip_mode;
        let in_no_data_value = self.model_metadata.in_no_data_code.map(|v| TIn::from_(v)).unwrap_or(TIn::NO_DATA_IN_FALLBACK);
        // TODO: discuss how we handle output nodata... For float a default of NaN works but using 0 for int is a problem!
        let out_no_data_value = self.model_metadata.in_no_data_code.map(|v| TOut::from_(v)).or(TOut::NO_DATA_OUT_FALLBACK); // While float types fallback to NaN, the check (later) always tests for NaN...

        let mut source_query = query.clone();
        source_query.attributes = (0..num_bands as u32).collect::<Vec<u32>>().try_into()?;

        // TODO: re-use session accross queries?
        // TODO: use another method: https://github.com/pykeio/ort/issues/402#issuecomment-2949993914
        let mut session = load_onnx_model_from_metadata(&self.model_metadata)?;
        let input_name = session.inputs[0].name.clone(); // clone input name to avoid mutability problems

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

                let tile_shape = self.tile_shape;
                let width = tile_shape.axis_size_x();
                let height = tile_shape.axis_size_y();

                // This determines if the tile the operator currently processes can be skipped entirely.
                // The Operator uses a "stack" of bands where each band is represented by a raster tile.
                // Each of the bands might be an empty tile. To determine if the processing should be skipped (onnx model is not called and output is an empty tile) we use the following match block:
                let skip_tile = match skip_mode {
                    // The production of the output tile is never skipped --> the onnx model is called even if all inputs are empty.
                    SkipOnNoData::Never => false,
                    // The onnx model is not called if all inputs are empty. This is usefull if the onnx model can handle missing data.
                    SkipOnNoData::IfAllInputsAreNoData => tiles.iter().all(geoengine_datatypes::raster::BaseTile::is_empty),
                    // The onnx model is not called if any band is empty. This is usefull if the onnx model can't handle missing data.
                    SkipOnNoData::IfAnyInputIsNoData => tiles.iter().any(geoengine_datatypes::raster::BaseTile::is_empty),
                };

                // If the model was applied, we need to handle single pixels based on the input pixel validity (mask).
                // The validity mask is a positive mask (0 == no-data, 1 == valid data).
                // To generate the output mask, we fold over the input masks starting with an appropriate accu value which is genernated as follows:
                let output_mask_fill_value = match skip_mode {
                    SkipOnNoData::IfAnyInputIsNoData => false, 
                    SkipOnNoData::IfAllInputsAreNoData | SkipOnNoData::Never => true
                };

                let mut output_mask = Grid2D::new_filled(self.tile_shape, output_mask_fill_value);
                if !(skip_tile || skip_mode == SkipOnNoData::Never) {
                    tracing::trace!("Merging masks with {skip_mode:?}");
                    for c in &tiles {
                        if let Some(mg) = c.grid_array.as_masked_grid() {
                            output_mask.update_indexed_elements(|idx: GridIdx2D, value| {
                                let mask_value = mg.mask_ref().get_at_grid_index_unchecked(idx);
                                match skip_mode {
                                    SkipOnNoData::IfAnyInputIsNoData => mask_value || value, // if any pixel is valid this sticks to true
                                    SkipOnNoData::IfAllInputsAreNoData => mask_value && value, // if any pixel is invalid, this sticks to false
                                    SkipOnNoData::Never => true,
                                }
                            });
                        }
                    }
                }

                // if the tile is skipable or the output mask has no valid pixels:
                if skip_tile || output_mask.data.iter().all(|&v| v == false) {
                    tracing::trace!("Skipping Tile {tile_position:?}");
                    return Ok(RasterTile2D::new(
                        time,
                        tile_position,
                        0,
                        global_geo_transform,
                        EmptyGrid2D::new(tile_shape).into(),
                        cache_hint,
                    ))
                }

                // TODO: use flat array instead of nested Vecs
                let mut move_axis_pixels: Vec<Vec<TIn>> = vec![vec![TIn::zero(); num_bands]; width * height];

                for (tile_index, tile) in tiles.into_iter().enumerate() {
                    // TODO: use map_elements or map_elements_parallel to avoid the double loop
                    for y in 0..height {
                        for x in 0..width {
                            let pixel_index = y * width + x;
                            let pixel_value = tile
                                .get_at_grid_index(GridIdx2D::from([y as isize, x as isize]))?
                                .unwrap_or(in_no_data_value); // TODO: properly handle missing values or skip the pixel entirely instead
                            move_axis_pixels[pixel_index][tile_index] = pixel_value;
                        }
                    }
                }

                let pixels = move_axis_pixels.into_iter().flatten().collect::<Vec<TIn>>();
                let outputs = if self.model_metadata.input_is_single_pixel() {
                    let samples = Array2::from_shape_vec((width*height, num_bands), pixels).expect(
                        "Array2 should be valid because it is created from a Vec with the correct size",
                    );
                    session
                        .run(ort::inputs![&input_name => TensorRef::from_array_view(&samples).context(Ort)?])
                        .context(Ort)
                } else if self.model_metadata.input_shape.yx_matches_tile_shape(&tile_shape){
                    let samples = Array4::from_shape_vec((1, height, width, num_bands), pixels).expect( // y,x, attributes
                        "Array4 should be valid because it is created from a Vec with the correct size",
                    );
                    session
                        .run(ort::inputs![&input_name => TensorRef::from_array_view(&samples).context(Ort)?])
                        .context(Ort)
                } else {
                    Err(
                        MachineLearningError::InvalidInputPixelShape {
                            tensor_shape: self.model_metadata.input_shape,
                            tiling_shape: tile_shape
                        }
                    )
                }.map_err(error::Error::from)?;

                // assume the first output is the prediction and ignore the other outputs (e.g. probabilities for classification)
                // we don't access the output by name because it can vary, e.g. "output_label" vs "variable"
                let predictions = outputs[0].try_extract_tensor::<TOut>().context(Ort)?;

                // extract the values as a raw vector because we expect one prediction per pixel.
                // this works for 1d tensors as well as 2d tensors with a single column
                let (shape, out_tensor_data) = predictions.to_owned();
                debug_assert!(shape.num_elements() == width*height); // TODO: use shape directly to check

                // transform the output intp a grid
                let out_grid = Grid2D::new([width, height].into(), Vec::from(out_tensor_data))?;

                // update the mask based on out no data value
                if let Some(out_no_data) = out_no_data_value { // The fallback for float types will cause this to always be Some!
                    output_mask.update_indexed_elements(|idx: GridIdx2D, mask_value| {
                        let out_value = out_grid.get_at_grid_index_unchecked(idx);
                        mask_value && !out_no_data.is_no_data(out_value) // Impl for F32 and F64 will always set NaN as masked
                    });
                }
                
                // if the mask has no valid pixels --> return an empty grid
                let final_grid = if output_mask.data.iter().all(|&v| v == false) {
                    EmptyGrid2D::new(out_grid.shape).into()
                } else {
                    MaskedGrid::new(out_grid, output_mask)?.into()
                };
                
                Ok(RasterTile2D::new(
                    time,
                    tile_position,
                    0,
                    global_geo_transform,
                    final_grid,
                    cache_hint,
                ))
            });

        Ok(stream.boxed())
    }

    fn raster_result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

// workaround trait to handle missing values for all datatypes.
// TODO: this should be handled differently, like skipping the pixel entirely or using a different value for missing values
trait NoDataValueInFallback {
    const NO_DATA_IN_FALLBACK: Self;
}

impl NoDataValueInFallback for f32 {
    const NO_DATA_IN_FALLBACK: Self = f32::NAN;
}

impl NoDataValueInFallback for f64 {
    const NO_DATA_IN_FALLBACK: Self = f64::NAN;
}

// Define a macro to implement NoDataValue for various types with NO_DATA as 0
macro_rules! impl_no_data_value_zero {
    ($($t:ty),*) => {
        $(
            impl NoDataValueInFallback for $t {
                const NO_DATA_IN_FALLBACK: Self = 0;
            }
        )*
    };
}

// Use the macro to implement NoDataValue for i8, u8, i16, u16, etc.
impl_no_data_value_zero!(i8, u8, i16, u16, i32, u32, i64, u64);

// workaround trait to handle missing values for all datatypes.
// TODO: this should be handled differently, like skipping the pixel entirely or using a different value for missing values
trait NoDataValueOutFallback where Self: Sized + PartialEq {
    const NO_DATA_OUT_FALLBACK: Option<Self>;

    fn is_no_data(&self, value: Self) -> bool {
        value == *self
    }
}

impl NoDataValueOutFallback for f32 {
    const NO_DATA_OUT_FALLBACK: Option<Self> = Some(f32::NAN);

    fn is_no_data(&self, value: Self) -> bool {
        value == *self || value != value
    }
}

impl NoDataValueOutFallback for f64 {
    const NO_DATA_OUT_FALLBACK: Option<Self> = Some(f64::NAN);

    fn is_no_data(&self, value: Self) -> bool {
        value == *self || value != value
    }
}

// Define a macro to implement NoDataValue for various types with NO_DATA as 0
macro_rules! impl_no_data_value_none {
    ($($t:ty),*) => {
        $(
            impl NoDataValueOutFallback for $t {
                const NO_DATA_OUT_FALLBACK: Option<Self> = None;
            }
        )*
    };
}

// Use the macro to implement NoDataValue for i8, u8, i16, u16, etc.
impl_no_data_value_none!(i8, u8, i16, u16, i32, u32, i64, u64);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{
            MockExecutionContext, MockQueryContext, MultipleRasterSources, RasterBandDescriptors,
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{RasterStacker, RasterStackerParams},
    };
    use approx::assert_abs_diff_eq;
    use geoengine_datatypes::{
        machine_learning::MlTensorShape3D,
        primitives::{CacheHint, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{
            Grid, GridOrEmpty, GridShape, RasterDataType, RenameBands, TilesEqualIgnoringCacheHint
        },
        spatial_reference::SpatialReference,
        test_data,
        util::test::TestDefault,
    };
    use ndarray::{Array1, Array2, arr2, array};

    #[test]
    fn ort() {
        let mut session = ort::session::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("ml/onnx/test_classification.onnx"))
            .unwrap();

        let input_name = &session.inputs[0].name.clone();

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

        let input_name = &session.inputs[0].name.clone();

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

        let input_name = &session.inputs[0].name.clone();

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
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.1, 0.2, 0.2])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.2f32, 0.2, 0.1, 0.1])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.2f32, 0.2, 0.1, 0.1])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.1, 0.2, 0.2])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
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
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
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
            params: OnnxParams::new_with_defaults(model_name.clone()),
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let ml_model_metadata = MlModelMetadata {
            file_path: test_data!("ml/onnx/test_classification.onnx").to_owned(),
            input_type: RasterDataType::F32,
            input_shape: MlTensorShape3D::new_single_pixel_bands(2),
            output_shape: MlTensorShape3D::new_single_pixel_single_band(),
            output_type: RasterDataType::I64,
            in_no_data_code: None,
            out_no_data_code: None
        };

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };
        exe_ctx.ml_models.insert(model_name, ml_model_metadata);

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 5),
            spatial_resolution: SpatialResolution::one(),
            attributes: [0].try_into().unwrap(),
        };

        let query_ctx = MockQueryContext::test_default();

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
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![33i64, 33, 42, 42])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![42i64, 42, 33, 33])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
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
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.2, 0.3, 0.4])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.5f32, 0.6, 0.7, 0.8])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.9f32, 0.8, 0.7, 0.6])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.5f32, 0.4, 0.3, 0.22])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data3: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.1f32, 0.2, 0.3, 0.4])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([2, 2].into(), vec![0.5f32, 0.6, 0.7, 0.8])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
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
                    time: None,
                    bbox: None,
                    resolution: None,
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
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
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
            params: OnnxParams::new_with_defaults(model_name.clone()),
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let ml_model_metadata = MlModelMetadata {
            file_path: test_data!("ml/onnx/test_regression.onnx").to_owned(),
            input_type: RasterDataType::F32,
            input_shape: MlTensorShape3D::new_single_pixel_bands(3),
            output_shape: MlTensorShape3D::new_single_pixel_single_band(),
            output_type: RasterDataType::F32,
            in_no_data_code: None,
            out_no_data_code: None
        };

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };
        exe_ctx.ml_models.insert(model_name, ml_model_metadata);

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked((0., 1.).into(), (3., 0.).into()),
            time_interval: TimeInterval::new_unchecked(0, 5),
            spatial_resolution: SpatialResolution::one(),
            attributes: [0].try_into().unwrap(),
        };

        let query_ctx = MockQueryContext::test_default();

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
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![0.1f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![1.0f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let data2: Vec<RasterTile2D<f32>> = vec![
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![0.2f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![2.0f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        let mrs1 = MockRasterSource {
            params: MockRasterSourceParams {
                data: data.clone(),
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::F32,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    time: None,
                    bbox: None,
                    resolution: None,
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
                    time: None,
                    bbox: None,
                    resolution: None,
                    bands: RasterBandDescriptors::new_single_band(),
                },
            },
        }
        .boxed();

        let stacker = RasterStacker {
            params: RasterStackerParams {
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
            params: OnnxParams::new_with_defaults(model_name.clone()),
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let ml_model_metadata = MlModelMetadata {
            file_path: test_data!("ml/onnx/test_a_plus_b.onnx").to_owned(),
            input_type: RasterDataType::F32,
            input_shape: MlTensorShape3D::new_y_x_bands(512, 512, 2),
            output_shape: MlTensorShape3D::new_y_x_bands(512, 512, 1),
            output_type: RasterDataType::F32,
            in_no_data_code: None,
            out_no_data_code: None
        };

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [512, 512],
        };
        exe_ctx.ml_models.insert(model_name, ml_model_metadata);

        let query_rect = RasterQueryRectangle {
            spatial_bounds: SpatialPartition2D::new_unchecked(
                (0., 511.).into(),
                (1023., 0.).into(),
            ),
            time_interval: TimeInterval::new_unchecked(0, 5),
            spatial_resolution: SpatialResolution::one(),
            attributes: [0].try_into().unwrap(),
        };

        let query_ctx = MockQueryContext::test_default();

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
                tile_position: [-1, 0].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![0.3f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
            RasterTile2D {
                time: TimeInterval::new_unchecked(0, 5),
                tile_position: [-1, 1].into(),
                band: 0,
                global_geo_transform: TestDefault::test_default(),
                grid_array: Grid::new([512, 512].into(), vec![3.0f32; 512 * 512])
                    .unwrap()
                    .into(),
                properties: Default::default(),
                cache_hint: CacheHint::default(),
            },
        ];

        assert!(expected.tiles_equal_ignoring_cache_hint(&result));
    }
}
