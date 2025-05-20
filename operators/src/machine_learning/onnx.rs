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
use geoengine_datatypes::machine_learning::{MlModelMetadata, MlModelName};
use geoengine_datatypes::primitives::{Measurement, RasterQueryRectangle};
use geoengine_datatypes::raster::{
    Grid, GridIdx2D, GridIndexAccess, GridShapeAccess, GridSize, Pixel, RasterTile2D,
};
use ndarray::{Array2, Array4};
use ort::tensor::{IntoTensorElementType, PrimitiveTensorElementType};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use super::onnx_util::load_onnx_model_from_metadata;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnnxParams {
    pub model: MlModelName,
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
}

impl<TIn, TOut> OnnxProcessor<TIn, TOut> {
    pub fn new(
        source: Box<dyn RasterQueryProcessor<RasterType = TIn>>,
        result_descriptor: RasterResultDescriptor,
        model_metadata: MlModelMetadata,
    ) -> Self {
        Self {
            source,
            result_descriptor,
            model_metadata,
            phantom: Default::default(),
        }
    }
}

#[async_trait]
impl<TIn, TOut> RasterQueryProcessor for OnnxProcessor<TIn, TOut>
where
    TIn: Pixel + NoDataValue,
    TOut: Pixel + IntoTensorElementType + PrimitiveTensorElementType,
    ort::value::Value: std::convert::TryFrom<ndarray::ArrayBase<ndarray::OwnedRepr<TIn>, ndarray::Dim<[usize; 2]>>>,
    ort::value::Value: std::convert::TryFrom<ndarray::ArrayBase<ndarray::OwnedRepr<TIn>, ndarray::Dim<[usize; 4]>>>,
    ort::Error: std::convert::From<
            <ort::value::Value as std::convert::TryFrom<
                ndarray::ArrayBase<ndarray::OwnedRepr<TIn>, ndarray::Dim<[usize; 2]>>,
            >>::Error,
        >,
    ort::Error: From<
        <ort::value::Value as std::convert::TryFrom<
            ndarray::ArrayBase<ndarray::OwnedRepr<TIn>, ndarray::Dim<[usize; 4]>>,
        >>::Error,
    >,
{
    type RasterType = TOut;

    async fn raster_query<'a>(
        &'a self,
        query: RasterQueryRectangle,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<RasterTile2D<TOut>>>> {
        let num_bands = self.source.raster_result_descriptor().bands.count() as usize;

        let mut source_query = query.clone();
        source_query.attributes = (0..num_bands as u32).collect::<Vec<u32>>().try_into()?;

        // TODO: re-use session accross queries?
        let session = load_onnx_model_from_metadata(&self.model_metadata)?;

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

                let tiles = chunk.into_iter().collect::<Result<Vec<_>>>()?;

                let first_tile = &tiles[0];
                let time = first_tile.time;
                let tile_position = first_tile.tile_position;
                let global_geo_transform = first_tile.global_geo_transform;
                let cache_hint = first_tile.cache_hint;

                let tile_shape = tiles[0].grid_shape();
                let width = tile_shape.axis_size_x();
                let height = tile_shape.axis_size_y();

                // TODO: collect into a ndarray directly

                // TODO: use flat array instead of nested Vecs
                let mut pixels: Vec<Vec<TIn>> = vec![vec![TIn::zero(); num_bands]; width * height];

                for (tile_index, tile) in tiles.into_iter().enumerate() {
                    // TODO: use map_elements or map_elements_parallel to avoid the double loop
                    for y in 0..height {
                        for x in 0..width {
                            let pixel_index = y * width + x;
                            let pixel_value = tile
                                .get_at_grid_index(GridIdx2D::from([y as isize, x as isize]))?
                                .unwrap_or(TIn::NO_DATA); // TODO: properly handle missing values or skip the pixel entirely instead
                            pixels[pixel_index][tile_index] = pixel_value;
                        }
                    }
                }

                let pixels = pixels.into_iter().flatten().collect::<Vec<TIn>>();
                let input_name = &session.inputs[0].name;


                let outputs = if self.model_metadata.input_is_single_pixel() {

                    let samples = Array2::from_shape_vec((pixels.len(), num_bands), pixels).expect(
                        "Array2 should be valid because it is created from a Vec with the correct size",
                    );

                    session
                        .run(ort::inputs![input_name => samples].context(Ort)?)
                        .context(Ort)
                } else if self.model_metadata.input_shape.yx_matches_tile_shape(&tile_shape){
                    let samples = Array4::from_shape_vec((1, height, width, num_bands), pixels).expect( // y,x, attributes
                        "Array4 should be valid because it is created from a Vec with the correct size",
                    );

                    session
                        .run(ort::inputs![input_name => samples].context(Ort)?)
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
                let (predictions, offset) = predictions.into_owned().into_raw_vec_and_offset();
                debug_assert!(offset.is_none() || offset == Some(0));

                // TODO: create no data mask from input no data masks
                Ok(RasterTile2D::new(
                    time,
                    tile_position,
                    0,
                    global_geo_transform,
                    Grid::new([width, height].into(), predictions)?.into(),
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
trait NoDataValue {
    const NO_DATA: Self;
}

impl NoDataValue for f32 {
    const NO_DATA: Self = f32::NAN;
}

impl NoDataValue for f64 {
    const NO_DATA: Self = f64::NAN;
}

// Define a macro to implement NoDataValue for various types with NO_DATA as 0
macro_rules! impl_no_data_value_zero {
    ($($t:ty),*) => {
        $(
            impl NoDataValue for $t {
                const NO_DATA: Self = 0;
            }
        )*
    };
}

// Use the macro to implement NoDataValue for i8, u8, i16, u16, etc.
impl_no_data_value_zero!(i8, u8, i16, u16, i32, u32, i64, u64);

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::{
        engine::{
            MockExecutionContext, MockQueryContext, MultipleRasterSources, RasterBandDescriptors,
        },
        machine_learning::{
            error::MultipleInputsNotSupported,
            onnx_util::{
                try_onnx_tensor_to_ml_tensorshape_3d, try_raster_datatype_from_tensor_element_type,
            },
        },
        mock::{MockRasterSource, MockRasterSourceParams},
        processing::{RasterStacker, RasterStackerParams},
    };
    use approx::assert_abs_diff_eq;
    use geoengine_datatypes::{
        primitives::{CacheHint, SpatialPartition2D, SpatialResolution, TimeInterval},
        raster::{
            GridOrEmpty, GridShape, RasterDataType, RenameBands, TilesEqualIgnoringCacheHint,
        },
        spatial_reference::SpatialReference,
        test_data,
        util::test::TestDefault,
    };
    use ndarray::{Array1, Array2, arr2, array};

    fn generate_model_metadata_from_onnx(
        path: &Path,
    ) -> Result<MlModelMetadata, MachineLearningError> {
        // TODO: proper error if model file cannot be found
        let session = ort::session::Session::builder()
            .context(Ort)?
            .commit_from_file(path)
            .context(Ort)?;

        // Onnx model may have multiple inputs, but we only support one input (with multiple features/bands)
        ensure!(
            session.inputs.len() == 1,
            MultipleInputsNotSupported {
                num_inputs: session.inputs.len()
            }
        );

        // Onnx model input type must be a Tensor in order to accept a 2d ndarray as input
        let ort::value::ValueType::Tensor {
            ty: input_tensor_element_type,
            dimensions: input_dimensions,
            dimension_symbols: _dimension_symbols,
        } = &session.inputs[0].input_type
        else {
            return Err(MachineLearningError::InvalidInputType {
                input_type: session.inputs[0].input_type.clone(),
            });
        };

        // Input dimensions must be convertable to a valid tensor shape
        let input_shape = try_onnx_tensor_to_ml_tensorshape_3d(input_dimensions)?;

        // Onnx model must output one prediction per pixel as
        // (1) a Tensor with a single dimension of unknown size (dim = [-1]), or
        // (2) a Tensor with two dimensions, the first of unknown size and the second of size 1 (dim = [-1, 1])
        let ort::value::ValueType::Tensor {
            ty: output_tensor_element_type,
            dimensions: output_dimensions,
            dimension_symbols: _,
        } = &session.outputs[0].output_type
        else {
            return Err(MachineLearningError::InvalidOutputType {
                output_type: session.outputs[0].output_type.clone(),
            });
        };

        let output_shape = try_onnx_tensor_to_ml_tensorshape_3d(output_dimensions)?;

        Ok(MlModelMetadata {
            file_path: path.to_owned(),
            input_type: try_raster_datatype_from_tensor_element_type(*input_tensor_element_type)?,
            input_shape,
            output_shape,
            output_type: try_raster_datatype_from_tensor_element_type(*output_tensor_element_type)?,
        })
    }

    #[test]
    fn ort() {
        let session = ort::session::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("ml/onnx/test_classification.onnx"))
            .unwrap();

        let input_name = &session.inputs[0].name;

        let new_samples = arr2(&[[0.1f32, 0.2], [0.2, 0.3], [0.2, 0.2], [0.3, 0.1]]);

        let outputs = session
            .run(ort::inputs![input_name => new_samples].unwrap())
            .unwrap();

        let predictions = outputs["output_label"]
            .try_extract_tensor::<i64>()
            .unwrap()
            .into_owned()
            .into_dimensionality()
            .unwrap();

        assert_eq!(predictions, &array![33i64, 33, 42, 42]);
    }

    #[test]
    fn ort_dynamic() {
        let session = ort::session::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("ml/onnx/test_classification.onnx"))
            .unwrap();

        let input_name = &session.inputs[0].name;

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
            .run(ort::inputs![input_name => new_samples].unwrap())
            .unwrap();

        let predictions = outputs["output_label"]
            .try_extract_tensor::<i64>()
            .unwrap()
            .into_owned()
            .into_dimensionality()
            .unwrap();

        assert_eq!(predictions, &array![33i64, 33, 42, 42]);
    }

    #[test]
    fn regression() {
        let session = ort::session::Session::builder()
            .unwrap()
            .commit_from_file(test_data!("ml/onnx/test_regression.onnx"))
            .unwrap();

        let input_name = &session.inputs[0].name;

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
            .run(ort::inputs![input_name => new_samples].unwrap())
            .unwrap();

        let predictions: Array1<f32> = outputs["variable"]
            .try_extract_tensor::<f32>()
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
            params: OnnxParams {
                model: model_name.clone(),
            },
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };
        exe_ctx.ml_models.insert(
            model_name,
            generate_model_metadata_from_onnx(test_data!("ml/onnx/test_classification.onnx"))
                .unwrap(),
        );

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
            params: OnnxParams {
                model: model_name.clone(),
            },
            sources: SingleRasterSource { raster: stacker },
        }
        .boxed();

        let mut exe_ctx = MockExecutionContext::test_default();
        exe_ctx.tiling_specification.tile_size_in_pixels = GridShape {
            shape_array: [2, 2],
        };
        exe_ctx.ml_models.insert(
            model_name,
            generate_model_metadata_from_onnx(test_data!("ml/onnx/test_regression.onnx")).unwrap(),
        );

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
}
