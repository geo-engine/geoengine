use geoengine_datatypes::{
    machine_learning::MlTensorShape3D,
    raster::{GridShape2D, RasterDataType},
};
use ort::tensor::TensorElementType;
use snafu::Snafu;
pub mod onnx;
pub mod onnx_util;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)), module(error))] // disables default `Snafu` suffix
pub enum MachineLearningError {
    #[snafu(display("Error in Onnx model: {}", source))]
    Ort { source: ort::Error },
    #[snafu(display("Onnx model may only have one input. Found {}.", num_inputs))]
    MultipleInputsNotSupported { num_inputs: usize },
    #[snafu(display("Onnx model must have Tensor input. Found {:?}.", input_type))]
    InvalidInputType { input_type: ort::value::ValueType },
    #[snafu(display(
        "Onnx model must have two dimensional input ([-1, b], b > 0). Found [{}].",
        dimensions.iter().map(std::string::ToString::to_string).collect::<Vec<_>>().join(", ")
    ))]
    InvalidDimensions { dimensions: Vec<i64> },
    #[snafu(display(
        "Onnx model must have one dimensional output or match the tiling tile size. Found {:?} and {:?}.",
        tensor_shape,
        tiling_shape
    ))]
    InvalidInputPixelShape {
        tensor_shape: MlTensorShape3D,
        tiling_shape: GridShape2D,
    },
    #[snafu(display(
        "Onnx model tensor shape must match the combination of raster y,x pixels * bands. Found {:?} and {:?}.",
        model_shape,
        input_shape
    ))]
    InvalidInputTensorShape {
        model_shape: MlTensorShape3D,
        input_shape: MlTensorShape3D,
    },
    #[snafu(display(
        "Onnx model must have one dimensional output or match the tiling tile size. Found {:?} and {:?}.",
        tensor_shape,
        tiling_shape
    ))]
    InvalidOutputPixelShape {
        tensor_shape: MlTensorShape3D,
        tiling_shape: GridShape2D,
    },
    #[snafu(display("Onnx model must have Tensor output. Found {:?}.", output_type))]
    InvalidOutputType { output_type: ort::value::ValueType },
    #[snafu(display("Onnx tensor element type {:?} is not supported.", element_type))]
    UnsupportedTensorElementType {
        element_type: ort::tensor::TensorElementType,
    },
    #[snafu(display(
        "The input shape of the model input  ({model_dimensions:?} => {model_shape:?}) does not match the metadata input spec ({metadata_shape:?})."
    ))]
    MetadataModelInputShapeMismatch {
        model_dimensions: Vec<i64>,
        model_shape: MlTensorShape3D,
        metadata_shape: MlTensorShape3D,
    },
    #[snafu(display(
        "The output shape of the model input  ({model_dimensions:?} => {model_shape:?}) does not match the metadata input spec ({metadata_shape:?})."
    ))]
    MetadataModelOutputShapeMismatch {
        model_dimensions: Vec<i64>,
        model_shape: MlTensorShape3D,
        metadata_shape: MlTensorShape3D,
    },
    #[snafu(display(
        "The input type of the model ({model_tensor_type:?} => {model_raster_type:?}) does not match the metadata input type  ({metadata_type:?})."
    ))]
    MetadataModelInputTypeMismatch {
        model_tensor_type: TensorElementType,
        model_raster_type: RasterDataType,
        metadata_type: RasterDataType,
    },
    #[snafu(display(
        "The output type of the model ({model_tensor_type:?} => {model_raster_type:?}) does not match the metadata output type  ({metadata_type:?})."
    ))]
    MetadataModelOutputTypeMismatch {
        model_tensor_type: TensorElementType,
        model_raster_type: RasterDataType,
        metadata_type: RasterDataType,
    },
    #[snafu(display(
        "Raster data types of source ({source_type:?}) does not match model input type ({model_input_type:?})."
    ))]
    InputTypeMismatch {
        model_input_type: RasterDataType,
        source_type: RasterDataType,
    },
    #[snafu(display(
        "Model input and output shapes must match. Found in: {in_shape:?} and out: {out_shape:?}."
    ))]
    UnsupportedInOutMapping {
        in_shape: MlTensorShape3D,
        out_shape: MlTensorShape3D,
    },
}

impl From<MachineLearningError> for crate::error::Error {
    fn from(error: MachineLearningError) -> Self {
        Self::MachineLearning {
            source: Box::new(error),
        }
    }
}
