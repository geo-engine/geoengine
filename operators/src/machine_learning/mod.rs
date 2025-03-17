use geoengine_datatypes::raster::RasterDataType;
use snafu::Snafu;

pub mod metadata_from_file;
pub mod onnx;

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
    InvalidInputDimensions { dimensions: Vec<i64> },
    #[snafu(display(
        "Onnx model must have one dimensional output. Found [{}].",
        dimensions.iter().map(std::string::ToString::to_string).collect::<Vec<_>>().join(", ")
    ))]
    InvalidOutputDimensions { dimensions: Vec<i64> },
    #[snafu(display("Onnx model must have Tensor output. Found {:?}.", output_type))]
    InvalidOutputType { output_type: ort::value::ValueType },
    #[snafu(display("Onnx tensor element type {:?} is not supported.", element_type))]
    UnsupportedTensorElementType {
        element_type: ort::tensor::TensorElementType,
    },
    #[snafu(display(
        "Number of bands in source ({source_bands}) does not match the model input bands ({model_input_bands})."
    ))]
    InputBandsMismatch {
        model_input_bands: u32,
        source_bands: u32,
    },
    #[snafu(display(
        "Raster data types of source ({source_type:?}) does not match model input type ({model_input_type:?})."
    ))]
    InputTypeMismatch {
        model_input_type: RasterDataType,
        source_type: RasterDataType,
    },
}

impl From<MachineLearningError> for crate::error::Error {
    fn from(error: MachineLearningError) -> Self {
        Self::MachineLearning {
            source: Box::new(error),
        }
    }
}
