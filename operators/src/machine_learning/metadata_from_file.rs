use super::MachineLearningError;
use crate::machine_learning::error::{
    InvalidInputDimensions, InvalidOutputDimensions, MultipleInputsNotSupported, Ort,
};
use geoengine_datatypes::{machine_learning::MlModelMetadata, raster::RasterDataType};
use snafu::{ResultExt, ensure};
use std::path::Path;

pub fn load_model_metadata(path: &Path) -> Result<MlModelMetadata, MachineLearningError> {
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

    // Input dimensions must be [-1, b] to accept a table of (arbitrarily many) single pixel features (rows) with `b` bands (columns)
    ensure!(
        input_dimensions.len() == 2 && input_dimensions[0] == -1 && input_dimensions[1] > 0,
        InvalidInputDimensions {
            dimensions: input_dimensions.clone()
        }
    );

    // Onnx model must output one prediction per pixel as
    // (1) a Tensor with a single dimension of unknown size (dim = [-1]), or
    // (2) a Tensor with two dimensions, the first of unknown size and the second of size 1 (dim = [-1, 1])
    let output_tensor_element_type = if let ort::value::ValueType::Tensor {
        ty,
        dimensions,
        dimension_symbols: _dimension_symbols,
    } = &session.outputs[0].output_type
    {
        ensure!(
            dimensions == &[-1] || dimensions == &[-1, 1],
            InvalidOutputDimensions {
                dimensions: dimensions.clone()
            }
        );

        ty
    } else {
        return Err(MachineLearningError::InvalidOutputType {
            output_type: session.outputs[0].output_type.clone(),
        });
    };

    Ok(MlModelMetadata {
        file_path: path.to_owned(),
        input_type: try_raster_datatype_from_tensor_element_type(*input_tensor_element_type)?,
        num_input_bands: input_dimensions[1] as u32,
        output_type: try_raster_datatype_from_tensor_element_type(*output_tensor_element_type)?,
    })
}

// can't implement `TryFrom` here because `RasterDataType` is in operators crate
fn try_raster_datatype_from_tensor_element_type(
    value: ort::tensor::TensorElementType,
) -> Result<RasterDataType, MachineLearningError> {
    match value {
        ort::tensor::TensorElementType::Float32 => Ok(RasterDataType::F32),
        ort::tensor::TensorElementType::Uint8 | ort::tensor::TensorElementType::Bool => {
            Ok(RasterDataType::U8)
        }
        ort::tensor::TensorElementType::Int8 => Ok(RasterDataType::I8),
        ort::tensor::TensorElementType::Uint16 => Ok(RasterDataType::U16),
        ort::tensor::TensorElementType::Int16 => Ok(RasterDataType::I16),
        ort::tensor::TensorElementType::Int32 => Ok(RasterDataType::I32),
        ort::tensor::TensorElementType::Int64 => Ok(RasterDataType::I64),
        ort::tensor::TensorElementType::Float64 => Ok(RasterDataType::F64),
        ort::tensor::TensorElementType::Uint32 => Ok(RasterDataType::U32),
        ort::tensor::TensorElementType::Uint64 => Ok(RasterDataType::U64),
        _ => Err(MachineLearningError::UnsupportedTensorElementType {
            element_type: value,
        }),
    }
}
