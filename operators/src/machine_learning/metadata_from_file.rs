use super::MachineLearningError;
use crate::machine_learning::error::{MultipleInputsNotSupported, Ort};
use geoengine_datatypes::{
    machine_learning::{MlModelMetadata, TensorShape3D},
    raster::RasterDataType,
};
use snafu::{ensure, ResultExt};
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
    let input_shape = try_dimensions_to_tensor_shape(input_dimensions)?;

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

    let output_shape = try_dimensions_to_tensor_shape(output_dimensions)?;

    Ok(MlModelMetadata {
        file_path: path.to_owned(),
        input_type: try_raster_datatype_from_tensor_element_type(*input_tensor_element_type)?,
        input_shape,
        output_shape,
        output_type: try_raster_datatype_from_tensor_element_type(*output_tensor_element_type)?,
    })
}

fn try_dimensions_to_tensor_shape(
    dimensions: &[i64],
) -> Result<TensorShape3D, MachineLearningError> {
    if dimensions.len() == 1 && dimensions[0] == -1 {
        Ok(TensorShape3D::new_y_x_bands(1, 1, 1))
    } else if dimensions.len() == 2 && dimensions[0] == -1 && dimensions[1] > 0 {
        Ok(TensorShape3D::new_y_x_bands(1, 1, dimensions[1] as u32))
    } else if dimensions.len() == 4
        && dimensions[0] == -1
        && dimensions[1] > 0
        && dimensions[2] > 0
        && dimensions[3] > 0
    {
        Ok(TensorShape3D::new_y_x_bands(
            dimensions[1] as u32, // TODO: figure out how the axis in the dimensions are ordered!
            dimensions[2] as u32,
            dimensions[3] as u32, // In this case we could also accept attributes at first position, however we need to figure out how we would handle this...
        ))
    } else {
        Err(MachineLearningError::InvalidDimensions {
            dimensions: dimensions.to_vec(),
        })
    }
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
