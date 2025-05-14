use geoengine_datatypes::{
    machine_learning::{MlModelMetadata, MlTensorShape3D},
    raster::{GridShape2D, GridSize, RasterDataType},
};
use ort::session::Session;
use snafu::{ResultExt, ensure};

use crate::machine_learning::error::{
    InvalidInputPixelShape, InvalidInputTensorShape, InvalidInputType, InvalidOutputPixelShape,
    InvalidOutputType, MetadataModelInputShapeMismatch, MetadataModelInputTypeMismatch,
    MetadataModelOutputShapeMismatch, MultipleInputsNotSupported, UnsupportedInOutMapping,
};

use super::{MachineLearningError, error::Ort};

pub fn load_onnx_model_from_metadata(
    ml_model_metadata: &MlModelMetadata,
) -> Result<Session, MachineLearningError> {
    ort::session::Session::builder()
        .context(Ort)?
        .commit_from_file(&ml_model_metadata.file_path)
        .context(Ort)
        .inspect_err(|e| {
            tracing::debug!(
                "Could not create ONNX session for {:?}. Error: {}",
                ml_model_metadata.file_path.file_name(),
                e
            );
        })
}

pub fn check_model_shape(
    model_metadata: &MlModelMetadata,
    tiling_shape: GridShape2D,
) -> Result<(), MachineLearningError> {
    check_model_input_shape_supported(model_metadata, tiling_shape)?;
    check_model_output_shape_supported(model_metadata, tiling_shape)?;
    check_input_output_mapping_supported(model_metadata)
}

pub fn check_model_input_shape_supported(
    model_metadata: &MlModelMetadata,
    tiling_shape: GridShape2D,
) -> Result<(), MachineLearningError> {
    // check that we can use the model input shape with the operator
    ensure!(
        model_metadata.input_is_single_pixel()
            || model_metadata
                .input_shape
                .yx_matches_tile_shape(&tiling_shape),
        InvalidInputPixelShape {
            tensor_shape: model_metadata.input_shape,
            tiling_shape
        }
    );

    Ok(())
}

pub fn check_model_output_shape_supported(
    model_metadata: &MlModelMetadata,
    tiling_shape: GridShape2D,
) -> Result<(), MachineLearningError> {
    // check that we can use the model output shape with the operator
    ensure!(
        model_metadata.output_is_single_pixel()
            || model_metadata
                .output_shape
                .yx_matches_tile_shape(&tiling_shape),
        InvalidOutputPixelShape {
            tensor_shape: model_metadata.output_shape,
            tiling_shape
        }
    );

    Ok(())
}

pub fn check_input_output_mapping_supported(
    model_metadata: &MlModelMetadata,
) -> Result<(), MachineLearningError> {
    ensure!(
        model_metadata.input_shape.axis_size_x() == model_metadata.output_shape.axis_size_x()
            && model_metadata.input_shape.axis_size_y()
                == model_metadata.output_shape.axis_size_y(),
        UnsupportedInOutMapping {
            in_shape: model_metadata.input_shape,
            out_shape: model_metadata.output_shape
        }
    );

    Ok(())
}

pub fn try_onnx_tensor_to_ml_tensorshape_3d(
    tensor_dimensions: &[i64],
) -> Result<MlTensorShape3D, MachineLearningError> {
    match *tensor_dimensions {
        [-1] => Ok(MlTensorShape3D {
            x: 1,
            y: 1,
            bands: 1,
        }),
        [bands] | [1 | -1, bands] if bands > 0 => Ok(MlTensorShape3D {
            x: 1,
            y: 1,
            bands: (bands as u32),
        }),
        [x, y] | [-1 | 1, x, y] if x > 0 && y > 0 => Ok(MlTensorShape3D {
            x: x as u32,
            y: y as u32,
            bands: 1,
        }),
        [x, y, bands] | [-1 | 1, x, y, bands] if x > 0 && y > 0 && bands > 0 => {
            Ok(MlTensorShape3D {
                x: x as u32,
                y: y as u32,
                bands: bands as u32,
            })
        }
        _ => Err(MachineLearningError::InvalidDimensions {
            dimensions: tensor_dimensions.to_vec(),
        }),
    }
}

///
/// Check that the session input is a tensor with the dimension specified in the metadata.
///
/// # Panics
///
/// If the input is a tensor but no `tensor_dimension` is provided.
///
pub fn check_onnx_model_input_matches_metadata(
    session: &Session,
    metadata_input: MlTensorShape3D,
    metadata_input_type: RasterDataType,
) -> Result<(), MachineLearningError> {
    let inputs = &session.inputs;
    ensure!(
        inputs.len() == 1,
        MultipleInputsNotSupported {
            num_inputs: inputs.len()
        }
    );

    let input = &inputs[0];
    ensure!(
        input.input_type.is_tensor(),
        InvalidInputType {
            input_type: input.input_type.clone()
        }
    );
    let dimensions = input
        .input_type
        .tensor_dimensions()
        .expect("input must be a tensor. checked before!");

    let shape = try_onnx_tensor_to_ml_tensorshape_3d(dimensions)?;

    ensure!(
        shape == metadata_input,
        MetadataModelInputShapeMismatch {
            model_dimensions: dimensions.clone(),
            model_shape: shape,
            metadata_shape: metadata_input
        }
    );

    let input_tensor_type = input
        .input_type
        .tensor_type()
        .expect("input must be a tensor. ckecked above!");
    let input_raster_type = try_raster_datatype_from_tensor_element_type(input_tensor_type)?;

    ensure!(
        input_raster_type == metadata_input_type,
        MetadataModelInputTypeMismatch {
            model_tensor_type: input_tensor_type,
            model_raster_type: input_raster_type,
            metadata_type: metadata_input_type
        }
    );

    Ok(())
}

///
/// Check that the session output is a tensor with the dimension speified in the metadata.
///
/// # Panics
///
/// If the output is a tensor but no `tensor_dimension` is provided.
///
pub fn check_onnx_model_output_matches_metadata(
    session: &Session,
    metadata_output: MlTensorShape3D,
    metadata_output_type: RasterDataType,
) -> Result<(), MachineLearningError> {
    let outputs = &session.outputs;

    // we assume that the first output is the one to use
    // TODO: make this configurable?
    let output = &outputs[0];
    ensure!(
        output.output_type.is_tensor(),
        InvalidOutputType {
            output_type: output.output_type.clone()
        }
    );

    let dimensions = output
        .output_type
        .tensor_dimensions()
        .expect("input must be a tensor. checked before!");

    let shape = try_onnx_tensor_to_ml_tensorshape_3d(dimensions)?;

    ensure!(
        shape == metadata_output,
        MetadataModelOutputShapeMismatch {
            model_dimensions: dimensions.clone(),
            model_shape: shape,
            metadata_shape: metadata_output
        }
    );

    let output_tensor_type = output
        .output_type
        .tensor_type()
        .expect("output must be a tensor. ckecked above!");
    let output_raster_type = try_raster_datatype_from_tensor_element_type(output_tensor_type)?;

    ensure!(
        output_raster_type == metadata_output_type,
        MetadataModelInputTypeMismatch {
            model_tensor_type: output_tensor_type,
            model_raster_type: output_raster_type,
            metadata_type: metadata_output_type
        }
    );

    Ok(())
}

pub fn check_onnx_model_matches_metadata(
    session: &Session,
    model_metadata: &MlModelMetadata,
) -> Result<(), MachineLearningError> {
    check_onnx_model_input_matches_metadata(
        session,
        model_metadata.input_shape,
        model_metadata.input_type,
    )?;
    check_onnx_model_output_matches_metadata(
        session,
        model_metadata.output_shape,
        model_metadata.output_type,
    )
}

pub fn check_model_input_features(
    model_metadata: &MlModelMetadata,
    tiling_shape: GridShape2D,
    num_bands: u32,
) -> Result<(), MachineLearningError> {
    let used_in_shape = if model_metadata.input_is_single_pixel() {
        MlTensorShape3D::new_single_pixel_bands(num_bands)
    } else {
        MlTensorShape3D::new_y_x_bands(
            tiling_shape.axis_size_y() as u32,
            tiling_shape.axis_size_x() as u32,
            num_bands,
        )
    };

    // check that number of input bands fits number of model features
    ensure!(
        model_metadata.input_shape == used_in_shape,
        InvalidInputTensorShape {
            input_shape: used_in_shape,
            model_shape: model_metadata.input_shape
        }
    );

    Ok(())
}

// can't implement `TryFrom` here because `RasterDataType` is in operators crate
pub(crate) fn try_raster_datatype_from_tensor_element_type(
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
