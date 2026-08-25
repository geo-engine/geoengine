use super::{MachineLearningError, error::Ort};
use crate::machine_learning::{
    MlModelLoadingInfo, MlModelMetadata,
    error::{
        AsymmetricModelOverlap, InvalidInputPixelShape, InvalidInputTensorShape,
        InvalidOutputPixelShape, InvalidOutputType, MetadataModelInputShapeMismatch,
        MetadataModelInputTypeMismatch, MetadataModelOutputShapeMismatch,
        MultipleInputsNotSupported, UnsupportedInOutMapping, UnsupportedNumberOfOutputAttributes,
    },
};
use geoengine_datatypes::{
    machine_learning::MlTensorShape3D,
    raster::{GridShape2D, GridSize, RasterDataType, TileOverlap},
};
use ort::session::Session;
use snafu::{ResultExt, ensure};

pub fn load_onnx_model_from_loading_info(
    ml_model_loading_info: &MlModelLoadingInfo,
) -> Result<Session, MachineLearningError> {
    ort::session::Session::builder()
        .context(Ort)?
        .commit_from_file(&ml_model_loading_info.storage_path)
        .context(Ort)
        .inspect_err(|e| {
            tracing::debug!(
                "Could not create ONNX session for {:?}. Error: {}",
                ml_model_loading_info.storage_path.file_name(),
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

    ensure!(
        model_metadata.output_is_single_attribute(),
        UnsupportedNumberOfOutputAttributes {
            output_attributes: model_metadata.num_output_bands()
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

/// The full data grid size for a `core` region padded by `halo` on every side.
#[must_use]
pub fn model_data_shape(core: &GridShape2D, halo: TileOverlap) -> GridShape2D {
    GridShape2D::new_2d(
        core.axis_size_y() + 2 * halo.axis_size_y(),
        core.axis_size_x() + 2 * halo.axis_size_x(),
    )
}

/// Like [`check_model_shape`], but for convolution models whose input and output
/// pixel shapes are the `core` padded by a (possibly different) symmetric halo.
///
/// Unlike [`check_model_shape`] this does **not** require `input.yx == output.yx`;
/// the two only need to share the same central `core`. The even/non-negative halo
/// derivation itself is validated by [`MlModelMetadata::input_output_overlap`].
pub fn check_model_shape_overlap(
    model_metadata: &MlModelMetadata,
    core: &GridShape2D,
    halo_in: TileOverlap,
    halo_out: TileOverlap,
) -> Result<(), MachineLearningError> {
    // A single-pixel model cannot carry a halo.
    ensure!(
        !model_metadata.input_is_single_pixel() || halo_in.is_zero(),
        AsymmetricModelOverlap {
            in_shape: model_metadata.input_shape,
            out_shape: model_metadata.output_shape,
            core: *core,
        }
    );
    ensure!(
        !model_metadata.output_is_single_pixel() || halo_out.is_zero(),
        AsymmetricModelOverlap {
            in_shape: model_metadata.input_shape,
            out_shape: model_metadata.output_shape,
            core: *core,
        }
    );

    let in_data = model_data_shape(core, halo_in);
    let out_data = model_data_shape(core, halo_out);
    check_model_input_shape_supported(model_metadata, in_data)?;
    check_model_output_shape_supported(model_metadata, out_data)
}

pub fn try_onnx_tensor_to_ml_tensorshape_3d(
    tensor_dimensions: &ort::value::Shape,
) -> Result<MlTensorShape3D, MachineLearningError> {
    let td: &[i64] = tensor_dimensions.as_ref();

    match *td {
        [] | [-1..=1] => Ok(MlTensorShape3D {
            x: 1,
            y: 1,
            bands: 1,
        }),
        [bands] | [-1..=1, bands] if bands > 0 => Ok(MlTensorShape3D {
            x: 1,
            y: 1,
            bands: (bands as u32),
        }),
        [x, y] | [-1..=1, x, y] if x > 0 && y > 0 => Ok(MlTensorShape3D {
            x: x as u32,
            y: y as u32,
            bands: 1,
        }),
        [x, y, bands] | [-1..=1, x, y, bands] if x > 0 && y > 0 && bands > 0 => {
            Ok(MlTensorShape3D {
                x: x as u32,
                y: y as u32,
                bands: bands as u32,
            })
        }
        _ => Err(MachineLearningError::InvalidDimensions {
            dimensions: td.to_vec(),
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
    let inputs = &session.inputs();
    ensure!(
        inputs.len() == 1,
        MultipleInputsNotSupported {
            num_inputs: inputs.len()
        }
    );

    let input = &inputs[0];

    let (Some(input_tensor_type), Some(tensor_shape)) =
        (input.dtype().tensor_type(), input.dtype().tensor_shape())
    else {
        return Err(MachineLearningError::InvalidInputType {
            input_type: input.dtype().clone(),
        });
    };

    let shape = try_onnx_tensor_to_ml_tensorshape_3d(tensor_shape)?;

    ensure!(
        shape == metadata_input,
        MetadataModelInputShapeMismatch {
            model_dimensions: (*tensor_shape).to_vec(),
            model_shape: shape,
            metadata_shape: metadata_input
        }
    );

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
    let outputs = &session.outputs();

    // we assume that the first output is the one to use
    // TODO: make this configurable?
    let output = &outputs[0];
    ensure!(
        output.dtype().is_tensor(),
        InvalidOutputType {
            output_type: output.dtype().clone()
        }
    );

    let dimensions = output
        .dtype()
        .tensor_shape()
        .expect("input must be a tensor. checked before!");

    let shape = try_onnx_tensor_to_ml_tensorshape_3d(dimensions)?;

    ensure!(
        shape == metadata_output,
        MetadataModelOutputShapeMismatch {
            model_dimensions: (*dimensions).to_vec(),
            model_shape: shape,
            metadata_shape: metadata_output
        }
    );

    let output_tensor_type = output
        .dtype()
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
    value: ort::value::TensorElementType,
) -> Result<RasterDataType, MachineLearningError> {
    match value {
        ort::value::TensorElementType::Float32 => Ok(RasterDataType::F32),
        ort::value::TensorElementType::Uint8 | ort::value::TensorElementType::Bool => {
            Ok(RasterDataType::U8)
        }
        ort::value::TensorElementType::Int8 => Ok(RasterDataType::I8),
        ort::value::TensorElementType::Uint16 => Ok(RasterDataType::U16),
        ort::value::TensorElementType::Int16 => Ok(RasterDataType::I16),
        ort::value::TensorElementType::Int32 => Ok(RasterDataType::I32),
        ort::value::TensorElementType::Int64 => Ok(RasterDataType::I64),
        ort::value::TensorElementType::Float64 => Ok(RasterDataType::F64),
        ort::value::TensorElementType::Uint32 => Ok(RasterDataType::U32),
        ort::value::TensorElementType::Uint64 => Ok(RasterDataType::U64),
        _ => Err(MachineLearningError::UnsupportedTensorElementType {
            element_type: value,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine_learning::{MlModelInputNoDataHandling, MlModelOutputNoDataHandling};

    fn metadata(in_yx: (u32, u32), out_yx: (u32, u32)) -> MlModelMetadata {
        MlModelMetadata {
            input_type: RasterDataType::F32,
            output_type: RasterDataType::F32,
            input_shape: MlTensorShape3D::new_y_x_bands(in_yx.0, in_yx.1, 3),
            output_shape: MlTensorShape3D::new_y_x_bands(out_yx.0, out_yx.1, 1),
            input_no_data_handling: MlModelInputNoDataHandling::SkipIfNoData,
            output_no_data_handling: MlModelOutputNoDataHandling::NanIsNoData,
        }
    }

    #[test]
    fn data_shape_pads_core() {
        let core = GridShape2D::new_2d(10, 10);
        assert_eq!(
            model_data_shape(&core, TileOverlap::new(1, 2)),
            GridShape2D::new_2d(12, 14)
        );
        assert_eq!(model_data_shape(&core, TileOverlap::zero()), core);
    }

    #[test]
    fn overlap_input_and_output_shapes_supported() {
        // core (y=10,x=12); in (y=12,x=16) -> halo_in (1,2); out (y=10,x=14) -> halo_out (0,1)
        let m = metadata((12, 16), (10, 14));
        check_model_shape_overlap(
            &m,
            &GridShape2D::new_2d(10, 12),
            TileOverlap::new(1, 2),
            TileOverlap::new(0, 1),
        )
        .unwrap();
    }

    #[test]
    fn equal_shapes_no_overlap_still_supported() {
        let m = metadata((10, 10), (10, 10));
        check_model_shape_overlap(
            &m,
            &GridShape2D::new_2d(10, 10),
            TileOverlap::zero(),
            TileOverlap::zero(),
        )
        .unwrap();
    }

    #[test]
    fn single_pixel_input_with_halo_rejected() {
        let m = metadata((1, 1), (1, 1));
        assert!(matches!(
            check_model_shape_overlap(
                &m,
                &GridShape2D::new_2d(1, 1),
                TileOverlap::new(1, 0),
                TileOverlap::zero(),
            ),
            Err(MachineLearningError::AsymmetricModelOverlap { .. })
        ));
    }
}
