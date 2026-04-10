"""
Util functions for machine learning
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

import geoengine_openapi_client
from geoengine_openapi_client.models import MlModel, MlModelMetadata, MlTensorShape3D, RasterDataType
from onnx import ModelProto, TensorProto, TypeProto
from onnx.helper import tensor_dtype_to_string

from geoengine.auth import get_session
from geoengine.error import InputException
from geoengine.resource_identifier import MlModelName, UploadId


@dataclass
class MlModelConfig:
    """Configuration for an ml model"""

    name: str
    file_name: str
    metadata: MlModelMetadata
    display_name: str = "My Ml Model"
    description: str = "My Ml Model Description"


def register_ml_model(
    onnx_model: ModelProto, model_config: MlModelConfig, upload_timeout: int = 3600, register_timeout: int = 60
) -> MlModelName:
    """Uploads an onnx file and registers it as an ml model"""

    validate_model_config(
        onnx_model,
        input_type=model_config.metadata.input_type,
        output_type=model_config.metadata.output_type,
        input_shape=model_config.metadata.input_shape,
        out_shape=model_config.metadata.output_shape,
    )
    check_backend_constraints(model_config.metadata.input_shape, model_config.metadata.output_shape)

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        with tempfile.TemporaryDirectory() as temp_dir:
            file_name = Path(temp_dir) / model_config.file_name

            with open(file_name, "wb") as file:
                file.write(onnx_model.SerializeToString())

            uploads_api = geoengine_openapi_client.UploadsApi(api_client)
            response = uploads_api.upload_handler([str(file_name)], _request_timeout=upload_timeout)

        upload_id = UploadId.from_response(response)

        ml_api = geoengine_openapi_client.MLApi(api_client)

        model = MlModel(
            name=model_config.name,
            file_name=model_config.file_name,
            upload=str(upload_id),
            metadata=model_config.metadata,
            display_name=model_config.display_name,
            description=model_config.description,
        )
        res_name = ml_api.add_ml_model(model, _request_timeout=register_timeout)
        return MlModelName.from_response(res_name)


def model_dim_to_tensorshape(model_dims):
    """Transform an ONNX dimension into a MlTensorShape3D"""

    mts = MlTensorShape3D(x=1, y=1, bands=1)
    if len(model_dims) == 1 and model_dims[0].dim_value in (-1, 0):
        pass  # in this case, the model will produce as many outs as inputs
    elif len(model_dims) == 1 and model_dims[0].dim_value > 0:
        mts.bands = model_dims[0].dim_value
    elif len(model_dims) == 2:
        if model_dims[0].dim_value in (None, -1, 0, 1):
            mts.bands = model_dims[1].dim_value
        else:
            mts.y = model_dims[0].dim_value
            mts.x = model_dims[1].dim_value
    elif len(model_dims) == 3:
        if model_dims[0].dim_value in (None, -1, 0, 1):
            mts.y = model_dims[1].dim_value
            mts.x = model_dims[2].dim_value
        else:
            mts.y = model_dims[0].dim_value
            mts.x = model_dims[1].dim_value
            mts.bands = model_dims[2].dim_value
    elif len(model_dims) == 4 and model_dims[0].dim_value in (None, -1, 0, 1):
        mts.y = model_dims[1].dim_value
        mts.x = model_dims[2].dim_value
        mts.bands = model_dims[3].dim_value
    else:
        raise InputException(f"Only 1D and 3D input tensors are supported. Got model dim {model_dims}")
    return mts


def check_backend_constraints(input_shape: MlTensorShape3D, output_shape: MlTensorShape3D, ge_tile_size=(512, 512)):
    """Checks that the shapes match the constraintsof the backend"""

    if not (input_shape.x in [1, ge_tile_size[0]] and input_shape.y in [1, ge_tile_size[1]] and input_shape.bands > 0):
        raise InputException(f"Backend currently supports single pixel and full tile shaped input! Got {input_shape}!")

    if not (
        output_shape.x in [1, ge_tile_size[0]] and output_shape.y in [1, ge_tile_size[1]] and output_shape.bands > 0
    ):
        raise InputException(f"Backend currently supports single pixel and full tile shaped Output! Got {input_shape}!")


# pylint: disable=too-many-branches,too-many-statements
def validate_model_config(
    onnx_model: ModelProto,
    *,
    input_type: RasterDataType,
    output_type: RasterDataType,
    input_shape: MlTensorShape3D,
    out_shape: MlTensorShape3D,
):
    """Validates the model config. Raises an exception if the model config is invalid"""

    def check_data_type(data_type: TypeProto, expected_type: RasterDataType, prefix: str):
        if not data_type.tensor_type:
            raise InputException("Only tensor input types are supported")
        elem_type = data_type.tensor_type.elem_type
        expected_tensor_type = RASTER_TYPE_TO_ONNX_TYPE[expected_type]
        if elem_type != expected_tensor_type:
            elem_type_str = tensor_dtype_to_string(elem_type)
            expected_type_str = tensor_dtype_to_string(expected_tensor_type)
            raise InputException(
                f"Model {prefix} type `{elem_type_str}` does not match the expected type `{expected_type_str}`"
            )

    model_inputs = onnx_model.graph.input
    model_outputs = onnx_model.graph.output

    if len(model_inputs) != 1:
        raise InputException("Models with multiple inputs are not supported")
    check_data_type(model_inputs[0].type, input_type, "input")

    dim = model_inputs[0].type.tensor_type.shape.dim

    in_ts3d = model_dim_to_tensorshape(dim)
    if not in_ts3d == input_shape:
        raise InputException(f"Input shape {in_ts3d} and metadata {input_shape} not equal!")

    if len(model_outputs) < 1:
        raise InputException("Models with no outputs are not supported")
    check_data_type(model_outputs[0].type, output_type, "output")

    dim = model_outputs[0].type.tensor_type.shape.dim
    out_ts3d = model_dim_to_tensorshape(dim)
    if not out_ts3d == out_shape:
        raise InputException(f"Output shape {out_ts3d} and metadata {out_shape} not equal!")


RASTER_TYPE_TO_ONNX_TYPE = {
    RasterDataType.F32: TensorProto.FLOAT,
    RasterDataType.F64: TensorProto.DOUBLE,
    RasterDataType.U8: TensorProto.UINT8,
    RasterDataType.U16: TensorProto.UINT16,
    RasterDataType.U32: TensorProto.UINT32,
    RasterDataType.U64: TensorProto.UINT64,
    RasterDataType.I8: TensorProto.INT8,
    RasterDataType.I16: TensorProto.INT16,
    RasterDataType.I32: TensorProto.INT32,
    RasterDataType.I64: TensorProto.INT64,
}
