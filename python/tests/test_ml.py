"""Tests ML functionality"""

import unittest

import numpy as np
from geoengine_openapi_client.models import (
    MlModelInputNoDataHandling,
    MlModelInputNoDataHandlingVariant,
    MlModelMetadata,
    MlModelOutputNoDataHandling,
    MlModelOutputNoDataHandlingVariant,
    MlTensorShape3D,
    RasterDataType,
)
from onnx import TensorShapeProto as TSP
from skl2onnx import to_onnx
from sklearn.ensemble import RandomForestClassifier

import geoengine as ge
from geoengine.ml import model_dim_to_tensorshape
from tests.ge_test import GeoEngineTestInstance


class MlModelTests(unittest.TestCase):
    """Test methods for MlModels"""

    def setUp(self) -> None:
        ge.reset(False)

    def test_model_dim_to_tensorshape(self) -> None:
        """Test model_dim_to_tensorshape"""

        dim_1d: list[TSP.Dimension] = [TSP.Dimension(dim_value=7)]
        mts_1d = MlTensorShape3D(bands=7, y=1, x=1)
        self.assertEqual(model_dim_to_tensorshape(dim_1d), mts_1d)

        dim_1d_v: list[TSP.Dimension] = [TSP.Dimension(dim_value=None), TSP.Dimension(dim_value=7)]
        mts_1d_v = MlTensorShape3D(bands=7, y=1, x=1)
        self.assertEqual(model_dim_to_tensorshape(dim_1d_v), mts_1d_v)

        dim_2d_t: list[TSP.Dimension] = [TSP.Dimension(dim_value=512), TSP.Dimension(dim_value=512)]
        mts_2d_t = MlTensorShape3D(bands=1, y=512, x=512)
        self.assertEqual(model_dim_to_tensorshape(dim_2d_t), mts_2d_t)

        dim_2d_1: list[TSP.Dimension] = [TSP.Dimension(dim_value=1), TSP.Dimension(dim_value=7)]
        mts_2d_1 = MlTensorShape3D(bands=7, y=1, x=1)
        self.assertEqual(model_dim_to_tensorshape(dim_2d_1), mts_2d_1)

        dim_3d_t: list[TSP.Dimension] = [
            TSP.Dimension(dim_value=512),
            TSP.Dimension(dim_value=512),
            TSP.Dimension(dim_value=7),
        ]
        mts_3d_t = MlTensorShape3D(bands=7, y=512, x=512)
        self.assertEqual(model_dim_to_tensorshape(dim_3d_t), mts_3d_t)

        dim_3d_v: list[TSP.Dimension] = [
            TSP.Dimension(dim_value=None),
            TSP.Dimension(dim_value=512),
            TSP.Dimension(dim_value=512),
        ]
        mts_3d_v = MlTensorShape3D(bands=1, y=512, x=512)
        self.assertEqual(model_dim_to_tensorshape(dim_3d_v), mts_3d_v)

        dim_4d_v: list[TSP.Dimension] = [
            TSP.Dimension(dim_value=None),
            TSP.Dimension(dim_value=512),
            TSP.Dimension(dim_value=512),
            TSP.Dimension(dim_value=4),
        ]
        mts_4d_v = MlTensorShape3D(bands=4, y=512, x=512)
        self.assertEqual(model_dim_to_tensorshape(dim_4d_v), mts_4d_v)

    def test_uploading_onnx_model(self) -> None:
        clf = RandomForestClassifier(random_state=42)
        training_x = np.array([[1, 2], [3, 4]], dtype=np.float32)
        training_y = np.array([0, 1], dtype=np.int64)
        clf.fit(training_x, training_y)

        onnx_clf = to_onnx(clf, training_x[:1], options={"zipmap": False}, target_opset=9)

        # TODO: use `enterContext(cm)` instead of `with cm:` in Python 3.11
        with GeoEngineTestInstance() as ge_instance:
            ge_instance.wait_for_ready()

            ge.initialize(ge_instance.address())

            session = ge.get_session()
            model_name = f"{session.user_id}:foo"

            res_name = ge.register_ml_model(
                onnx_model=onnx_clf,
                model_config=ge.ml.MlModelConfig(
                    name=model_name,
                    file_name="model.onnx",
                    metadata=MlModelMetadata(
                        input_type=RasterDataType.F32,
                        output_type=RasterDataType.I64,
                        input_shape=MlTensorShape3D(y=1, x=1, bands=2),
                        output_shape=MlTensorShape3D(y=1, x=1, bands=1),
                        input_no_data_handling=MlModelInputNoDataHandling(
                            variant=MlModelInputNoDataHandlingVariant.SKIPIFNODATA
                        ),
                        output_no_data_handling=MlModelOutputNoDataHandling(
                            variant=MlModelOutputNoDataHandlingVariant.NANISNODATA
                        ),
                    ),
                    display_name="Decision Tree",
                    description="A simple decision tree model",
                ),
            )
            self.assertEqual(str(res_name), model_name)

            # Now test permission setting and removal
            ge.add_permission(ge.REGISTERED_USER_ROLE_ID, ge.Resource.from_ml_model_name(res_name), ge.Permission.READ)

            expected = ge.permissions.PermissionListing(
                permission=ge.Permission.READ,
                resource=ge.Resource.from_ml_model_name(res_name),
                role=ge.permissions.Role(ge.REGISTERED_USER_ROLE_ID, "user"),
            )

            self.assertIn(expected, ge.permissions.list_permissions(ge.Resource.from_ml_model_name(res_name)))

            ge.remove_permission(
                ge.REGISTERED_USER_ROLE_ID, ge.Resource.from_ml_model_name(res_name), ge.Permission.READ
            )

            self.assertNotIn(expected, ge.permissions.list_permissions(ge.Resource.from_ml_model_name(res_name)))

            # failing tests
            with self.assertRaises(ge.InputException) as exception:
                _res_name = ge.register_ml_model(
                    onnx_model=onnx_clf,
                    model_config=ge.ml.MlModelConfig(
                        name=model_name,
                        file_name="model.onnx",
                        metadata=MlModelMetadata(
                            input_type=RasterDataType.F32,
                            output_type=RasterDataType.I64,
                            input_shape=MlTensorShape3D(y=1, x=1, bands=4),
                            output_shape=MlTensorShape3D(y=1, x=1, bands=1),
                            input_no_data_handling=MlModelInputNoDataHandling(
                                variant=MlModelInputNoDataHandlingVariant.SKIPIFNODATA
                            ),
                            output_no_data_handling=MlModelOutputNoDataHandling(
                                variant=MlModelOutputNoDataHandlingVariant.NANISNODATA
                            ),
                        ),
                        display_name="Decision Tree",
                        description="A simple decision tree model",
                    ),
                )
            self.assertEqual(
                str(exception.exception), "Input shape bands=2 x=1 y=1 and metadata bands=4 x=1 y=1 not equal!"
            )

            with self.assertRaises(ge.InputException) as exception:
                _res_name = ge.register_ml_model(
                    onnx_model=onnx_clf,
                    model_config=ge.ml.MlModelConfig(
                        name=model_name,
                        file_name="model.onnx",
                        metadata=MlModelMetadata(
                            input_type=RasterDataType.F64,
                            output_type=RasterDataType.I64,
                            input_shape=MlTensorShape3D(y=1, x=1, bands=2),
                            output_shape=MlTensorShape3D(y=1, x=1, bands=1),
                            input_no_data_handling=MlModelInputNoDataHandling(
                                variant=MlModelInputNoDataHandlingVariant.SKIPIFNODATA
                            ),
                            output_no_data_handling=MlModelOutputNoDataHandling(
                                variant=MlModelOutputNoDataHandlingVariant.NANISNODATA
                            ),
                        ),
                        display_name="Decision Tree",
                        description="A simple decision tree model",
                    ),
                )
            self.assertEqual(
                str(exception.exception),
                "Model input type `TensorProto.FLOAT` does not match the expected type `TensorProto.DOUBLE`",
            )

            with self.assertRaises(ge.InputException) as exception:
                ge.register_ml_model(
                    onnx_model=onnx_clf,
                    model_config=ge.ml.MlModelConfig(
                        name="foo",
                        file_name="model.onnx",
                        metadata=MlModelMetadata(
                            input_type=RasterDataType.F32,
                            output_type=RasterDataType.I32,
                            input_shape=MlTensorShape3D(y=1, x=1, bands=2),
                            output_shape=MlTensorShape3D(y=1, x=1, bands=1),
                            input_no_data_handling=MlModelInputNoDataHandling(
                                variant=MlModelInputNoDataHandlingVariant.SKIPIFNODATA
                            ),
                            output_no_data_handling=MlModelOutputNoDataHandling(
                                variant=MlModelOutputNoDataHandlingVariant.NANISNODATA
                            ),
                        ),
                        display_name="Decision Tree",
                        description="A simple decision tree model",
                    ),
                )
            self.assertEqual(
                str(exception.exception),
                "Model output type `TensorProto.INT64` does not match the expected type `TensorProto.INT32`",
            )
