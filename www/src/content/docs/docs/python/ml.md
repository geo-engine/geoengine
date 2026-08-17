---
sidebar_label: ml
title: ml
---

Util functions for machine learning

## MlModelConfig Objects

```python
@dataclass
class MlModelConfig()
```

Configuration for an ml model

#### register\_ml\_model

```python
def register_ml_model(onnx_model: ModelProto,
                      model_config: MlModelConfig,
                      upload_timeout: int = 3600,
                      register_timeout: int = 60) -> MlModelName
```

Uploads an onnx file and registers it as an ml model

#### model\_dim\_to\_tensorshape

```python
def model_dim_to_tensorshape(model_dims)
```

Transform an ONNX dimension into a MlTensorShape3D

#### check\_backend\_constraints

```python
def check_backend_constraints(input_shape: MlTensorShape3D,
                              output_shape: MlTensorShape3D,
                              ge_tile_size=(512, 512))
```

Checks that the shapes match the constraintsof the backend

#### validate\_model\_config

```python
def validate_model_config(onnx_model: ModelProto, *,
                          input_type: RasterDataType,
                          output_type: RasterDataType,
                          input_shape: MlTensorShape3D,
                          out_shape: MlTensorShape3D)
```

Validates the model config. Raises an exception if the model config is invalid
