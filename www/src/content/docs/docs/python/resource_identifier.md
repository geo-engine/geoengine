---
sidebar_label: resource_identifier
title: resource_identifier
---

Types that identify a ressource in the Geo Engine

## MlModelName Objects

```python
class MlModelName()
```

A wrapper for an MlModel name

#### from_response

```python
@classmethod
def from_response(
    cls, response: geoengine_openapi_client.models.MlModelNameResponse
) -> MlModelName
```

Parse a http response to an `DatasetName`

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two dataset names are equal

## DatasetName Objects

```python
class DatasetName()
```

A wrapper for a dataset name

#### from_response

```python
@classmethod
def from_response(
        cls,
        response: geoengine_openapi_client.DatasetNameResponse) -> DatasetName
```

Parse a http response to an `DatasetName`

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two dataset names are equal

## UploadId Objects

```python
class UploadId()
```

A wrapper for an upload id

#### from_response

```python
@classmethod
def from_response(cls,
                  response: geoengine_openapi_client.IdResponse) -> UploadId
```

Parse a http response to an `UploadId`

#### \_\_eq\_\_

```python
def __eq__(other) -> bool
```

Checks if two upload ids are equal

#### to_api_dict

```python
def to_api_dict() -> geoengine_openapi_client.IdResponse
```

Converts the upload id to a dict for the api

## Resource Objects

```python
class Resource()
```

A wrapper for a resource id

#### \_\_init\_\_

```python
def __init__(resource_type: Literal["dataset", "layer", "layerCollection",
                                    "mlModel", "project"],
             resource_id: str | UUID) -> None
```

Create a resource id

#### from_layer_id

```python
@classmethod
def from_layer_id(cls, layer_id: LayerId) -> Resource
```

Create a resource id from a layer id

#### from_layer_collection_id

```python
@classmethod
def from_layer_collection_id(
        cls, layer_collection_id: LayerCollectionId) -> Resource
```

Create a resource id from a layer collection id

#### from_dataset_name

```python
@classmethod
def from_dataset_name(cls, dataset_name: DatasetName | str) -> Resource
```

Create a resource id from a dataset name

#### from_ml_model_name

```python
@classmethod
def from_ml_model_name(cls, ml_model_name: MlModelName | str) -> Resource
```

Create a resource from an ml model name

#### to_api_dict

```python
def to_api_dict() -> geoengine_openapi_client.Resource
```

Convert to a dict for the API

#### from_response

```python
@classmethod
def from_response(cls,
                  response: geoengine_openapi_client.Resource) -> Resource
```

Convert to a dict for the API

#### \_\_eq\_\_

```python
def __eq__(value)
```

Checks if two listings are equal
