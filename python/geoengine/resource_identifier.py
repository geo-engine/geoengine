"""Types that identify a ressource in the Geo Engine"""

from __future__ import annotations

from typing import Any, Literal, NewType
from uuid import UUID

import geoengine_openapi_client

LayerId = NewType("LayerId", str)
LayerCollectionId = NewType("LayerCollectionId", str)
LayerProviderId = NewType("LayerProviderId", UUID)

LAYER_DB_PROVIDER_ID = LayerProviderId(UUID("ce5e84db-cbf9-48a2-9a32-d4b7cc56ea74"))
LAYER_DB_ROOT_COLLECTION_ID = LayerCollectionId("05102bb3-a855-4a37-8a8a-30026a91fef1")


class MlModelName:
    """A wrapper for an MlModel name"""

    __ml_model_name: str

    def __init__(self, ml_model_name: str) -> None:
        self.__ml_model_name = ml_model_name

    @classmethod
    def from_response(cls, response: geoengine_openapi_client.models.MlModelNameResponse) -> MlModelName:
        """Parse a http response to an `DatasetName`"""
        return MlModelName(response.ml_model_name)

    def __str__(self) -> str:
        return self.__ml_model_name

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other) -> bool:
        """Checks if two dataset names are equal"""
        if not isinstance(other, self.__class__):
            return False

        return self.__ml_model_name == other.__ml_model_name  # pylint: disable=protected-access

    def to_api_dict(self) -> geoengine_openapi_client.models.MlModelNameResponse:
        return geoengine_openapi_client.models.MlModelNameResponse(ml_model_name=str(self.__ml_model_name))


class DatasetName:
    """A wrapper for a dataset name"""

    __dataset_name: str

    def __init__(self, dataset_name: str) -> None:
        self.__dataset_name = dataset_name

    @classmethod
    def from_response(cls, response: geoengine_openapi_client.DatasetNameResponse) -> DatasetName:
        """Parse a http response to an `DatasetName`"""
        return DatasetName(response.dataset_name)

    def __str__(self) -> str:
        return self.__dataset_name

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other) -> bool:
        """Checks if two dataset names are equal"""
        if not isinstance(other, self.__class__):
            return False

        return self.__dataset_name == other.__dataset_name  # pylint: disable=protected-access

    def to_api_dict(self) -> geoengine_openapi_client.DatasetNameResponse:
        return geoengine_openapi_client.DatasetNameResponse(dataset_name=str(self.__dataset_name))


class UploadId:
    """A wrapper for an upload id"""

    __upload_id: UUID

    def __init__(self, upload_id: UUID) -> None:
        self.__upload_id = upload_id

    @classmethod
    def from_response(cls, response: geoengine_openapi_client.IdResponse) -> UploadId:
        """Parse a http response to an `UploadId`"""
        return UploadId(response.id)

    def __str__(self) -> str:
        return str(self.__upload_id)

    def __repr__(self) -> str:
        return str(self)

    def __eq__(self, other) -> bool:
        """Checks if two upload ids are equal"""
        if not isinstance(other, self.__class__):
            return False

        return self.__upload_id == other.__upload_id  # pylint: disable=protected-access

    def to_api_dict(self) -> geoengine_openapi_client.IdResponse:
        """Converts the upload id to a dict for the api"""
        return geoengine_openapi_client.IdResponse(id=str(self.__upload_id))


class Resource:
    """A wrapper for a resource id"""

    id: str | UUID
    type: Literal["dataset", "layer", "layerCollection", "mlModel", "project"]

    def __init__(
        self,
        resource_type: Literal["dataset", "layer", "layerCollection", "mlModel", "project"],
        resource_id: str | UUID,
    ) -> None:
        """Create a resource id"""
        self.type = resource_type
        self.id = resource_id

    @classmethod
    def from_layer_id(cls, layer_id: LayerId) -> Resource:
        """Create a resource id from a layer id"""
        return Resource("layer", str(layer_id))

    @classmethod
    def from_layer_collection_id(cls, layer_collection_id: LayerCollectionId) -> Resource:
        """Create a resource id from a layer collection id"""
        return Resource("layerCollection", str(layer_collection_id))

    @classmethod
    def from_dataset_name(cls, dataset_name: DatasetName | str) -> Resource:
        """Create a resource id from a dataset name"""
        if isinstance(dataset_name, DatasetName):
            dataset_name = str(dataset_name)
        return Resource("dataset", dataset_name)

    @classmethod
    def from_ml_model_name(cls, ml_model_name: MlModelName | str) -> Resource:
        """Create a resource from an ml model name"""
        if isinstance(ml_model_name, MlModelName):
            ml_model_name = str(ml_model_name)
        return Resource("mlModel", ml_model_name)

    def to_api_dict(self) -> geoengine_openapi_client.Resource:
        """Convert to a dict for the API"""
        inner: Any = None

        if self.type == "layer":
            inner = geoengine_openapi_client.LayerResource(type="layer", id=self.id)
        elif self.type == "layerCollection":
            inner = geoengine_openapi_client.LayerCollectionResource(type="layerCollection", id=self.id)
        elif self.type == "project":
            inner = geoengine_openapi_client.ProjectResource(type="project", id=self.id)
        elif self.type == "dataset":
            inner = geoengine_openapi_client.DatasetResource(type="dataset", id=self.id)
        elif self.type == "mlModel":
            inner = geoengine_openapi_client.MlModelResource(type="mlModel", id=self.id)
        else:
            raise KeyError(f"Unknown resource type: {self.type}")

        return geoengine_openapi_client.Resource(inner)

    @classmethod
    def from_response(cls, response: geoengine_openapi_client.Resource) -> Resource:
        """Convert to a dict for the API"""
        inner: Resource
        if isinstance(response.actual_instance, geoengine_openapi_client.LayerResource):
            inner = Resource("layer", response.actual_instance.id)
        elif isinstance(response.actual_instance, geoengine_openapi_client.LayerCollectionResource):
            inner = Resource("layerCollection", response.actual_instance.id)
        elif isinstance(response.actual_instance, geoengine_openapi_client.ProjectResource):
            inner = Resource("project", response.actual_instance.id)
        elif isinstance(response.actual_instance, geoengine_openapi_client.DatasetResource):
            inner = Resource("dataset", response.actual_instance.id)
        elif isinstance(response.actual_instance, geoengine_openapi_client.MlModelResource):
            inner = Resource("mlModel", response.actual_instance.id)
        else:
            raise KeyError(f"Unknown resource type from API: {response.actual_instance}")
        return inner

    def __repr__(self):
        return "id: " + repr(self.id) + ", type: " + repr(self.type)

    def __eq__(self, value):
        """Checks if two listings are equal"""
        if not isinstance(value, self.__class__):
            return False
        return self.id == value.id and self.type == value.type
