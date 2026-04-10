"""
A wrapper around the layer and layerDb API
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import auto
from io import StringIO
from typing import Any, Generic, Literal, TypeVar, cast

import geoengine_openapi_client
from strenum import LowercaseStrEnum

from geoengine.auth import get_session
from geoengine.error import InputException, ModificationNotOnLayerDbException
from geoengine.permissions import Permission, RoleId, add_permission
from geoengine.resource_identifier import LAYER_DB_PROVIDER_ID, LayerCollectionId, LayerId, LayerProviderId, Resource
from geoengine.tasks import Task, TaskId
from geoengine.types import Symbology
from geoengine.workflow import Workflow, WorkflowId
from geoengine.workflow_builder.operators import Operator as WorkflowBuilderOperator


class LayerCollectionListingType(LowercaseStrEnum):
    LAYER = auto()
    COLLECTION = auto()


LISTINGID = TypeVar("LISTINGID")


@dataclass(repr=False)
class Listing(Generic[LISTINGID]):
    """A listing item of a collection"""

    listing_id: LISTINGID
    provider_id: LayerProviderId
    name: str
    description: str

    def __repr__(self) -> str:
        """String representation of a `Listing`"""

        buf = StringIO()

        buf.write(f"{self._type_str()}{os.linesep}")
        buf.write(f"name: {self.name}{os.linesep}")
        buf.write(f"description: {self.description}{os.linesep}")
        buf.write(f"id: {self.listing_id}{os.linesep}")
        buf.write(f"provider id: {self.provider_id}{os.linesep}")

        return buf.getvalue()

    def html_str(self) -> str:
        """HTML representation for Jupyter notebooks"""

        buf = StringIO()

        buf.write("<table>")
        buf.write(f'<thead><tr><th colspan="2">{self._type_str()}</th></tr></thead>')
        buf.write("<tbody>")
        buf.write(f"<tr><th>name</th><td>{self.name}</td></tr>")
        buf.write(f"<tr><th>description</th><td>{self.description}</td></tr>")
        buf.write(f"<tr><th>id</th><td>{self.listing_id}</td></tr>")
        buf.write(f"<tr><th>provider id</th><td>{self.provider_id}</td></tr>")
        buf.write("</tbody>")
        buf.write("</table>")

        return buf.getvalue()

    def _repr_html_(self) -> str:
        """HTML representation for Jupyter notebooks"""

        return self.html_str()

    def _type_str(self) -> str:
        """String representation of the listing type"""
        raise NotImplementedError("Please Implement this method")

    def load(self, timeout: int = 60) -> LayerCollection | Layer:
        """Load the listing item"""
        raise NotImplementedError("Please Implement this method")

    def _remove(self, collection_id: LayerCollectionId, provider_id: LayerProviderId, timeout: int = 60) -> None:
        """Remove the item behind the listing from the parent collection"""
        raise NotImplementedError("Please Implement this method")


@dataclass(repr=False)
class LayerListing(Listing[LayerId]):
    """A layer listing as item of a collection"""

    def _type_str(self) -> str:
        """String representation of the listing type"""

        return "Layer"

    def load(self, timeout: int = 60) -> LayerCollection | Layer:
        """Load the listing item"""

        return layer(self.listing_id, self.provider_id, timeout)

    def _remove(self, collection_id: LayerCollectionId, provider_id: LayerProviderId, timeout: int = 60) -> None:
        if provider_id != LAYER_DB_PROVIDER_ID:
            raise ModificationNotOnLayerDbException("Layer collection is not stored in the layer database")

        _delete_layer_from_collection(
            collection_id,
            self.listing_id,
            timeout,
        )


@dataclass(repr=False)
class LayerCollectionListing(Listing[LayerCollectionId]):
    """A layer listing as item of a collection"""

    def _type_str(self) -> str:
        """String representation of the listing type"""

        return "Layer Collection"

    def load(self, timeout: int = 60) -> LayerCollection | Layer:
        """Load the listing item"""

        return layer_collection(self.listing_id, self.provider_id, timeout)

    def _remove(self, collection_id: LayerCollectionId, provider_id: LayerProviderId, timeout: int = 60) -> None:
        if provider_id != LAYER_DB_PROVIDER_ID:
            raise ModificationNotOnLayerDbException("Layer collection is not stored in the layer database")

        _delete_layer_collection_from_collection(
            collection_id,
            self.listing_id,
            timeout,
        )


class LayerCollection:
    """A layer collection"""

    name: str
    description: str
    collection_id: LayerCollectionId
    provider_id: LayerProviderId
    items: list[Listing]

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        name: str,
        description: str,
        collection_id: LayerCollectionId,
        provider_id: LayerProviderId,
        items: list[Listing],
    ) -> None:
        """Create a new `LayerCollection`"""

        self.name = name
        self.description = description
        self.collection_id = collection_id
        self.provider_id = provider_id
        self.items = items

    @classmethod
    def from_response(cls, response_pages: list[geoengine_openapi_client.LayerCollection]) -> LayerCollection:
        """Parse an HTTP JSON response to an `LayerCollection`"""

        assert len(response_pages) > 0, "No response pages"

        def parse_listing(response: geoengine_openapi_client.CollectionItem) -> Listing:
            inner = response.actual_instance
            if inner is None:
                raise AssertionError("Invalid listing type")

            item_type = LayerCollectionListingType(inner.type)

            if item_type is LayerCollectionListingType.LAYER:
                layer_id_response = cast(geoengine_openapi_client.ProviderLayerId, inner.id)
                return LayerListing(
                    listing_id=LayerId(layer_id_response.layer_id),
                    provider_id=LayerProviderId(layer_id_response.provider_id),
                    name=inner.name,
                    description=inner.description,
                )

            if item_type is LayerCollectionListingType.COLLECTION:
                collection_id_response = cast(geoengine_openapi_client.ProviderLayerCollectionId, inner.id)
                return LayerCollectionListing(
                    listing_id=LayerCollectionId(collection_id_response.collection_id),
                    provider_id=LayerProviderId(collection_id_response.provider_id),
                    name=inner.name,
                    description=inner.description,
                )

            raise AssertionError("Invalid listing type")

        items = []
        for response in response_pages:
            for item_response in response.items:
                items.append(parse_listing(item_response))

        response = response_pages[0]

        return LayerCollection(
            name=response.name,
            description=response.description,
            collection_id=LayerCollectionId(response.id.collection_id),
            provider_id=LayerProviderId(response.id.provider_id),
            items=items,
        )

    def reload(self) -> LayerCollection:
        """Reload the layer collection"""

        return layer_collection(self.collection_id, self.provider_id)

    def __repr__(self) -> str:
        """String representation of a `LayerCollection`"""

        buf = StringIO()

        buf.write(f"Layer Collection{os.linesep}")
        buf.write(f"name: {self.name}{os.linesep}")
        buf.write(f"description: {self.description}{os.linesep}")
        buf.write(f"id: {self.collection_id}{os.linesep}")
        buf.write(f"provider id: {self.provider_id}{os.linesep}")

        for i, item in enumerate(self.items):
            items_str = "items: "
            buf.write(items_str if i == 0 else " " * len(items_str))
            buf.write(f"{item}{os.linesep}")

        return buf.getvalue()

    def _repr_html_(self) -> str | None:
        """HTML representation for Jupyter notebooks"""

        buf = StringIO()

        buf.write("<table>")
        buf.write('<thead><tr><th colspan="2">Layer Collection</th></tr></thead>')
        buf.write("<tbody>")
        buf.write(f"<tr><th>name</th><td>{self.name}</td></tr>")
        buf.write(f"<tr><th>description</th><td>{self.description}</td></tr>")
        buf.write(f"<tr><th>id</th><td>{self.collection_id}</td></tr>")
        buf.write(f"<tr><th>provider id</th><td>{self.provider_id}</td></tr>")

        num_items = len(self.items)
        for i, item in enumerate(self.items):
            buf.write("<tr>")
            if i == 0:
                buf.write(f'<th rowspan="{num_items}">items</th>')
            buf.write(f"<td>{item.html_str()}</td>")
            buf.write("</tr>")

        buf.write("</tbody>")
        buf.write("</table>")

        return buf.getvalue()

    def remove(self, timeout: int = 60) -> None:
        """Remove the layer collection itself"""

        if self.provider_id != LAYER_DB_PROVIDER_ID:
            raise ModificationNotOnLayerDbException("Layer collection is not stored in the layer database")

        _delete_layer_collection(self.collection_id, timeout)

    def remove_item(self, index: int, timeout: int = 60):
        """Remove a layer or collection from this collection"""

        if index < 0 or index >= len(self.items):
            raise IndexError(f"index {index} out of range")

        item = self.items[index]

        if self.provider_id != LAYER_DB_PROVIDER_ID:
            raise ModificationNotOnLayerDbException("Layer collection is not stored in the layer database")

        # pylint: disable=protected-access
        item._remove(self.collection_id, self.provider_id, timeout)

        self.items.pop(index)

    def add_layer(
        self,
        name: str,
        description: str,
        # TODO: improve type
        workflow: dict[str, Any] | WorkflowBuilderOperator,
        symbology: Symbology | None,
        timeout: int = 60,
    ) -> LayerId:
        """Add a layer to this collection"""
        # pylint: disable=too-many-arguments,too-many-positional-arguments

        if self.provider_id != LAYER_DB_PROVIDER_ID:
            raise ModificationNotOnLayerDbException("Layer collection is not stored in the layer database")

        layer_id = _add_layer_to_collection(name, description, workflow, symbology, self.collection_id, timeout)

        self.items.append(
            LayerListing(
                listing_id=layer_id,
                provider_id=self.provider_id,
                name=name,
                description=description,
            )
        )

        return layer_id

    def add_layer_with_permissions(
        self,
        name: str,
        description: str,
        # TODO: improve type
        workflow: dict[str, Any] | WorkflowBuilderOperator,
        symbology: Symbology | None,
        permission_tuples: list[tuple[RoleId, Permission]] | None = None,
        timeout: int = 60,
    ) -> LayerId:
        """
        Add a layer to this collection and set permissions.
        """

        layer_id = self.add_layer(name, description, workflow, symbology, timeout)

        if permission_tuples is not None:
            res = Resource.from_layer_id(layer_id)
            for role, perm in permission_tuples:
                add_permission(role, res, perm)

        return layer_id

    def add_existing_layer(self, existing_layer: LayerListing | Layer | LayerId, timeout: int = 60):
        """Add an existing layer to this collection"""

        if self.provider_id != LAYER_DB_PROVIDER_ID:
            raise ModificationNotOnLayerDbException("Layer collection is not stored in the layer database")

        if isinstance(existing_layer, LayerListing):
            layer_id = existing_layer.listing_id
        elif isinstance(existing_layer, Layer):
            layer_id = existing_layer.layer_id
        elif isinstance(existing_layer, str):  # TODO: check for LayerId in Python 3.11+
            layer_id = existing_layer
        else:
            raise InputException("Invalid layer type")

        _add_existing_layer_to_collection(layer_id, self.collection_id, timeout)

        child_layer = layer(layer_id, self.provider_id)

        self.items.append(
            LayerListing(
                listing_id=layer_id,
                provider_id=self.provider_id,
                name=child_layer.name,
                description=child_layer.description,
            )
        )

        return layer_id

    def add_collection(self, name: str, description: str, timeout: int = 60) -> LayerCollectionId:
        """Add a collection to this collection"""

        if self.provider_id != LAYER_DB_PROVIDER_ID:
            raise ModificationNotOnLayerDbException("Layer collection is not stored in the layer database")

        collection_id = _add_layer_collection_to_collection(name, description, self.collection_id, timeout)

        self.items.append(
            LayerCollectionListing(
                listing_id=collection_id,
                provider_id=self.provider_id,
                name=name,
                description=description,
            )
        )

        return collection_id

    def add_existing_collection(
        self, existing_collection: LayerCollectionListing | LayerCollection | LayerCollectionId, timeout: int = 60
    ) -> LayerCollectionId:
        """Add an existing collection to this collection"""

        if self.provider_id != LAYER_DB_PROVIDER_ID:
            raise ModificationNotOnLayerDbException("Layer collection is not stored in the layer database")

        if isinstance(existing_collection, LayerCollectionListing):
            collection_id = existing_collection.listing_id
        elif isinstance(existing_collection, LayerCollection):
            collection_id = existing_collection.collection_id
        # TODO: check for LayerId in Python 3.11+
        elif isinstance(existing_collection, str):
            collection_id = existing_collection
        else:
            raise InputException("Invalid collection type")

        _add_existing_layer_collection_to_collection(
            collection_id=collection_id, parent_collection_id=self.collection_id, timeout=timeout
        )

        child_collection = layer_collection(collection_id, self.provider_id)

        self.items.append(
            LayerCollectionListing(
                listing_id=collection_id,
                provider_id=self.provider_id,
                name=child_collection.name,
                description=child_collection.description,
            )
        )

        return collection_id

    def get_items_by_name(self, name: str) -> list[Listing]:
        """Get all children with the given name"""

        return [item for item in self.items if item.name == name]

    def get_items_by_name_unique(self, name: str) -> Listing | None:
        """Get all children with the given name"""

        items = self.get_items_by_name(name=name)
        if len(items) == 0:
            return None
        if len(items) > 1:
            raise KeyError("{name} is not unique")
        return items[0]

    def search(
        self,
        search_string: str,
        *,
        search_type: Literal["fulltext", "prefix"] = "fulltext",
        offset: int = 0,
        limit: int = 20,
        timeout: int = 60,
    ) -> list[Listing]:
        """Search for a string in the layer collection"""

        session = get_session()

        if search_type not in [
            geoengine_openapi_client.SearchType.FULLTEXT,
            geoengine_openapi_client.SearchType.PREFIX,
        ]:
            raise ValueError(f"Invalid search type {search_type}")

        with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
            layers_api = geoengine_openapi_client.LayersApi(api_client)
            layer_collection_response = layers_api.search_handler(
                provider=self.provider_id,
                collection=str(self.collection_id),
                search_string=search_string,
                search_type=geoengine_openapi_client.SearchType(search_type),
                offset=offset,
                limit=limit,
                _request_timeout=timeout,
            )

        listings: list[Listing] = []
        for item in layer_collection_response.items:
            inner = item.actual_instance
            if inner is None:
                continue
            if isinstance(inner, geoengine_openapi_client.LayerCollectionListing):
                listings.append(
                    LayerCollectionListing(
                        listing_id=LayerCollectionId(inner.id.collection_id),
                        provider_id=LayerProviderId(inner.id.provider_id),
                        name=inner.name,
                        description=inner.description,
                    )
                )
            elif isinstance(inner, geoengine_openapi_client.LayerListing):
                listings.append(
                    LayerListing(
                        listing_id=LayerId(inner.id.layer_id),
                        provider_id=LayerProviderId(inner.id.provider_id),
                        name=inner.name,
                        description=inner.description,
                    )
                )
            else:
                pass  # ignore, should not happen

        return listings

    def get_or_create_unique_collection(
        self,
        collection_name: str,
        create_collection_description: str | None = None,
        delete_existing_with_same_name: bool = False,
        create_permissions_tuples: list[tuple[RoleId, Permission]] | None = None,
    ) -> LayerCollection:
        """
        Get a unique child by name OR if it does not exist create it.
        Removes existing collections with same name if forced!
        Sets permissions if the collection is created from a list of tuples
        """
        parent_collection = self.reload()  # reload just to be safe since self's state change on the server
        existing_collections = parent_collection.get_items_by_name(collection_name)

        if delete_existing_with_same_name and len(existing_collections) > 0:
            for c in existing_collections:
                actual = c.load()
                if isinstance(actual, LayerCollection):
                    actual.remove()
            parent_collection = parent_collection.reload()
            existing_collections = parent_collection.get_items_by_name(collection_name)

        if len(existing_collections) == 0:
            new_desc = create_collection_description if create_collection_description is not None else collection_name
            new_collection = parent_collection.add_collection(collection_name, new_desc)
            new_ressource = Resource.from_layer_collection_id(new_collection)

            if create_permissions_tuples is not None:
                for role, perm in create_permissions_tuples:
                    add_permission(role, new_ressource, perm)
            parent_collection = parent_collection.reload()
            existing_collections = parent_collection.get_items_by_name(collection_name)

        if len(existing_collections) == 0:
            raise KeyError(
                f"No collection with name {collection_name} exists in {parent_collection.name} and none was created!"
            )

        if len(existing_collections) > 1:
            raise KeyError(f"Multiple collections with name {collection_name} exist in {parent_collection.name}")

        res = existing_collections[0].load()
        if isinstance(res, Layer):
            raise TypeError(f"Found a Layer not a Layer collection for {collection_name}")

        # we know that it is a collection since check that
        return cast(LayerCollection, existing_collections[0].load())

    def __eq__(self, other):
        """Tests if two layer listings are identical"""
        if not isinstance(other, self.__class__):
            return False

        return (
            self.name == other.name
            and self.description == other.description
            and self.provider_id == other.provider_id
            and self.collection_id == other.collection_id
            and self.items == other.items
        )


@dataclass(repr=False)
class Layer:
    """A layer"""

    # pylint: disable=too-many-instance-attributes

    name: str
    description: str
    layer_id: LayerId
    provider_id: LayerProviderId
    workflow: dict[str, Any]  # TODO: specify in more detail
    symbology: Symbology | None
    properties: list[Any]  # TODO: specify in more detail
    metadata: dict[str, Any]  # TODO: specify in more detail

    def __init__(
        self,
        name: str,
        description: str,
        layer_id: LayerId,
        provider_id: LayerProviderId,
        workflow: dict[str, Any],
        symbology: Symbology | None,
        properties: list[Any],
        metadata: dict[Any, Any],
    ) -> None:
        """Create a new `Layer`"""
        # pylint: disable=too-many-arguments,too-many-positional-arguments

        self.name = name
        self.description = description
        self.layer_id = layer_id
        self.provider_id = provider_id
        self.workflow = workflow
        self.symbology = symbology
        self.properties = properties
        self.metadata = metadata

    @classmethod
    def from_response(cls, response: geoengine_openapi_client.Layer) -> Layer:
        """Parse an HTTP JSON response to an `Layer`"""
        symbology = None
        if response.symbology is not None:
            symbology = Symbology.from_response(response.symbology)

        workflow_dict = cast(dict[str, Any], response.workflow.to_dict())  # silence mypy here

        return Layer(
            name=response.name,
            description=response.description,
            layer_id=LayerId(response.id.layer_id),
            provider_id=LayerProviderId(response.id.provider_id),
            workflow=workflow_dict,
            symbology=symbology,
            properties=cast(list[Any], response.properties),
            metadata=cast(dict[Any, Any], response.metadata),
        )

    def __repr__(self) -> str:
        """String representation of a `Layer`"""

        buf = StringIO()

        buf.write(f"Layer{os.linesep}")
        buf.write(f"name: {self.name}{os.linesep}")
        buf.write(f"description: {self.description}{os.linesep}")
        buf.write(f"id: {self.layer_id}{os.linesep}")
        buf.write(f"provider id: {self.provider_id}{os.linesep}")
        # TODO: better representation of workflow, symbology, properties, metadata
        buf.write(f"workflow: {self.workflow}{os.linesep}")
        buf.write(f"symbology: {self.symbology}{os.linesep}")
        buf.write(f"properties: {self.properties}{os.linesep}")
        buf.write(f"metadata: {self.metadata}{os.linesep}")

        return buf.getvalue()

    def _repr_html_(self) -> str | None:
        """HTML representation for Jupyter notebooks"""

        buf = StringIO()

        buf.write("<table>")
        buf.write('<thead><tr><th colspan="2">Layer</th></tr></thead>')
        buf.write("<tbody>")
        buf.write(f"<tr><th>name</th><td>{self.name}</td></tr>")
        buf.write(f"<tr><th>description</th><td>{self.description}</td></tr>")
        buf.write(f"<tr><th>id</th><td>{self.layer_id}</td></tr>")
        buf.write(f"<tr><th>provider id</th><td>{self.provider_id}</td></tr>")

        # TODO: better representation of workflow, symbology, properties, metadata
        buf.write('<tr><th>workflow</th><td align="left">')
        buf.write(f"<pre>{json.dumps(self.workflow, indent=4)}{os.linesep}</pre></td></tr>")
        buf.write("<tr><th>symbology</th>")
        if self.symbology is None:
            buf.write('<td align="left">None</td></tr>')
        else:
            buf.write(f'<td align="left"><pre>{self.symbology.to_api_dict().to_json()}{os.linesep}</pre></td></tr>')
        buf.write(f"<tr><th>properties</th><td>{self.properties}{os.linesep}</td></tr>")
        buf.write(f"<tr><th>metadata</th><td>{self.metadata}{os.linesep}</td></tr>")

        buf.write("</tbody>")
        buf.write("</table>")

        return buf.getvalue()

    def save_as_dataset(self, timeout: int = 60) -> Task:
        """
        Save a layer as a new dataset.
        """
        session = get_session()

        with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
            layers_api = geoengine_openapi_client.LayersApi(api_client)
            response = layers_api.layer_to_dataset(self.provider_id, str(self.layer_id), _request_timeout=timeout)

        return Task(TaskId.from_response(response))

    def to_api_dict(self) -> geoengine_openapi_client.Layer:
        """Convert to a dictionary that can be serialized to JSON"""
        return geoengine_openapi_client.Layer(
            name=self.name,
            description=self.description,
            id=geoengine_openapi_client.ProviderLayerId(
                layer_id=str(self.layer_id),
                provider_id=str(self.provider_id),
            ),
            workflow=self.workflow,
            symbology=self.symbology.to_api_dict() if self.symbology is not None else None,
            properties=self.properties,
            metadata=self.metadata,
        )

    def as_workflow_id(self, timeout: int = 60) -> WorkflowId:
        """
        Register a layer as a workflow and returns its workflowId
        """
        session = get_session()

        with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
            layers_api = geoengine_openapi_client.LayersApi(api_client)
            response = layers_api.layer_to_workflow_id_handler(
                self.provider_id, self.layer_id, _request_timeout=timeout
            )

        return WorkflowId.from_response(response)

    def as_workflow(self, timeout: int = 60) -> Workflow:
        """
        Register a layer as a workflow and returns the workflow
        """
        workflow_id = self.as_workflow_id(timeout=timeout)

        return Workflow(workflow_id)


def layer_collection(
    layer_collection_id: LayerCollectionId | None = None,
    layer_provider_id: LayerProviderId = LAYER_DB_PROVIDER_ID,
    timeout: int = 60,
) -> LayerCollection:
    """
    Retrieve a layer collection that contains layers and layer collections.
    """

    session = get_session()

    page_limit = 20
    pages: list[geoengine_openapi_client.LayerCollection] = []

    offset = 0
    while True:
        with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
            layers_api = geoengine_openapi_client.LayersApi(api_client)

            if layer_collection_id is None:
                page = layers_api.list_root_collections_handler(offset, page_limit, _request_timeout=timeout)
            else:
                page = layers_api.list_collection_handler(
                    layer_provider_id, layer_collection_id, offset, page_limit, _request_timeout=timeout
                )

        if len(page.items) < page_limit:
            # we need at least one page before breaking
            if len(pages) == 0 or len(page.items) > 0:
                pages.append(page)
            break

        pages.append(page)
        offset += page_limit

    return LayerCollection.from_response(pages)


def layer(layer_id: LayerId, layer_provider_id: LayerProviderId = LAYER_DB_PROVIDER_ID, timeout: int = 60) -> Layer:
    """
    Retrieve a layer from the server.
    """

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        layers_api = geoengine_openapi_client.LayersApi(api_client)
        response = layers_api.layer_handler(layer_provider_id, str(layer_id), _request_timeout=timeout)

    return Layer.from_response(response)


def _delete_layer_from_collection(collection_id: LayerCollectionId, layer_id: LayerId, timeout: int = 60) -> None:
    """Delete a layer from a collection"""

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        layers_api = geoengine_openapi_client.LayersApi(api_client)
        layers_api.remove_layer_from_collection(collection_id, layer_id, _request_timeout=timeout)


def _delete_layer_collection_from_collection(
    parent_id: LayerCollectionId, collection_id: LayerCollectionId, timeout: int = 60
) -> None:
    """Delete a layer collection from a collection"""

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        layers_api = geoengine_openapi_client.LayersApi(api_client)
        layers_api.remove_collection_from_collection(parent_id, collection_id, _request_timeout=timeout)


def _delete_layer_collection(collection_id: LayerCollectionId, timeout: int = 60) -> None:
    """Delete a layer collection"""

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        layers_api = geoengine_openapi_client.LayersApi(api_client)
        layers_api.remove_collection(collection_id, _request_timeout=timeout)


def _add_layer_collection_to_collection(
    name: str, description: str, parent_collection_id: LayerCollectionId, timeout: int = 60
) -> LayerCollectionId:
    """Add a new layer collection"""

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        layers_api = geoengine_openapi_client.LayersApi(api_client)
        response = layers_api.add_collection(
            parent_collection_id,
            geoengine_openapi_client.AddLayerCollection(
                name=name,
                description=description,
            ),
            _request_timeout=timeout,
        )

    return LayerCollectionId(str(response.id))


def _add_existing_layer_collection_to_collection(
    collection_id: LayerCollectionId, parent_collection_id: LayerCollectionId, timeout: int = 60
) -> None:
    """Add an existing layer collection to a collection"""

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        layers_api = geoengine_openapi_client.LayersApi(api_client)
        layers_api.add_existing_collection_to_collection(parent_collection_id, collection_id, _request_timeout=timeout)


def _add_layer_to_collection(
    name: str,
    description: str,
    workflow: dict[str, Any] | WorkflowBuilderOperator,  # TODO: improve type
    symbology: Symbology | None,
    collection_id: LayerCollectionId,
    timeout: int = 60,
) -> LayerId:
    """Add a new layer"""
    # pylint: disable=too-many-arguments,too-many-positional-arguments

    # convert workflow to dict if necessary
    if isinstance(workflow, WorkflowBuilderOperator):
        workflow = workflow.to_workflow_dict()

    symbology_dict = symbology.to_api_dict() if symbology is not None and isinstance(symbology, Symbology) else None

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        layers_api = geoengine_openapi_client.LayersApi(api_client)
        response = layers_api.add_layer(
            collection_id,
            geoengine_openapi_client.AddLayer(
                name=name,
                description=description,
                workflow=geoengine_openapi_client.Workflow.from_dict(workflow),
                symbology=symbology_dict,
            ),
            _request_timeout=timeout,
        )

    return LayerId(str(response.id))


def _add_existing_layer_to_collection(layer_id: LayerId, collection_id: LayerCollectionId, timeout: int = 60) -> None:
    """Add an existing layer to a collection"""

    session = get_session()

    with geoengine_openapi_client.ApiClient(session.configuration) as api_client:
        layers_api = geoengine_openapi_client.LayersApi(api_client)
        layers_api.add_existing_layer_to_collection(collection_id, layer_id, _request_timeout=timeout)
