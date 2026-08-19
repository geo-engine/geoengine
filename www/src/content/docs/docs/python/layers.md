---
sidebar_label: layers
title: layers
---

A wrapper around the layer and layerDb API

## Listing Objects

```python
@dataclass(repr=False)
class Listing(Generic[LISTINGID])
```

A listing item of a collection

#### \_\_repr\_\_

```python
def __repr__() -> str
```

String representation of a `Listing`

#### html\_str

```python
def html_str() -> str
```

HTML representation for Jupyter notebooks

#### \_repr\_html\_

```python
def _repr_html_() -> str
```

HTML representation for Jupyter notebooks

#### load

```python
def load(timeout: int = 60) -> LayerCollection | Layer
```

Load the listing item

## LayerListing Objects

```python
@dataclass(repr=False)
class LayerListing(Listing[LayerId])
```

A layer listing as item of a collection

#### load

```python
def load(timeout: int = 60) -> LayerCollection | Layer
```

Load the listing item

## LayerCollectionListing Objects

```python
@dataclass(repr=False)
class LayerCollectionListing(Listing[LayerCollectionId])
```

A layer listing as item of a collection

#### load

```python
def load(timeout: int = 60) -> LayerCollection | Layer
```

Load the listing item

## LayerCollection Objects

```python
class LayerCollection()
```

A layer collection

#### \_\_init\_\_

```python
def __init__(name: str, description: str, collection_id: LayerCollectionId,
             provider_id: LayerProviderId, items: list[Listing]) -> None
```

Create a new `LayerCollection`

#### from\_response

```python
@classmethod
def from_response(
    cls, response_pages: list[geoengine_api_client.LayerCollection]
) -> LayerCollection
```

Parse an HTTP JSON response to an `LayerCollection`

#### reload

```python
def reload() -> LayerCollection
```

Reload the layer collection

#### \_\_repr\_\_

```python
def __repr__() -> str
```

String representation of a `LayerCollection`

#### \_repr\_html\_

```python
def _repr_html_() -> str | None
```

HTML representation for Jupyter notebooks

#### remove

```python
def remove(timeout: int = 60) -> None
```

Remove the layer collection itself

#### remove\_item

```python
def remove_item(index: int, timeout: int = 60)
```

Remove a layer or collection from this collection

#### add\_layer

```python
def add_layer(name: str,
              description: str,
              workflow: dict[str, Any] | WorkflowBuilderOperator,
              symbology: Symbology | None,
              replace_existing: bool = False,
              timeout: int = 60) -> LayerId
```

Add a layer to this collection. Removes existing layers with the same name if forced.

#### add\_layer\_with\_permissions

```python
def add_layer_with_permissions(name: str,
                               description: str,
                               workflow: dict[str, Any]
                               | WorkflowBuilderOperator,
                               symbology: Symbology | None,
                               permission_tuples: list[tuple[RoleId,
                                                             Permission]]
                               | None = None,
                               replace_existing: bool = False,
                               timeout: int = 60) -> LayerId
```

Add a layer to this collection and set permissions.
Removes existing layers with the same name if forced.

#### add\_existing\_layer

```python
def add_existing_layer(existing_layer: LayerListing | Layer | LayerId,
                       timeout: int = 60)
```

Add an existing layer to this collection

#### add\_collection

```python
def add_collection(name: str,
                   description: str,
                   timeout: int = 60) -> LayerCollectionId
```

Add a collection to this collection

#### add\_existing\_collection

```python
def add_existing_collection(existing_collection: LayerCollectionListing
                            | LayerCollection | LayerCollectionId,
                            timeout: int = 60) -> LayerCollectionId
```

Add an existing collection to this collection

#### get\_items\_by\_name

```python
def get_items_by_name(name: str) -> list[Listing]
```

Get all children with the given name

#### get\_items\_by\_name\_unique

```python
def get_items_by_name_unique(name: str) -> Listing | None
```

Get all children with the given name

#### search

```python
def search(search_string: str,
           *,
           search_type: Literal["fulltext", "prefix"] = "fulltext",
           offset: int = 0,
           limit: int = 20,
           timeout: int = 60) -> list[Listing]
```

Search for a string in the layer collection

#### get\_or\_create\_unique\_collection

```python
def get_or_create_unique_collection(
    collection_name: str,
    create_collection_description: str | None = None,
    delete_existing_with_same_name: bool = False,
    create_permissions_tuples: list[tuple[RoleId, Permission]] | None = None
) -> LayerCollection
```

Get a unique child by name OR if it does not exist create it.
Removes existing collections with same name if forced!
Sets permissions if the collection is created from a list of tuples

#### \_\_eq\_\_

```python
def __eq__(other)
```

Tests if two layer listings are identical

## Layer Objects

```python
@dataclass(repr=False)
class Layer()
```

A layer

#### workflow

TODO: specify in more detail

#### properties

TODO: specify in more detail

#### metadata

TODO: specify in more detail

#### \_\_init\_\_

```python
def __init__(name: str, description: str, layer_id: LayerId,
             provider_id: LayerProviderId, workflow: dict[str, Any],
             symbology: Symbology | None, properties: list[Any],
             metadata: dict[Any, Any]) -> None
```

Create a new `Layer`

#### from\_response

```python
@classmethod
def from_response(cls, response: geoengine_api_client.Layer) -> Layer
```

Parse an HTTP JSON response to an `Layer`

#### \_\_repr\_\_

```python
def __repr__() -> str
```

String representation of a `Layer`

#### \_repr\_html\_

```python
def _repr_html_() -> str | None
```

HTML representation for Jupyter notebooks

#### save\_as\_dataset

```python
def save_as_dataset(timeout: int = 60) -> Task
```

Save a layer as a new dataset.

#### to\_api\_dict

```python
def to_api_dict() -> geoengine_api_client.Layer
```

Convert to a dictionary that can be serialized to JSON

#### as\_workflow\_id

```python
def as_workflow_id(timeout: int = 60) -> WorkflowId
```

Register a layer as a workflow and returns its workflowId

#### as\_workflow

```python
def as_workflow(timeout: int = 60) -> Workflow
```

Register a layer as a workflow and returns the workflow

#### layer\_collection

```python
def layer_collection(layer_collection_id: LayerCollectionId | None = None,
                     layer_provider_id: LayerProviderId = LAYER_DB_PROVIDER_ID,
                     timeout: int = 60) -> LayerCollection
```

Retrieve a layer collection that contains layers and layer collections.

#### layer

```python
def layer(layer_id: LayerId,
          layer_provider_id: LayerProviderId = LAYER_DB_PROVIDER_ID,
          timeout: int = 60) -> Layer
```

Retrieve a layer from the server.
