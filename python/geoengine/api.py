"""These types represent Geo Engine's backend API types before/after JSON (de)serialization."""

from typing import TypedDict

from geoengine_openapi_client.models import *  # pylint: disable=wildcard-import,unused-wildcard-import  # noqa: F403

Rgba = tuple[int, int, int, int]

GEOMETRY_COLUMN_NAME = "__geometry"
TIME_COLUMN_NAME = "__time"


class StoredDataset(TypedDict):  # pylint: disable=too-few-public-methods
    """A stored dataset"""

    dataset: str
    upload: str
