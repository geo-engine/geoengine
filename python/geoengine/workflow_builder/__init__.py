"""Deprecated: use `geoengine.processing_graph_builder` instead."""

from geoengine._deprecation import warn_deprecated_module

warn_deprecated_module("geoengine.workflow_builder", "geoengine.processing_graph_builder")

# pylint: disable=wrong-import-position
from . import blueprints, operators  # noqa: E402
