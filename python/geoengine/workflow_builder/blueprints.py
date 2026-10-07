"""Deprecated: use `geoengine.processing_graph_builder.blueprints` instead."""

import sys

from geoengine.processing_graph_builder import blueprints

# alias the new module, so that classes and `isinstance` checks stay identical
sys.modules[__name__] = blueprints
