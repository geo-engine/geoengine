"""Deprecated: use `geoengine.processing_graph_builder.operators` instead."""

import sys

from geoengine.processing_graph_builder import operators

# alias the new module, so that classes and `isinstance` checks stay identical
sys.modules[__name__] = operators
